//! ps-cache-warmer — sitemap-driven Varnish cache warmer.
//!
//! Walks a site's gsitemap (index -> child sitemaps -> page URLs) and replays
//! every page against the LOCAL Varnish so the object is hot before a real
//! visitor arrives. Two design rules drive the whole thing:
//!
//!   1. NEVER go through Cloudflare. We pin the site's hostname to a local
//!      socket (default 127.0.0.1:6081) via reqwest's DNS override, so the
//!      request carries the correct `Host:` header for vhost matching but the
//!      bytes only ever touch the local Varnish — DNS/CF is never consulted.
//!      Redirects are not followed (a 301 to https://www.… would escape local).
//!
//!   2. NEVER spam the server. A global token-bucket caps req/s, a semaphore
//!      caps in-flight requests, and consecutive 5xx/errors trigger an abort so
//!      a struggling backend is not hammered.
//!
//! Cache-key correctness (must match the PrestaShop VCL hash): every URL is
//! warmed once per encoding in `--encodings` (default br + gzip — both variants
//! are hashed separately) with `X-Forwarded-Proto: https` and NO cookie, so the
//! warmed object lands on the shared anonymous cache key real visitors hit.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use quick_xml::events::Event;
use quick_xml::Reader;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep_until, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "ps-cache-warmer",
    about = "Sitemap-driven Varnish cache warmer (local-only, rate-limited)"
)]
struct Args {
    /// Site hostname = the Host header to warm, e.g. funecobikes.com
    #[arg(long)]
    domain: String,

    /// Local upstream to pin the hostname to (the Varnish listener).
    #[arg(long, default_value = "127.0.0.1:6081")]
    upstream: String,

    /// Sitemap entry point (gsitemap index), relative to the host root.
    #[arg(long, default_value = "/1_index_sitemap.xml")]
    sitemap: String,

    /// Global request rate cap, in requests per second.
    #[arg(long, default_value_t = 2.0)]
    rate: f64,

    /// Max in-flight requests.
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    /// Cap on number of page URLs to warm (0 = no cap).
    #[arg(long, default_value_t = 0)]
    max: usize,

    /// Per-request timeout, in seconds.
    #[arg(long, default_value_t = 30)]
    timeout: u64,

    /// Value sent as X-Forwarded-Proto (part of the VCL cache key).
    #[arg(long, default_value = "https")]
    proto: String,

    /// Comma-separated Accept-Encoding variants to warm (each is hashed separately).
    #[arg(long, default_value = "br,gzip")]
    encodings: String,

    /// User-Agent identifying the warmer in access logs.
    #[arg(long, default_value = "CyberialCacheWarmer/0.1")]
    user_agent: String,

    /// Abort after this many consecutive 5xx/transport errors (protects a sick backend).
    #[arg(long, default_value_t = 50)]
    max_consecutive_errors: usize,

    /// Print one line per warmed URL.
    #[arg(long, default_value_t = false)]
    verbose: bool,
}

/// Global token-bucket: spaces out request *starts* across all worker tasks so
/// the combined rate never exceeds `--rate`, regardless of concurrency.
struct RateLimiter {
    interval: Duration,
    next: Mutex<Instant>,
}

impl RateLimiter {
    fn new(rate_per_sec: f64) -> Self {
        let interval = if rate_per_sec > 0.0 {
            Duration::from_secs_f64(1.0 / rate_per_sec)
        } else {
            Duration::ZERO
        };
        RateLimiter {
            interval,
            next: Mutex::new(Instant::now()),
        }
    }

    async fn acquire(&self) {
        if self.interval.is_zero() {
            return;
        }
        let scheduled = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            let scheduled = if *next > now { *next } else { now };
            *next = scheduled + self.interval;
            scheduled
        };
        sleep_until(scheduled).await;
    }
}

#[derive(Default)]
struct Stats {
    hit: AtomicUsize,
    miss: AtomicUsize,
    other: AtomicUsize,
    error: AtomicUsize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let upstream: SocketAddr = args
        .upstream
        .parse()
        .with_context(|| format!("invalid --upstream socket address: {}", args.upstream))?;

    let encodings: Vec<String> = args
        .encodings
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if encodings.is_empty() {
        return Err(anyhow!("--encodings is empty"));
    }

    // Pin the hostname to the local Varnish. The request URL uses the real
    // hostname (so Host: is correct), but resolution is hard-wired to the local
    // socket — Cloudflare/DNS is never involved. Redirects are not followed so a
    // backend 301/302 to an external URL cannot escape the local upstream.
    let client = reqwest::Client::builder()
        .resolve(&args.domain, upstream)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(args.timeout))
        .build()
        .context("building HTTP client")?;

    eprintln!(
        "[warm] {} via {} (rate={}/s, conc={}, enc={})",
        args.domain,
        args.upstream,
        args.rate,
        args.concurrency,
        encodings.join("+")
    );

    // ---- collect page URLs from the sitemap tree ------------------------
    let pages = collect_pages(&client, &args).await?;
    let total = pages.len();
    if total == 0 {
        eprintln!("[warm] no page URLs found in sitemap — nothing to do");
        return Ok(());
    }
    eprintln!(
        "[warm] {} page URLs to warm x {} encoding(s)",
        total,
        encodings.len()
    );

    // ---- warm ------------------------------------------------------------
    let limiter = Arc::new(RateLimiter::new(args.rate));
    let sem = Arc::new(Semaphore::new(args.concurrency.max(1)));
    let stats = Arc::new(Stats::default());
    let abort = Arc::new(AtomicBool::new(false));
    let consecutive_errors = Arc::new(AtomicUsize::new(0));
    let args = Arc::new(args);
    let encodings = Arc::new(encodings);

    let mut handles = Vec::new();
    'outer: for path in pages {
        for enc in encodings.iter().cloned() {
            if abort.load(Ordering::Relaxed) {
                break 'outer;
            }
            limiter.acquire().await;
            let permit = sem.clone().acquire_owned().await.unwrap();
            let client = client.clone();
            let args = args.clone();
            let stats = stats.clone();
            let abort = abort.clone();
            let consecutive_errors = consecutive_errors.clone();
            let url = format!("http://{}{}", args.domain, path);

            handles.push(tokio::spawn(async move {
                let _permit = permit;
                warm_one(&client, &args, &url, &enc, &stats, &abort, &consecutive_errors).await;
            }));
        }
    }

    for h in handles {
        let _ = h.await;
    }

    let hit = stats.hit.load(Ordering::Relaxed);
    let miss = stats.miss.load(Ordering::Relaxed);
    let other = stats.other.load(Ordering::Relaxed);
    let error = stats.error.load(Ordering::Relaxed);
    let done = hit + miss + other + error;
    let aborted = abort.load(Ordering::Relaxed);
    println!(
        "[warm] done {} req: hit={} miss={} other={} error={}{}",
        done,
        hit,
        miss,
        other,
        error,
        if aborted {
            " (ABORTED: too many consecutive errors)"
        } else {
            ""
        }
    );

    if aborted {
        std::process::exit(1);
    }
    Ok(())
}

/// Walk the sitemap tree (index -> child sitemaps -> pages) and return the
/// de-duplicated list of page paths (`/path?query`) to warm.
async fn collect_pages(client: &reqwest::Client, args: &Args) -> Result<Vec<String>> {
    let mut queue = vec![args.sitemap.clone()];
    let mut seen_sitemaps = std::collections::HashSet::new();
    let mut pages: Vec<String> = Vec::new();
    let mut seen_pages = std::collections::HashSet::new();
    let mut guard = 0usize;

    while let Some(sm_path) = queue.pop() {
        guard += 1;
        if guard > 1000 {
            eprintln!("[warm] sitemap walk guard hit (1000) — stopping discovery");
            break;
        }
        if !seen_sitemaps.insert(sm_path.clone()) {
            continue;
        }
        let url = format!("http://{}{}", args.domain, sm_path);
        let body = match fetch_text(client, &url).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[warm] sitemap fetch failed {}: {}", url, e);
                continue;
            }
        };
        let (is_index, locs) = parse_sitemap(&body);
        for loc in locs {
            if is_index {
                if let Some(p) = to_local_path(&loc) {
                    queue.push(p);
                }
            } else if let Some(p) = to_local_path(&loc) {
                if is_warmable(&p) && seen_pages.insert(p.clone()) {
                    pages.push(p);
                    if args.max > 0 && pages.len() >= args.max {
                        return Ok(pages);
                    }
                }
            }
        }
    }
    Ok(pages)
}

async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!("status {}", resp.status()));
    }
    Ok(resp.text().await?)
}

/// Returns (is_sitemapindex, locs). `is_index` true when the root element is
/// <sitemapindex> (locs are child sitemaps), false for <urlset> (locs are pages).
fn parse_sitemap(xml: &str) -> (bool, Vec<String>) {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut is_index = false;
    let mut locs = Vec::new();
    let mut in_loc = false;
    let mut root_seen = false;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let local = e.name().as_ref().to_vec();
                if !root_seen {
                    if local == b"sitemapindex" {
                        is_index = true;
                    }
                    root_seen = true;
                }
                if local == b"loc" {
                    in_loc = true;
                }
            }
            Ok(Event::Text(e)) => {
                if in_loc {
                    if let Ok(t) = e.unescape() {
                        locs.push(t.into_owned());
                    }
                }
            }
            // gsitemap wraps <loc> values in CDATA, which quick-xml delivers as
            // a CData event (not Text). Without this arm every page URL is lost.
            Ok(Event::CData(e)) => {
                if in_loc {
                    locs.push(String::from_utf8_lossy(e.as_ref()).into_owned());
                }
            }
            Ok(Event::End(e)) => {
                if e.name().as_ref() == b"loc" {
                    in_loc = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    (is_index, locs)
}

/// Turn an absolute sitemap <loc> ("https://host/path?q") into a host-relative
/// path ("/path?q"). Returns None if it can't be parsed.
fn to_local_path(loc: &str) -> Option<String> {
    let loc = loc.trim();
    if loc.is_empty() {
        return None;
    }
    let after_scheme = loc.split_once("://").map(|(_, r)| r).unwrap_or(loc);
    let path = match after_scheme.find('/') {
        Some(idx) => &after_scheme[idx..],
        None => "/",
    };
    // Some sitemaps carry malformed entries with raw spaces (e.g.
    // "/meilleures ventes"); a space breaks URL parsing, so percent-encode it.
    Some(path.replace(' ', "%20"))
}

/// Defensive filter: sitemaps should not list these, but never warm paths the
/// VCL passes (cart/account/add-to-cart) or the recursive facet bombs.
fn is_warmable(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    const SKIP: &[&str] = &[
        "/panier",
        "/cart",
        "/checkout",
        "/order",
        "/commande",
        "/quick-order",
        "/mon-compte",
        "/my-account",
        "/login",
        "/authentification",
        "/addresses",
        "?add",
        "&add=",
        "/admin",
        "%3forder%3d",
    ];
    !SKIP.iter().any(|s| p.contains(s))
}

#[allow(clippy::too_many_arguments)]
async fn warm_one(
    client: &reqwest::Client,
    args: &Args,
    url: &str,
    encoding: &str,
    stats: &Stats,
    abort: &AtomicBool,
    consecutive_errors: &AtomicUsize,
) {
    let req = client
        .get(url)
        .header("Accept-Encoding", encoding)
        .header("X-Forwarded-Proto", &args.proto)
        .header("User-Agent", &args.user_agent);

    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let cache = resp
                .headers()
                .get("x-cache")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_ascii_uppercase();
            // Drain the body so Varnish completes (and stores) the backend fetch.
            let _ = resp.bytes().await;

            if status.is_server_error() {
                bump_error(stats, abort, consecutive_errors, args);
                if args.verbose {
                    eprintln!("[warm] {} [{}] {} -> 5xx", url, encoding, status.as_u16());
                }
                return;
            }
            consecutive_errors.store(0, Ordering::Relaxed);
            if cache.contains("HIT") {
                stats.hit.fetch_add(1, Ordering::Relaxed);
            } else if cache.contains("MISS") {
                stats.miss.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.other.fetch_add(1, Ordering::Relaxed);
            }
            if args.verbose {
                eprintln!(
                    "[warm] {} [{}] {} {}",
                    url,
                    encoding,
                    status.as_u16(),
                    if cache.is_empty() { "-" } else { &cache }
                );
            }
        }
        Err(e) => {
            bump_error(stats, abort, consecutive_errors, args);
            if args.verbose {
                eprintln!("[warm] {} [{}] ERR {}", url, encoding, e);
            }
        }
    }
}

fn bump_error(stats: &Stats, abort: &AtomicBool, consecutive_errors: &AtomicUsize, args: &Args) {
    stats.error.fetch_add(1, Ordering::Relaxed);
    let n = consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
    if n >= args.max_consecutive_errors {
        abort.store(true, Ordering::Relaxed);
    }
}
