# ps-cache-warmer

Sitemap-driven Varnish cache warmer for the Cyberial PrestaShop fleet.

It walks a site's gsitemap (`index → child sitemaps → page URLs`) and replays
every page against the **local** Varnish so the object is hot before a real
visitor arrives. This closes the gap that `grace` + a big malloc cannot:
long-tail URLs visited less than once per `TTL+grace`, a cold cache after a
reboot/restart, and post-deploy.

## Two hard rules

1. **Never go through Cloudflare.** The site hostname is pinned to a local
   socket (default `127.0.0.1:6081`) via reqwest's DNS override. The request
   carries the correct `Host:` header for vhost matching, but the bytes only
   ever touch the local Varnish — DNS/CF is never consulted. Redirects are not
   followed (a `301` to `https://www.…` would escape local).

2. **Never spam the server.** A global token-bucket caps requests/second, a
   semaphore caps in-flight requests, and N consecutive `5xx`/transport errors
   abort the run so a struggling backend is not hammered.

## Cache-key correctness

The warmed request must land on the **same** cache key a real anonymous
visitor hits. Matching the PrestaShop VCL hash, each URL is warmed:

- once **per encoding** in `--encodings` (default `br,gzip` — both variants are
  hashed separately, so both must be primed),
- with `X-Forwarded-Proto: https`,
- with **no cookie** (so it shares the anonymous cache entry).

## Usage

```bash
ps-cache-warmer --domain funecobikes.com
# tuning:
ps-cache-warmer --domain funecobikes.com \
  --upstream 127.0.0.1:6081 \
  --sitemap /1_index_sitemap.xml \
  --rate 2 --concurrency 4 \
  --encodings br,gzip \
  --max 0 --verbose
```

Key flags:

| Flag | Default | Meaning |
|---|---|---|
| `--domain` | (required) | Host header / vhost to warm |
| `--upstream` | `127.0.0.1:6081` | Local Varnish socket the host is pinned to |
| `--sitemap` | `/1_index_sitemap.xml` | gsitemap index path |
| `--rate` | `2.0` | Global cap, requests/second |
| `--concurrency` | `4` | Max in-flight requests |
| `--encodings` | `br,gzip` | Accept-Encoding variants warmed per URL |
| `--max` | `0` | Cap on page count (0 = all) |
| `--urls-from` | _(none)_ | Warm paths from a newline file (popularity order) instead of the sitemap; falls back to sitemap if missing/empty |
| `--max-consecutive-errors` | `50` | Abort threshold protecting a sick backend |

### Popularity-driven warming (`--urls-from`)

Warming the full sitemap nightly renders every long-tail page, including those
nobody visits. To warm only what gets traffic, feed a ranked path list:

```bash
ps-cache-warmer --domain funecobikes.com --urls-from /run/ps-warm/funecobikes.com.urls --max 1000
```

The list is generated from a host-aware varnishncsa log by `ps-warm-toplist`
(deployed by `playbooks/install_cache_warmer.yml`). One path per line, most
popular first; full URLs or host-relative paths both work. If the file is
missing or empty (e.g. a freshly deployed host with no traffic history yet) the
warmer falls back to a full sitemap walk.

It exits non-zero if it aborted on errors. A site whose sitemap is missing or
empty is a clean no-op (it logs and exits 0). Deployment (cron + binary) lives
in the `prestashop-servers` repo: `playbooks/install_cache_warmer.yml`.

## Build

```bash
cargo build --release
```

Releases ship a static `x86_64-unknown-linux-musl` binary packaged as
`ps-cache-warmer-linux-amd64.tar.gz` (built by `.github/workflows/release.yml`
on tag push).
