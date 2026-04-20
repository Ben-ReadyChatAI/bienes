# Deploying BIENES to Coolify + Cloudflare Access

## What you'll have when done

A private URL like `https://bienes.habitaone.com` that:
- Serves the BIENES GUI (run pipeline / discover seeds / view shortlists)
- Requires Cloudflare Access login (email OTP / Google / GitHub) before the request hits Coolify
- Persists cache + run history across redeploys
- Costs ~$0 in CF (free tier covers Access for up to 50 users)

## 1. Push to a Git repo (private!)

```bash
cd "~/Desktop/Backlinker/Blog Planner"
git init
git add .gitignore .dockerignore Dockerfile docker-compose.yml requirements.txt \
        *.py tools/ DEPLOY.md
# Verify .env is NOT staged:
git status | grep .env && echo "STOP — .env is staged, .gitignore is broken" || echo "OK"
git commit -m "Initial commit"
# Push to a PRIVATE repo
gh repo create bienes --private --source=. --push   # GitHub CLI
# OR manually: git remote add origin … && git push
```

**Critical**: The `.env` file holds your API keys. The `.gitignore` and `.dockerignore` exclude it, but verify with `git status` before every push.

## 2. Set up the Coolify app

In your Coolify dashboard:

1. **New Resource → Application → Public Repository** (or Private with GitHub PAT)
2. **Build Pack**: choose **Docker Compose** (uses your `docker-compose.yml`)
   - Or **Dockerfile** if you prefer single-container
3. **Port**: `5055`
4. **Environment Variables** — add each key from `.env` via Coolify's env-var UI:
   - `SERPER_API_KEY`
   - `BRAVE_API_KEY`
   - `GOOGLE_CSE_API_KEY`
   - `GOOGLE_CSE_ID`
   - `OPR_API_KEY`
   - `TAVILY_API_KEY`
5. **Persistent Storage**: mount the `bienes-output` volume to `/app/output` (Coolify UI lets you add named volumes)
6. **Domain**: assign `bienes.habitaone.com` (or any subdomain you control)
7. Hit **Deploy**

First build takes ~2 min. Subsequent deploys ~30 s thanks to Docker layer caching.

## 3. Cloudflare Access (the auth wall)

You need:
- A Cloudflare account managing the `habitaone.com` DNS zone (or whatever domain)
- Cloudflare Zero Trust enabled (free for up to 50 users)

In the Cloudflare dashboard:

1. **Zero Trust → Access → Applications → Add Application**
2. Type: **Self-hosted**
3. Application name: `BIENES`
4. Subdomain: `bienes` · Domain: `habitaone.com`
5. Session duration: `24 hours` (or whatever)
6. **Add an Access Policy**:
   - Policy name: `team`
   - Action: `Allow`
   - Include: choose one of —
     - `Emails ending in @habitaone.com` (broadest — anyone in your domain)
     - `Emails` listing specific addresses (tightest — only you + invited)
     - `GitHub` org membership (if you use GitHub orgs)
7. **Save**

Now any HTTP request to `bienes.habitaone.com` triggers Cloudflare's login UI before reaching Coolify. No code change in the app.

## 4. (Optional) Show logged-in user in the GUI

Cloudflare Access adds a header `Cf-Access-Authenticated-User-Email` to every request. To surface it in the UI, add this to `gui.py` near the top:

```python
from flask import g

@app.before_request
def grab_cf_user():
    g.cf_email = request.headers.get('Cf-Access-Authenticated-User-Email', '')
```

Then thread `g.cf_email` into the `<div class="ticker">` in `PAGE` (e.g., `<span>Editor · {g.cf_email}</span>`). Skip if you're the only user.

## 5. (Optional) IP-restrict the Coolify origin

Cloudflare Access protects the public URL, but the Coolify origin (e.g., `coolify.yourserver.com:port`) is still reachable directly. To prevent bypassing Access:

- In your Coolify proxy / Caddy / Traefik config, restrict the bienes route to only accept connections from Cloudflare IPs (https://www.cloudflare.com/ips/)
- Or set up Cloudflare Tunnel (`cloudflared`) so the origin only listens on the tunnel and isn't publicly reachable at all — strongest option

For most setups, the public URL behind Cloudflare proxy + Access is sufficient.

## 6. Resource sizing

| Metric | Value |
|---|---|
| Image size | ~150 MB |
| Idle RAM | ~80 MB (gunicorn + Python) |
| Peak RAM during pipeline | ~250 MB (subprocess) |
| CPU | minimal, network-bound |
| Disk | ~50 MB image + ~20 MB persistent cache |

A `1 GB / 1 vCPU` Coolify slot is more than enough.

## 7. Updating the deployment

After code changes:

```bash
git add . && git commit -m "..."
git push
```

In Coolify, click **Redeploy** (or set up auto-deploy on push). The persistent volume keeps cache + history across deploys.

## 8. Local testing of the production setup

Before pushing, verify the container builds and runs locally:

```bash
docker compose up --build
# Open http://localhost:5055
# Verify the form works, status updates, etc.
docker compose down
```

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| 502 from Cloudflare | Coolify not running / port mismatch | Check Coolify logs; verify port 5055 |
| Pipeline times out | Gunicorn worker timeout | Already set to 600s in Dockerfile; bump if needed |
| API keys missing in app | env-vars not set in Coolify | Re-check Coolify env-var UI |
| Cache lost after redeploy | Volume not mounted | Verify `bienes-output → /app/output` mount |
| Loading shortlists hangs | output/ permissions | `RUN chmod -R 777 /app/output` in Dockerfile (or fix UID) |

## 10. Cost estimate

- Coolify VPS: whatever you already pay
- Cloudflare Access: **$0** (free tier covers up to 50 users)
- Cloudflare Tunnel: **$0**
- DNS: **$0** (already managing the domain)
- API costs: same as local — Serper $0.04/run, Brave free 2k/mo, OPR free 1k/day

Net additional cost: **$0** if you already have Coolify + Cloudflare set up.
