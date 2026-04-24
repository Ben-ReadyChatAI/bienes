#!/usr/bin/env python3
"""Capture screenshots of the deployed Bienes UI for the /guide page.

Run from the project root:
    python tools/screenshot_guide.py [--url <url>]

Output goes to static/guide/*.png — these are committed to the repo so the
guide page can reference them.
"""

import argparse
import asyncio
import sys
from pathlib import Path

from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parent.parent
OUTDIR = ROOT / "static" / "guide"
OUTDIR.mkdir(parents=True, exist_ok=True)

VIEWPORT = {"width": 1280, "height": 850}


async def capture(url):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            viewport=VIEWPORT,
            device_scale_factor=2,  # crisp screenshots
            ignore_https_errors=True,
        )
        page = await ctx.new_page()

        async def shot(name, *, full=False):
            path = OUTDIR / f"{name}.png"
            await page.screenshot(path=str(path), full_page=full)
            print(f"  ✓ {name}.png ({path.stat().st_size // 1024} KB)")

        # 1. MAIN — idle
        print("\n[1/5] Main page · idle state")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector(".wordmark", timeout=10000)
        await page.wait_for_timeout(800)  # let SSE snapshot arrive
        await shot("01-main-idle")
        await shot("01b-main-idle-full", full=True)

        # 2. MAIN — focused on the form (zoomed crop area)
        print("\n[2/5] Form close-up")
        # Scroll to top to ensure form is visible
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(300)
        # Crop to left column where the form is (roughly first 380px wide region)
        form_locator = page.locator(".col-left").first
        if await form_locator.count() > 0:
            await form_locator.screenshot(path=str(OUTDIR / "02-form-detail.png"))
            print(f"  ✓ 02-form-detail.png")

        # 3. MAIN — running (kick off a search and capture mid-run)
        print("\n[3/5] Main page · running state")
        # Click the run button
        await page.click("#runbtn")
        # Wait for the status to flip to running, then snap
        try:
            await page.wait_for_selector(".statusblock.running", timeout=8000)
            await page.wait_for_timeout(2500)  # let some log lines arrive
            await shot("03-main-running")
        except Exception as e:
            print(f"  ! couldn't catch running state: {e}")

        # 4. MAIN — done with results
        print("\n[4/5] Main page · finished with results (waiting up to 90s)")
        try:
            await page.wait_for_selector(".statusblock.done", timeout=90000)
            await page.wait_for_timeout(2500)  # let results render
            await shot("04-main-done")
            await shot("04b-main-done-full", full=True)
        except Exception as e:
            print(f"  ! run didn't finish: {e}")
            await shot("04-main-state-fallback")

        # 5. DISCOVER page
        print("\n[5/5] Discover page")
        await page.goto(f"{url.rstrip('/')}/discover", wait_until="domcontentloaded")
        await page.wait_for_selector(".wordmark", timeout=10000)
        await page.wait_for_timeout(500)
        await shot("05-discover-idle")
        await shot("05b-discover-idle-full", full=True)

        await browser.close()
        print(f"\n✓ All screenshots in {OUTDIR}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="https://habitablog.readychatai.lat",
                    help="Bienes URL to screenshot")
    args = ap.parse_args()
    print(f"Capturing screenshots from {args.url}")
    asyncio.run(capture(args.url))


if __name__ == "__main__":
    main()
