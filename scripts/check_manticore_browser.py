"""Exercise the deployed UI using the existing temporary Playwright installation."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright
from openalex_api.config import config, ROOT

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--base-url", default="http://127.0.0.1:18088")
parser.add_argument("--output", type=Path, default=ROOT / "reports/manticore-implementation-20261106")
args = parser.parse_args()
out = args.output
out.mkdir(parents=True, exist_ok=True)
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, executable_path="/tmp/openalex-browser/chromium_headless_shell-1223/chrome-linux/headless_shell",
        args=["--no-sandbox"])
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(args.base_url)
    page.wait_for_load_state("networkidle")
    page.locator("#api-key").fill(config()["api_keys"][0])
    page.locator("#connect-form").evaluate("form => form.requestSubmit()")
    page.locator("#workspace").wait_for(state="visible")
    page.wait_for_load_state("networkidle")
    for selector in ("#topic-filter", "#source-filter", "#earth-group", "#type-filter", "#primary-topic"):
        assert page.locator(selector).is_visible()
    page.locator("#query").fill("10.1038/nature06095")
    page.locator("#search-button").click()
    page.locator("#detail .record-title").wait_for(state="visible", timeout=30000)
    page.wait_for_load_state("networkidle")
    assert page.locator("#detail .record-title").count(), page.locator("#message").inner_text()
    with page.expect_response(lambda r: "/references?" in r.url, timeout=20000) as references:
        page.get_by_role("tab", name="引用关系", exact=True).click()
    assert references.value.status == 200
    page.wait_for_load_state("networkidle")
    target = page.locator("#edge-list .title-button").first
    target.wait_for(state="visible")
    title = target.inner_text()
    target.click()
    page.wait_for_function("title => document.querySelector('#detail .record-title')?.textContent === title", arg=title)
    page.locator("#query").fill("climate")
    page.locator("#source-filter").fill("Geology")
    page.locator("#source-options option[value='S56162041']").wait_for(state="attached", timeout=15000)
    assert page.locator("#source-options option").first.get_attribute("value") == "S56162041"
    page.locator("#source-filter").fill("S56162041")
    page.locator("#earth-group").select_option("01_core_journals")
    with page.expect_response(lambda r: "/v1/works?" in r.url, timeout=20000) as pending:
        page.locator("#search-button").click()
    response = pending.value
    assert response.status == 200 and response.json()["results"]
    page.wait_for_load_state("networkidle")
    assert page.locator("#results .result-card").count()
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth + 1"), "desktop horizontal overflow"
    page.screenshot(path=str(out / "browser-desktop.png"), full_page=True)
    page.set_viewport_size({"width": 390, "height": 844})
    page.screenshot(path=str(out / "browser-mobile.png"), full_page=True)
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth + 1"), "horizontal overflow"
    assert not errors, errors
    (out / "browser.json").write_text(json.dumps(dict(passed=True, errors=errors,
        base_url=args.base_url, checked=["authenticated connection", "filter controls", "real DOI detail",
        "reference expansion and detail navigation", "exact journal name suggestion", "Earth title plus journal/group search", "desktop/mobile overflow"],
        limitations=["browser runs in the server Pod; cross-Pod API acceptance is recorded separately"]), indent=2))
    browser.close()
