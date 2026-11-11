#!/usr/bin/env python3
"""Check the packaged UI with synthetic API responses; never uses a real workspace.

Requires the optional Playwright package and a Chromium install. Examples:
  python scripts/check_web_layout.py --output /tmp/openalex-web-check
  python scripts/check_web_layout.py --executable /path/to/chromium
"""
import argparse
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from playwright.sync_api import sync_playwright, expect

STATIC = Path(__file__).resolve().parents[1] / 'openalex_api' / 'static'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--executable')
    parser.add_argument('--output', type=Path, default=Path('/tmp/openalex-web-check'))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    refs = ['https://openalex.org/W3', 'https://openalex.org/W4', 'https://openalex.org/W5']
    records = {
        'W1': {'id': 'https://openalex.org/W1', 'title': 'Synthetic weather preprint', 'type': 'preprint', 'referenced_works': []},
        'W2': {'id': 'https://openalex.org/W2', 'title': 'Synthetic published weather paper', 'type': 'article', 'referenced_works': refs},
        'W6': {'id': 'https://openalex.org/W6', 'title': 'Synthetic missing reference field'},
    }
    state = {'failure': False, 'mismatch': False}
    ready = {e: {'ready': True} for e in ['works', 'authors', 'institutions', 'sources', 'topics']}
    ready['funders'] = {'ready': False}
    status = {'auth_required': False, 'search_ready': True, 'metadata_ready': True, 'metadata_complete': True,
              'generation': 'synthetic', 'coverage': {'works': {'unique_entities': 10}},
              'search_coverage': {'entities': ready, 'published_scopes': ['earth_sciences']}}

    def edge(wid):
        return {'id': 'https://openalex.org/' + wid, 'resolved': wid != 'W5',
                'work': {'id': 'https://openalex.org/' + wid, 'title': 'Synthetic reference ' + wid,
                         'doi': 'https://doi.org/10.0000/' + wid} if wid != 'W5' else None}

    def handle(route):
        url = urlsplit(route.request.url)
        path, params = url.path, parse_qs(url.query)
        if path == '/' or path.startswith('/assets/'):
            file = STATIC / ('index.html' if path == '/' else path.rsplit('/', 1)[-1])
            route.fulfill(path=str(file))
            return
        if path == '/status':
            data = status
        elif path == '/capabilities':
            data = {'exports': {'worker_ready': True}}
        elif path == '/v1/exports':
            data = {'results': []}
        elif path == '/v1/works/lookup':
            data = records[params['identifier'][0]]
        elif path == '/v1/works':
            data = {'results': list(records.values()), 'meta': {'count': 3}}
        elif path.endswith('/references'):
            if state['failure']:
                route.fulfill(status=503, json={'detail': 'Synthetic unavailable'})
                return
            wid = path.split('/')[-2]
            data = {'results': [] if wid != 'W2' or state['mismatch'] else ([edge('W5')] if params.get('cursor') else [edge('W3'), edge('W4')]),
                    'meta': {'next_cursor': 'next' if wid == 'W2' and not state['mismatch'] and not params.get('cursor') else None}}
        elif path.endswith('/citations'):
            data = {'results': [edge('W3')], 'meta': {'next_cursor': None}}
        elif path.endswith('/related-works'):
            data = {'results': [], 'meta': {}}
        else:
            raise AssertionError('Unexpected API request: ' + route.request.url)
        route.fulfill(json=data)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=args.executable)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.route('http://openalex.test/**', handle)
        page.goto('http://openalex.test/')
        page.wait_for_load_state('networkidle')
        expect(page.locator('#workspace')).to_be_visible()
        assert not page.locator('#advanced-filters').evaluate('(el)=>el.open')
        assert not page.locator('#export-panel').evaluate('(el)=>el.open')
        assert not page.locator('#other-collections').evaluate('(el)=>el.open')
        for width in [1440, 1024, 760, 390]:
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
            assert page.locator('#results-title').bounding_box()['y'] < 1000, width
            page.screenshot(path=str(args.output / f'home-{width}.png'), full_page=True)
        page.set_viewport_size({'width': 1440, 'height': 1000})

        def lookup(wid):
            page.locator('#query').fill(wid)
            page.locator('#search-button').click()
            expect(page.locator('.record-title')).to_have_text(records[wid]['title'])
            page.get_by_role('tab', name='引用关系', exact=True).click()
            expect(page.locator('#edge-list')).to_have_attribute('aria-busy', 'false')

        lookup('W1')
        expect(page.locator('[data-direction=references]')).to_have_text('参考文献 · 0')
        expect(page.locator('#edge-list')).to_contain_text('这不表示论文原文没有参考文献')
        page.locator('[data-direction=citations]').click()
        expect(page.locator('#edge-list')).to_contain_text('Synthetic reference W3')
        expect(page.locator('[data-direction=citations]')).to_have_attribute('aria-pressed', 'true')
        page.locator('[data-direction=related-works]').click()
        expect(page.locator('#edge-list')).to_contain_text('源记录未提供相关文献')
        lookup('W2')
        expect(page.locator('[data-direction=references]')).to_have_text('参考文献 · 3')
        expect(page.locator('#edge-list .edge-item')).to_have_count(2)
        page.locator('#more-edges').click()
        expect(page.locator('#edge-list .edge-item')).to_have_count(3)
        expect(page.locator('#edge-list')).to_contain_text('https://openalex.org/W5')
        expect(page.locator('#more-edges')).to_be_hidden()
        state['failure'] = True
        page.locator('[data-direction=references]').click()
        expect(page.locator('#edge-list')).to_contain_text('关系查询失败')
        expect(page.locator('#edge-list .edge-item')).to_have_count(0)
        state.update(failure=False, mismatch=True)
        page.locator('[data-direction=references]').click()
        expect(page.locator('#edge-list')).to_contain_text('参考关系与源记录不一致')
        lookup('W6')
        expect(page.locator('[data-direction=references]')).to_have_text('参考文献')
        expect(page.locator('#relation-note')).to_contain_text('未提供结构化参考文献列表')
        page.get_by_role('tab', name='概览', exact=True).focus()
        page.keyboard.press('ArrowRight')
        expect(page.get_by_role('tab', name='引用关系', exact=True)).to_have_attribute('aria-selected', 'true')
        page.goto('http://openalex.test/?author-query=Synthetic&oa-only=1')
        page.wait_for_load_state('networkidle')
        assert page.locator('#advanced-filters').evaluate('(el)=>el.open')
        expect(page.locator('#author-query')).to_have_value('Synthetic')
        expect(page.locator('#oa-only')).to_be_checked()
        # Expanded controls must also remain usable at phone width.
        page.locator('#other-collections>summary').click()
        page.set_viewport_size({'width': 390, 'height': 1000})
        page.locator('#export-panel>summary').click()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors, errors
        browser.close()
    result = {'passed': True, 'widths': [1440, 1024, 760, 390], 'synthetic_only': True,
              'checks': ['layout', 'disclosures', 'source-empty', 'missing-field', 'directions',
                         'pagination', 'unresolved-ID', 'request-error', 'source-mismatch',
                         'keyboard-tabs', 'query-URL'], 'page_errors': errors}
    (args.output / 'acceptance.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
