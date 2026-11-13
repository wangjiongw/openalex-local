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
    arxiv_record = dict(id='arxiv:1001.0001',source='arxiv',title='Synthetic arXiv climate paper',authors='A. Example',abstract='Climate research',categories=['cs.AI'],first_submitted='2010-01-01',latest_submitted='2010-02-01',version_links=dict(preferred_openalex_id='W2',confirmed=[]))
    state = {'failure': False, 'mismatch': False, 'work_filters': [], 'version_groups': False}
    directories={'domains':[dict(id='https://openalex.org/domains/3',display_name='Physical Sciences')],
                 'fields':[dict(id='https://openalex.org/fields/19',display_name='Earth and Planetary Sciences')],
                 'subfields':[dict(id='https://openalex.org/subfields/1907',display_name='Geology')]}

    ready = {e: {'ready': True} for e in ['works', 'authors', 'institutions', 'sources', 'topics']}
    ready['funders'] = {'ready': False}
    status = {'application_version': '0.4.0', 'auth_required': False, 'search_ready': True, 'metadata_ready': True, 'metadata_complete': True,
              'arxiv':dict(ready=True,count=1,min_year=2010,release='fixture'), 'generation': 'synthetic', 'coverage': {'works': {'unique_entities': 10}},
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
            data = {'exports': {'worker_ready': True},'small_directories':{'entities':list(directories)}}
        elif path.removeprefix('/v1/') in directories:
            data={'results':directories[path.removeprefix('/v1/')],'meta':{'count':1,'next_cursor':None}}
        elif path == '/v1/exports':
            data = {'results': []}
        elif path == '/v1/arxiv/works':
            data={'results':[arxiv_record],'meta':{'count':1}}
        elif path == '/v1/arxiv/lookup':
            data=arxiv_record
        elif path.endswith('/version-group'):
            wid=path.split('/')[-2]
            data=dict(relationship_release='versions_fixture',generation='synthetic',state='confirmed',group_id='vg_'+'1'*32,
                      requested_openalex_id=wid,requested_record=records[wid],members=['W1','W2'],preferred_openalex_id='W2',
                      evidence=[dict(source_id='arxiv:1001.0001',rule='unique_exact_doi')],
                      records=[dict(openalex_id=k,record=records[k]) for k in ['W1','W2']])
            if state.get('notice'):data.update(state='unmatched',group_id=None,automatic_display_allowed=False)
        elif path.endswith('/versions'):
            data={'results':[]}
        elif path == '/v1/works/lookup':
            data = records[params['identifier'][0]]
        elif path == '/v1/works':
            state['work_filters'].append(params.get('filter',[''])[0])
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
        expect(page.locator('#service-version')).to_have_text('v0.4.0')
        expect(page.locator('#export-selected')).to_be_disabled()
        assert not page.locator('#advanced-filters').evaluate('(el)=>el.open')
        assert not page.locator('#export-panel').evaluate('(el)=>el.open')
        assert not page.locator('#other-collections').evaluate('(el)=>el.open')
        for width in [1440, 1024, 760, 390]:
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
            assert page.locator('#results-title').bounding_box()['y'] < 1000, width
            page.screenshot(path=str(args.output / f'home-{width}.png'), full_page=True)
        page.set_viewport_size({'width': 1440, 'height': 1000})

        page.locator('#advanced-filters>summary').click()
        page.locator('#domain-filter').select_option('domains/3')
        page.locator('#field-filter').select_option('fields/19')
        page.locator('#subfield-filter').select_option('subfields/1907')
        page.locator('#search-button').click()
        expect(page.locator('#results .result-card')).to_have_count(3)
        assert 'domain:domains/3' in state['work_filters'][-1] and 'subfield:subfields/1907' in state['work_filters'][-1]
        page.reload();page.wait_for_load_state('networkidle')
        expect(page.locator('#domain-filter')).to_have_value('domains/3')
        expect(page.locator('#subfield-filter')).to_have_value('subfields/1907')
        page.locator('#clear-filters').click()

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
        status['version_groups']={'ready':True}
        page.reload();page.wait_for_load_state('networkidle')
        page.locator('#query').fill('W1');page.locator('#search-button').click()
        expect(page.locator('.record-title')).to_have_text(records['W2']['title'])
        expect(page.locator('.version-group')).to_contain_text('命中记录 W1 · 当前显示 W2')
        page.locator('.version-group').get_by_role('button',name='其他版本 · '+records['W1']['title'],exact=True).click()
        expect(page.locator('.record-title')).to_have_text(records['W1']['title'])
        expect(page.locator('.version-group')).to_contain_text('命中记录 W1 · 当前显示 W1')
        page.get_by_role('tab',name='引用关系',exact=True).click()
        expect(page.locator('[data-direction=references]')).to_have_text('参考文献 · 0')
        with page.expect_download() as download:
            page.get_by_role('button',name='下载版本组 JSON',exact=True).click()
        assert download.value.suggested_filename.startswith('vg_')
        for width in [1440,390]:
            page.set_viewport_size({'width':width,'height':1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.set_viewport_size({'width':1440,'height':1000})
        status['version_groups']={'ready':False}
        page.goto('http://openalex.test/?author-query=Synthetic&oa-only=1')
        page.wait_for_load_state('networkidle')
        assert page.locator('#advanced-filters').evaluate('(el)=>el.open')
        expect(page.locator('#author-query')).to_have_value('Synthetic')
        expect(page.locator('#oa-only')).to_be_checked()
        expect(page.locator('#advanced-summary')).to_have_text('已启用 2 项条件')
        page.locator('#query').fill('weather')
        page.locator('#clear-filters').click()
        expect(page.locator('#query')).to_have_value('weather')
        expect(page.locator('#author-query')).to_have_value('')
        expect(page.locator('#oa-only')).not_to_be_checked()
        expect(page.locator('#filter-summary')).to_have_text('使用默认检索设置')
        assert 'author-query' not in page.url and 'oa-only' not in page.url
        page.locator('#search-button').click()
        expect(page.locator('#results .result-card')).to_have_count(3)
        page.locator('#export-panel>summary').click()
        page.locator('#results input[type=checkbox]').first.check()
        expect(page.locator('#selected-count')).to_contain_text('1 篇')
        expect(page.locator('#export-selected')).to_be_enabled()
        expect(page.locator('#export-query')).to_be_enabled()
        page.locator('#select-page').click()
        expect(page.locator('#selected-count')).to_contain_text('3 篇')
        page.locator('#query').fill('changed')
        expect(page.locator('#export-query')).to_be_disabled()
        page.locator('#entity').select_option('authors')
        expect(page.locator('#export-selected')).to_be_disabled()
        expect(page.locator('#select-page')).to_be_disabled()
        page.locator('#entity').select_option('works')
        page.locator('#export-panel>summary').click()
        # Expanded controls must also remain usable at phone width.
        page.locator('#other-collections>summary').click()
        page.set_viewport_size({'width': 390, 'height': 1000})
        page.locator('#export-panel>summary').click()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.set_viewport_size({'width':1440,'height':1000})
        page.locator('#data-source').select_option('arxiv')
        expect(page.locator('#arxiv-category')).to_be_visible()
        expect(page.locator('#advanced-filters')).to_be_hidden()
        page.locator('#query').fill('climate')
        page.locator('#arxiv-author').fill('Example')
        page.locator('#search-button').click()
        expect(page.locator('#result-count')).to_contain_text('1 条 · arXiv')
        page.locator('#results .title-button').click()
        expect(page.locator('.record-title')).to_have_text(records['W2']['title'])
        page.get_by_role('button',name='当前为已确认发表版 · 切换到 arXiv 预印本').click()
        expect(page.locator('.record-title')).to_have_text(arxiv_record['title'])
        expect(page.locator('#record-pane')).to_contain_text('arXiv 元数据未提供参考文献列表')
        for width in [1440,390]:
            page.set_viewport_size({'width':width,'height':1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            page.screenshot(path=str(args.output/f'arxiv-{width}.png'),full_page=True)
        page.locator('#clear-filters').click()
        expect(page.locator('#data-source')).to_have_value('arxiv')
        page.locator('#data-source').select_option('openalex')
        expect(page.locator('#advanced-filters')).to_be_visible()
        status['version_groups']={'ready':True};state['notice']=True
        arxiv_record['version_links']['confirmed']=[dict(openalex_id='W2',kind='publication')]
        page.reload();page.wait_for_load_state('networkidle')
        page.locator('#data-source').select_option('arxiv')
        page.locator('#query').fill('1001.0001');page.locator('#search-button').click()
        expect(page.locator('.record-title')).to_have_text(arxiv_record['title'])
        assert not errors, errors
        browser.close()
    result = {'passed': True, 'widths': [1440, 1024, 760, 390], 'synthetic_only': True,
              'checks': ['layout', 'disclosures', 'source-empty', 'missing-field', 'directions',
                         'pagination', 'unresolved-ID', 'request-error', 'source-mismatch',
                         'keyboard-tabs', 'query-URL', 'filter-reset', 'active-conditions', 'selection-count', 'service-version','taxonomy-hierarchy','taxonomy-filter-payload','taxonomy-URL', 'version-preference', 'manual-version-selection', 'version-reference-provenance', 'version-group-download', 'single-target-notice-no-auto-jump'], 'page_errors': errors}
    (args.output / 'acceptance.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
