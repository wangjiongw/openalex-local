"use strict";
const $ = id => document.getElementById(id);
const labels = {works:"作品",authors:"作者",institutions:"机构",sources:"期刊与来源",publishers:"出版商",funders:"资助方",awards:"资助项目",topics:"主题",keywords:"关键词",concepts:"概念",domains:"学科领域",fields:"学科",subfields:"子学科",countries:"国家与地区",continents:"大洲",languages:"语言",licenses:"许可协议","institution-types":"机构类型","source-types":"来源类型","work-types":"作品类型",sdgs:"可持续发展目标"};
let key = sessionStorage.getItem("openalex-key") || "", status = null, nextResults = null, record = null, recordText = "", recordEntity = "works", edgeCursor = null, edgeDirection = "references", busy = false, detailRequest = 0, edgeRequest = 0;
let capabilities = null, lastQuery = null, searchRevision = 0;
const selectedWorks = new Set(), loadedWorks = new Set();
const taxonomyControls = {domains:"domain-filter",fields:"field-filter",subfields:"subfield-filter"};
const directoryWorkControls = {...taxonomyControls,countries:"country-filter",languages:"language-filter",licenses:"license-filter","work-types":"type-filter"};
const hasDirectory = entity => capabilities?.small_directories?.entities?.includes(entity);
const directoryValue = (entity,id) => taxonomyControls[entity] ? id.replace(/^https?:\/\/openalex\.org\//i,"") : id.split("/").pop();
let taxonomyRevision=0;
const stateFields = ["data-source","arxiv-category","arxiv-author","entity","query","search-field","search-scope","author-query","author-id-filter","institution-filter","language-filter","domain-filter","field-filter","subfield-filter","country-filter","license-filter","year","sort","topic-filter","primary-topic","source-filter","type-filter","earth-group","oa-only"];
const fmt = n => Number(n).toLocaleString("zh-CN");
function node(tag, text, cls) { const el = document.createElement(tag); if (text !== undefined) el.textContent = text; if (cls) el.className = cls; return el; }
function button(text, action, cls = "secondary") { const el = node("button", text, cls); el.type = "button"; el.addEventListener("click", action); return el; }
function message(text = "") { $("message").textContent = text; }
function updateSelection() {
  const works = $("entity").value === "works";
  $("selected-count").textContent = `当前结果已勾选 ${fmt(selectedWorks.size)} 篇文献。`;
  $("export-selected").disabled = !works || !selectedWorks.size;
  $("select-page").disabled = !works || !loadedWorks.size;
  $("export-query").disabled = !works || !lastQuery || busy;
}
function updateFilterSummary() {
  const controls=[...$("search-form").querySelectorAll(".filters input,.filters select")];
  const active=el=>!el.disabled && (el.type === "checkbox" ? el.checked : el.tagName === "SELECT" ? el.selectedIndex > 0 : !!el.value.trim());
  const count=controls.filter(active).length;
  const advanced=controls.filter(el=>el.closest("#advanced-filters") && active(el)).length;
  $("filter-summary").textContent=count ? `已启用 ${count} 项筛选或检索设置` : "使用默认检索设置";
  $("advanced-summary").textContent=advanced ? `已启用 ${advanced} 项条件` : "作者、机构、主题、期刊与开放获取";
}
function invalidateQuery() {
  searchRevision++;nextResults=null;lastQuery=null;$("more-results").hidden=true;updateFilterSummary();updateSelection();
}
async function request(path, params, payload) {
  const url = new URL(path, location.origin); if (params) url.search = new URLSearchParams(params).toString();
  const res = await fetch(url, {method:payload === undefined ? "GET" : "POST",headers:{...(key ? {"X-API-Key":key} : {}),"Content-Type":"application/json"},body:payload === undefined ? undefined : JSON.stringify(payload)}); const text = await res.text(); let data;
  try { data = JSON.parse(text); } catch { throw new Error("服务返回了无法读取的响应，请稍后重试。"); }
  if (!res.ok && res.status !== 409) {
    if (res.status === 401) throw new Error("API 密钥无效，请断开连接后重新输入。");
    if (res.status === 404) throw new Error("当前快照中没有找到这条记录。");
    if (res.status === 429) throw new Error("请求较频繁，请稍候再试。");
    if (data.detail?.code === "search_not_ready") throw new Error("文本检索尚未就绪。当前可以通过 DOI 或 OpenAlex ID 查看完整记录。");
    throw new Error(typeof data.detail === "string" ? data.detail : "查询暂时不可用，请检查服务状态或稍后重试。");
  }
  return {data, text, ambiguous:res.status === 409};
}
function safeLink(url, text) {
  try { const u = new URL(url); if (!["http:","https:"].includes(u.protocol)) return node("span", text); const a = node("a", text); a.href = u.href; a.target = "_blank"; a.rel = "noopener noreferrer"; return a; } catch { return node("span", text); }
}
function identifier(value, entity) {
  let v = value.trim().replace(/^https?:\/\/(?:api\.)?openalex\.org\//i, "");
  if (/^(?:https?:\/\/(?:dx\.)?doi\.org\/|doi:\s*)?10\.\d{4,9}\//i.test(v)) return {entity:"works", scheme:"doi", value:v};
  const prefix = {W:"works",A:"authors",I:"institutions",S:"sources",P:"publishers",F:"funders",G:"awards",T:"topics",C:"concepts"};
  if (/^[WAISPFGTC]\d+$/i.test(v) && prefix[v[0].toUpperCase()]) return {entity:prefix[v[0].toUpperCase()], scheme:"openalex", value:v};
  const family = v.split("/")[0];
  if (labels[family] && v.includes("/")) return {entity:family, scheme:"openalex", value:v};
  if (!hasDirectory(entity) && !status?.search_ready && entity !== "works" && !["authors","institutions","sources","publishers","funders","awards","topics","concepts"].includes(entity)) return {entity, scheme:"openalex", value:v};
  return null;
}
function chooseEntity(entity) {
  $("entity").value = entity;
  if (status?.search_coverage?.entities?.[entity]?.ready === false) $("other-collections").open = true;
  $("search-scope").disabled = entity !== "works";
  const works = entity === "works";
  const entityReady = status?.search_coverage?.entities?.[entity]?.ready;
  if (hasDirectory(entity)) $("search-note").textContent="浏览已导入的小型实体目录；名称按字面子串匹配，不依赖文本索引。关联文献仍受已发布检索范围约束。";
  else if (entityReady === false) $("search-note").textContent = `${labels[entity]}尚未建立独立名称索引；仍可通过 OpenAlex ID 或适用的标识符查看完整记录。`;
  else if (status?.search_ready) $("search-note").textContent = works ? "搜索当前已上线的文献范围；可固定选择广义地球科学。DOI 和 ID 定位覆盖全库。" : "在当前已上线的数据中检索名称；标识符定位覆盖全库。";
  if(entity === "authors" && status?.search_coverage?.catalog_scopes?.authors) {const counts=status.search_coverage.catalog_counts?.authors;$("search-note").textContent="作者名称目录覆盖与已发布地学候选文献关联的作者；标识符详情仍覆盖全部已导入作者。"+(counts?`已索引 ${fmt(counts.indexed_entities)} / ${fmt(counts.selected_ids)} 个关联 ID，${fmt(counts.missing_entities)} 个缺少实体元数据。`:"");}
  for (const el of [$("year"),$("author-query"),$("oa-only"),$("sort"),$("topic-filter"),$("primary-topic"),$("source-filter"),$("type-filter"),$("earth-group"),$("author-id-filter"),$("institution-filter"),$("language-filter"),...Object.values(taxonomyControls).map($),$("country-filter"),$("license-filter")]) el.disabled = !works;
  if (!works) { $("author-query").value = ""; $("year").value = ""; $("oa-only").checked = false; $("sort").value = ""; }
  for (const option of $("search-field").options) option.disabled = !works && !["all","title"].includes(option.value);
  if ($("search-field").selectedOptions[0].disabled) $("search-field").value = "all";
  document.querySelectorAll(".collection-item").forEach(el => el.classList.toggle("active", el.dataset.entity === entity));
  updateSource();
  updateFilterSummary();
}
async function refreshStatus() {
  const result = await request("/status"); status = result.data;
  capabilities = (await request("/capabilities")).data;
  $("export-note").textContent = capabilities.exports?.worker_ready ? "导出 worker 正在运行。" : "导出 worker 未运行；任务将排队，请在服务器启动 export-worker。";
  for (const [id,entity] of [["author-id-filter","authors"],["institution-filter","institutions"]]) $(id).placeholder = status.search_coverage?.entities?.[entity]?.ready ? "输入名称选择身份，或直接填写 ID" : "名称索引未发布；可直接填写 ID";
  $("service-version").textContent = status.application_version ? "v"+status.application_version : "";
  $("snapshot-date").textContent = status.snapshot_date || "本地快照"; $("generation").textContent = status.generation;
  const coverage = status.coverage || {};
  $("works-count").textContent = fmt(coverage.works?.unique_entities || 0); $("authors-count").textContent = fmt(coverage.authors?.unique_entities || 0); $("entity-count").textContent = Object.keys(coverage).length;
  const searchCoverage = status.search_coverage || {};
  $("coverage-note").textContent = !status.search_ready ? "当前提供元数据与引用关系查询；文本索引为可选功能。" : searchCoverage.engine === "manticore" ? `已上线 ${fmt(searchCoverage.entities?.works?.indexed_entities || 0)} / ${fmt(searchCoverage.earth_expected)} 篇地学候选文献 · ${searchCoverage.earth_complete ? "地学集合完整" : "分批上线，当前未命中不代表全库没有"}` : "DOI、详情和引用关系覆盖完整快照；名称检索按验收后的范围逐批上线。";
  for (const option of $("earth-group").options) option.disabled = !!option.value && !(searchCoverage.indexed_groups?.[option.value] > 0);
  if ($("earth-group").selectedOptions[0].disabled) $("earth-group").value = "";
  $("search-scope").disabled = $("entity").value !== "works";
  $("availability").textContent = !status.metadata_ready ? "元数据服务暂时不可用" : `${status.metadata_complete ? "全量元数据可查" : "部分覆盖元数据可查"} · ${status.search_ready ? `文本检索可用 · ${searchCoverage.global_search_complete ? "全库" : (searchCoverage.published_scopes || []).join("、").replace("earth_sciences","广义地球科学")}` : "未发布文本索引"}`;
  $("search-note").textContent = status.search_ready ? "支持题名、摘要、作者姓名及组合筛选；输入 DOI 或 OpenAlex ID 可直接定位。" : "可通过 DOI / OpenAlex ID 查询详情与引用关系；按需构建并发布索引后可启用文本检索。";
  $("collections").replaceChildren();
  $("metadata-collections").replaceChildren();
  for (const [entity,label] of Object.entries(labels)) {
    const el = button("", () => {chooseEntity(entity); invalidateQuery(); $("query").focus();}, "collection-item"); el.dataset.entity = entity;
    el.append(node("span", label), node("small", coverage[entity] ? fmt(coverage[entity].unique_entities) : "—")); const ready = hasDirectory(entity) || searchCoverage.entities?.[entity]?.ready; el.title = ready ? "可按名称检索，或按 ID 查看详情" : "名称索引未发布；可按 ID 查看完整详情"; $(ready || entity === "works" ? "collections" : "metadata-collections").append(el);
  }
  await loadTaxonomy();
  chooseEntity($("entity").value);
}
async function connect(event) {
  event?.preventDefault(); const supplied = $("api-key").value.trim(); if (supplied) key = supplied;
  message();
  try { await refreshStatus(); if(status.auth_required === false) {key = "";sessionStorage.removeItem("openalex-key");} else sessionStorage.setItem("openalex-key", key); $("api-key").value = ""; $("workspace").hidden = false; $("connection").hidden = true; $("disconnect").hidden = status.auth_required === false; }
  catch (error) { message(error.message); }
}
function card(r, entity) {
  const article = node("article", undefined, "result-card"); article.dataset.id = r.id; const meta = node("div", undefined, "result-meta");
  meta.append(node("span", r.publication_year || labels[entity]), node("span", r.type || "")); if (r.open_access?.is_oa) meta.append(node("span", "OPEN ACCESS", "tag oa"));
  const title = node("h3"); title.append(button(r.title || r.display_name || r.id, () => loadDetail(entity, r.id), "title-button"));
  article.append(meta,title);
  if(entity === "works") { loadedWorks.add(r.id); const label=node("label",undefined,"check"); const box=node("input");box.type="checkbox";box.checked=selectedWorks.has(r.id);box.addEventListener("change",()=>{if(box.checked)selectedWorks.add(r.id);else selectedWorks.delete(r.id);updateSelection();});label.append(box,node("span","选择导出"));article.append(label); }
  if(entity === "authors" || entity === "institutions") article.append(node("p",[r.orcid,r.ror,r.country_code,...(r.last_known_institutions||[]).map(i=>i.display_name)].filter(Boolean).join(" · ")));
  if(r.source === "arxiv") article.append(node("p",r.authors),node("p",`arXiv · ${(r.categories||[]).join(" · ")} · 首次提交 ${r.first_submitted?.slice(0,10)}`));
  const names = (r.authorships || []).slice(0,3).map(a=>a.author?.display_name).filter(Boolean);
  if (names.length) article.append(node("p", names.join(" · ") + (r.authorships.length > 3 ? " 等" : "")));
  if (r.primary_location?.source?.display_name) article.append(node("p",r.primary_location.source.display_name));
  article.append(node("p",r.doi || (entity === "works" ? "源记录未提供 DOI" : r.id))); if (r.cited_by_count != null) article.append(node("span",`源被引数 ${fmt(r.cited_by_count)}`,"tag"));
  return article;
}
async function search(event, more = false) {
  event?.preventDefault(); if (busy) return; busy = true; $("search-button").disabled = true; $("more-results").disabled = true; message();
  const revision = searchRevision;
  try {
    if ($("data-source").value === "arxiv") { await searchArxiv(more,revision); return; }
    const value = $("query").value.trim(), entity = $("entity").value;
    if(!more){selectedWorks.clear();loadedWorks.clear();lastQuery=null;saveQuery();}
    if (!more) {
      const id = !$("author-query").value.trim() ? identifier(value, entity) : null;
      if (id) { nextResults = null; $("more-results").hidden = true; chooseEntity(id.entity); await loadDetail(id.entity,id.value,id.scheme,true); return; }
      if (!hasDirectory(entity) && (value || $("author-query").value.trim() || $("earth-group").value) && (!status?.search_ready || status?.search_coverage?.entities?.[entity]?.ready === false)) throw new Error("文本检索尚未就绪，请输入 DOI 或 OpenAlex ID。全部原始字段均可在详情中查看。");
    }
    if (!more && !hasDirectory(entity) && !Object.values(directoryWorkControls).some(id=>$(id).value.trim()) && !value && !$("author-query").value.trim() && !$("year").value.trim() && !$("oa-only").checked && !$("topic-filter").value.trim() && !$("source-filter").value.trim() && !$("type-filter").value && !$("author-id-filter").value.trim() && !$("institution-filter").value.trim() && !$("language-filter").value.trim() && !$("earth-group").value) throw new Error("请输入检索词、作者或筛选条件。");
    const filters = [];
    if (entity === "works") { if ($("year").value.trim()) filters.push("year:"+$("year").value.trim()); if ($("oa-only").checked) filters.push("is_oa:true"); }
    if (entity === "works") {
      for (const [id,field,pattern] of [["topic-filter",$("primary-topic").checked ? "primary_topic" : "topic",/^T\d+$/i],["source-filter","source",/^S\d+$/i],["author-id-filter","author",/^A\d+$/i],["institution-filter","institution",/^I\d+$/i]]) {
        const value = $(id).value.trim().replace(/^https?:\/\/openalex\.org\//i, "");
        if (value && !pattern.test(value)) throw new Error("请从名称候选中选择，或直接填写相应的 OpenAlex ID。");
        if (value) filters.push(field + ":" + value.toUpperCase());
      }
      for(const [entity,id] of Object.entries(taxonomyControls)) {
        const value=$(id).value;
        if(value) {if(!new RegExp("^"+entity+"/\\d+$").test(value))throw new Error("请选择有效学科 ID。");filters.push(({domains:"domain",fields:"field",subfields:"subfield"})[entity]+":"+value);}
      }
      const country=$("country-filter").value.trim();
      if(country) {if(!/^[A-Z]{2}$/i.test(country))throw new Error("国家请填写两位代码，例如 US。");filters.push("country:"+country.toUpperCase());}
      const license=$("license-filter").value.trim();
      if(license) {if(!/^[a-z0-9-]+$/i.test(license))throw new Error("许可请从目录选择代码。");filters.push("license:"+license.toLowerCase());}
      if ($("language-filter").value.trim()) {if(!/^[a-z]{2}$/i.test($("language-filter").value.trim()))throw new Error("语言请填写两位代码，例如 en。");filters.push("language:"+$("language-filter").value.trim().toLowerCase());}
      if ($("type-filter").value) filters.push("type:" + $("type-filter").value);
      if ($("earth-group").value) filters.push("earth_group:" + $("earth-group").value);
    }
    const params = more ? {...nextResults.params,cursor:nextResults.cursor} : {search:value,search_scope:entity === "works" ? $("search-scope").value : "available",search_field:$("search-field").value,per_page:"20",select:"id,title,display_name,publication_year,doi,cited_by_count,type,authorships,primary_location,open_access,orcid,ror,country_code,last_known_institutions"};
    if (!more) { if ($("author-query").value.trim()) params.author_search = $("author-query").value.trim(); if (filters.length) params.filter = filters.join(","); if ($("sort").value) params.sort = $("sort").value; }
    const usedEntity = more ? nextResults.entity : entity;
    let route = more ? nextResults.route : `/v1/${usedEntity}`;
    if (!more && !status?.search_ready && !value && !$("author-query").value.trim()) {
      route = `/${usedEntity}`;
      delete params.search; delete params.search_scope; delete params.search_field;
      const aliases = {year:"publication_year",is_oa:"open_access.is_oa",topic:"topics.id",primary_topic:"primary_topic.id",source:"locations.source.id",author:"authorships.author.id",institution:"authorships.institutions.id",language:"language"};
      if (params.filter) params.filter = params.filter.split(",").map(x=>{const [field,...rest]=x.split(":");return (aliases[field] || field)+":"+rest.join(":");}).join(",");
      if (params.sort) params.sort = params.sort.replace(/^year:/,"publication_year:");
    }
    const {data} = await request(route, params);
    if(revision !== searchRevision)return;
    if(usedEntity === "works") lastQuery={route,params:Object.fromEntries(Object.entries(params).filter(([k])=>!["cursor","per_page","select"].includes(k)))};
    if (!more) $("results").replaceChildren();
    for (const r of data.results) $("results").append(card(r,usedEntity));
    if (!data.results.length && !more) $("results").append(node("p",data.meta.earth_complete === false ? "当前已上线集合没有匹配；其余地学文献仍在逐批上线。" : "没有匹配的记录。可以调整检索词或筛选条件。","empty"));
    const coverageLabel = data.meta.search_scope ? ` · ${data.meta.search_scope === "earth_sciences" ? "广义地球科学" : "全部已上线范围"}` : "";
    $("result-count").textContent = `${data.meta.count_relation === "gte" ? "≥ " : ""}${fmt(data.meta.count)} 条${coverageLabel}`;
    nextResults = data.meta.next_cursor ? {entity:usedEntity,route,params:Object.fromEntries(Object.entries(params).filter(([k])=>k!=="cursor")),cursor:data.meta.next_cursor} : null;
    $("more-results").hidden = !nextResults;
  } catch (error) { message(error.message); }
  finally { busy = false; $("search-button").disabled = false; $("more-results").disabled = false; updateSelection(); }
}
async function loadDetail(entity, value, scheme = "openalex", replaceResults = false, preferPublication = true) {
  if (String(value).startsWith("arxiv:")) return loadArxiv(value);
  const ticket = ++detailRequest; edgeRequest++; message();
  try {
    const response = await request(`/v1/${entity}/lookup`,{scheme,identifier:value}); if (ticket !== detailRequest) return;
    if (response.ambiguous) { $("results").replaceChildren(...response.data.candidates.map(r=>card(r,entity))); $("result-count").textContent = "标识符对应多个记录"; return; }
    record = response.data; recordText = response.text; recordEntity = entity; edgeCursor = null;
    if(replaceResults){$("results").replaceChildren(card(record,entity));$("result-count").textContent="1 条 · 精确定位";}
    updateSelection();
    renderDetail();
    if(entity === "works" && status?.arxiv?.ready) {
      try {
        const linked=(await request(`/v1/works/${record.id.split("/").pop()}/versions`)).data;
        if(ticket!==detailRequest)return;
        const preferences=[...new Set(linked.results.map(r=>r.version_links?.preferred_openalex_id).filter(Boolean))];
        if(preferPublication && preferences.length===1 && !record.id.endsWith("/"+preferences[0]) && record.id!==preferences[0]) {await loadDetail("works",preferences[0],"openalex",false,false);return;}
        for(const r of linked.results)$("detail").prepend(button("已确认关联 · 查看 arXiv 预印本",()=>loadArxiv(r.id,false,false)));
      } catch(error) {if(ticket===detailRequest)$("detail").append(node("p","版本关联暂不可用："+error.message,"subtle"));}
    }
    document.querySelectorAll(".result-card").forEach(el => el.classList.toggle("selected", el.dataset.id === record.id));
    if (innerWidth < 761) $("detail").scrollIntoView({behavior:matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth"});
  } catch (error) { if (ticket === detailRequest) message(error.message); }
}
function entityButton(entity, item) { return button(item.display_name || item.id, () => loadDetail(entity,item.id), "entity-link"); }
function renderDetail() {
  const root = $("detail"); root.replaceChildren();
  const top = node("div",undefined,"detail-top"); top.append(node("span",`${recordEntity === "arxiv" ? "arXiv 预印本" : labels[recordEntity]} / FULL RECORD`,"folio"),button("下载 JSON",()=>{
    const url = URL.createObjectURL(new Blob([recordText],{type:"application/json;charset=utf-8"})); const a=node("a");a.href=url;a.download=(record.id?.split("/").pop() || "record")+".json";a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
  },"secondary"));
  root.append(top,node("h2",record.title || record.display_name || record.id,"record-title"));
  const meta = node("div",undefined,"record-meta");
  if (record.publication_year) meta.append(node("span",record.publication_year,"tag")); if (record.type) meta.append(node("span",record.type,"tag"));
  if (record.doi) meta.append(safeLink(record.doi.startsWith("http")?record.doi:"https://doi.org/"+record.doi,record.doi.replace(/^https?:\/\/doi.org\//,"")));
  root.append(meta);
  const tabs = node("div",undefined,"tabs");tabs.setAttribute("role","tablist"); const pane=node("div");pane.id="record-pane";pane.setAttribute("role","tabpanel");
  const activate = (label,render) => { for(const b of tabs.children) {b.setAttribute("aria-selected",String(b.textContent===label));b.tabIndex=b.textContent===label?0:-1;} edgeRequest++; render(pane); };
  for (const [label,render] of [["概览",overview],["引用关系",relations],["全部字段",raw]]) {
    if (label === "引用关系" && recordEntity !== "works") continue;
    const b=button(label,()=>activate(label,render),""); b.setAttribute("role","tab");b.setAttribute("aria-controls","record-pane");b.setAttribute("aria-selected","false"); tabs.append(b);
  }
  root.append(tabs,pane);activate("概览",overview);
  tabs.addEventListener("keydown",event=>{if(!["ArrowLeft","ArrowRight","Home","End"].includes(event.key))return;event.preventDefault();const buttons=[...tabs.children],i=buttons.indexOf(document.activeElement),next=event.key==="Home"?0:event.key==="End"?buttons.length-1:(i+(event.key==="ArrowRight"?1:-1)+buttons.length)%buttons.length;buttons[next].click();buttons[next].focus();});
}
function overview(pane) {
  pane.replaceChildren();pane.append(node("p",record.id,"field-summary"));
  if(record.source === "arxiv") {pane.append(node("p",record.authors),node("p",`首次提交 ${record.first_submitted} · 最新提交 ${record.latest_submitted}`),node("p",(record.categories||[]).join(" · ")),node("p",record.abstract,"abstract"),node("p","arXiv 元数据未提供参考文献列表；已确认发表版的引用关系属于发表版。","subtle"));if(record.version_links)for(const v of record.version_links.confirmed)pane.append(button(`${v.kind === "publication" ? "查看发表版及其参考文献" : "查看 OpenAlex 预印本"}：${v.title||v.openalex_id}`,()=>loadDetail("works",v.openalex_id)));return;}
  if (record.description) pane.append(node("p",record.description,"abstract"));
  if (record.abstract_inverted_index) {
    const words=[]; for(const [word,offsets] of Object.entries(record.abstract_inverted_index)) for(const offset of offsets) words.push([offset,word]); words.sort((a,b)=>a[0]-b[0]);
    pane.append(node("h3","摘要","block-title"),node("p",words.map(x=>x[1]).join(" "),"abstract"));
  } else if (recordEntity === "works") pane.append(node("p","源记录未提供摘要。","subtle"));
  if (record.authorships?.length) {
    pane.append(node("h3",`作者与发表时机构 · ${fmt(record.authorships.length)}`,"block-title"));const list=node("ol",undefined,"author-list");
    for(const a of record.authorships) {const li=node("li");if(a.author?.id) li.append(entityButton("authors",a.author));else li.append(node("span",a.raw_author_name || "未提供姓名"));if(a.is_corresponding) li.append(node("span"," · 通讯作者","subtle"));const institutions=node("div",undefined,"institutions");for(const i of a.institutions || []) {if(institutions.childNodes.length) institutions.append(document.createTextNode(" · "));institutions.append(entityButton("institutions",i));}li.append(institutions);list.append(li);}pane.append(list);
  }
  const source=record.primary_location?.source;
  if(source?.id) {pane.append(node("h3","主要来源","block-title"),entityButton("sources",source));}
  const oa=record.open_access; if(oa) {pane.append(node("h3","开放获取","block-title"),node("p",oa.is_oa ? "开放获取 · "+(oa.oa_status||"") : "源记录未标为开放获取","subtle"));if(oa.oa_url) pane.append(safeLink(oa.oa_url,"打开源记录中的获取链接 ↗"));}
  for(const field of ["topics","keywords","concepts","funders","awards","last_known_institutions"]) {
    if(!record[field]?.length) continue;const entity=field==="last_known_institutions"?"institutions":field;pane.append(node("h3",field==="last_known_institutions"?"当前机构":labels[field],"block-title"));const chips=node("div",undefined,"detail-actions");for(const item of record[field]) if(item.id) chips.append(entityButton(entity,item));pane.append(chips);
  }
  if(["authors","institutions","sources","topics",...Object.keys(directoryWorkControls)].includes(recordEntity)) pane.append(button("查看该实体关联的已索引文献",()=>navigateWorks(recordEntity,record.id),"secondary"));
  pane.append(node("p",`共 ${Object.keys(record).length} 个顶层字段；切换“全部字段”查看未经裁剪的完整 JSON。`,"subtle"));
}
function raw(pane) {pane.replaceChildren(node("p",`${Object.keys(record).length} 个顶层字段 · 完整 API 响应，可下载保存。`,"field-summary"));const pre=node("pre",recordText,"raw");pre.tabIndex=0;pane.append(pre);}
const relationLabels = {references:"参考文献",citations:"引用本文的文献","related-works":"相关文献"};
function referenceCount() { return Array.isArray(record.referenced_works) ? record.referenced_works.length : null; }
function relationExplanation(direction) {
  if(direction === "citations") return "哪些文献引用了本文：从本地快照的参考关系反向查询，与本文是否有参考列表无关。源被引数不一定等于本地可查数量。";
  if(direction === "related-works") return "来自 OpenAlex 快照的 related_works 推荐列表，不代表引用关系，也不是本地语义搜索。";
  const count=referenceCount();
  return `本文引用了哪些文献：${count === null ? "源记录未提供结构化参考文献列表" : `源记录包含 ${fmt(count)} 条参考标识`}。目标详情从全库元数据读取，不受地学文本索引范围限制。`;
}
function relations(pane) {
  pane.replaceChildren();edgeCursor=null;edgeDirection="references";
  const actions=node("div",undefined,"detail-actions relation-switch");actions.setAttribute("role","group");actions.setAttribute("aria-label","引用关系方向");
  for(const [value,label] of Object.entries(relationLabels)) {
    const b=button(value === "references" && referenceCount() !== null ? `${label} · ${fmt(referenceCount())}` : label,()=>{edgeDirection=value;edgeCursor=null;loadEdges(false);},"secondary");b.dataset.direction=value;b.setAttribute("aria-pressed",String(value === edgeDirection));actions.append(b);
  }
  const note=node("p",undefined,"relation-note");note.id="relation-note";
  const list=node("div",undefined,"edge-list");list.id="edge-list";list.setAttribute("aria-live","polite");
  const more=button("加载下一页",()=>loadEdges(true),"secondary wide");more.id="more-edges";more.hidden=true;
  pane.append(actions,note,list,more);loadEdges(false);
}
function emptyRelations(direction) {
  const box=node("div",undefined,"relation-empty");
  if(direction === "references") {
    const count=referenceCount();
    if(count > 0) box.append(node("h3","参考关系与源记录不一致"),node("p",`源记录包含 ${fmt(count)} 条参考标识，但关系接口未返回结果。请保留本文 ID 并检查服务数据，不应视为零参考文献。`));
    else box.append(node("h3","快照未收录本文的参考列表"),node("p","这不表示论文原文没有参考文献。有其他论文引用本文时，“引用本文的文献”仍然可以有结果。"),node("p","预印本和正式发表版本是独立记录，参考列表可能不同；可用正式发表 DOI 或标题查找另一版本。系统不会自动合并版本或在线补全参考文献。"));
    if(record.type === "preprint" && !count) box.append(node("p","当前为预印本记录：OpenAlex 对预印本参考列表的覆盖可能不完整；其他论文仍可引用它。这不代表所有 arXiv 文献都缺少参考关系。","subtle"));
    if(record.referenced_works_count > 0 && !count) box.append(node("p",`源声明参考数为 ${fmt(record.referenced_works_count)}，但未提供可展开的参考标识。`,"subtle"));
  } else if(direction === "citations") box.append(node("h3","快照中未找到引用本文的文献"),node("p","此结果取决于其他文献的参考列表和快照时间，不代表真实被引数为零。"));
  else box.append(node("h3","源记录未提供相关文献"),node("p","快照没有提供推荐列表，本地不会推断或生成相关记录。"));
  return box;
}
async function loadEdges(more) {
  const ticket=++edgeRequest, direction=edgeDirection, id=record.id.split("/").pop(), params={select:"id,title,publication_year,doi,authorships,primary_location"};if(direction!=="related-works")params.per_page="20";if(more && edgeCursor) params.cursor=edgeCursor;
  const target=$("edge-list"), moreButton=$("more-edges");if(!target)return;moreButton.disabled=true;message();
  document.querySelectorAll("[data-direction]").forEach(b=>b.setAttribute("aria-pressed",String(b.dataset.direction===direction)));
  $("relation-note").textContent=relationExplanation(direction);
  if(!more){target.replaceChildren(node("p","正在读取"+relationLabels[direction]+"…","subtle"));moreButton.hidden=true;}
  target.setAttribute("aria-busy","true");
  try {
    const {data}=await request(`/v1/works/${id}/${direction}`,params);if(ticket!==edgeRequest)return;if(!more)target.replaceChildren();
    for(const edge of data.results){
      const item=node("article",undefined,"edge-item"), work=edge.work;
      item.append(edge.resolved?button(work.title||"源记录未提供标题",()=>loadDetail("works",edge.id),"title-button"):node("span","快照中未收录的文献"));
      if(edge.resolved){
        const names=(work.authorships||[]).slice(0,3).map(a=>a.author?.display_name).filter(Boolean);
        item.append(node("p",[work.publication_year,names.join(" · "),work.primary_location?.source?.display_name].filter(Boolean).join(" · ")));
        const doi=node("p");doi.append(work.doi?safeLink(work.doi.startsWith("http")?work.doi:"https://doi.org/"+work.doi,"DOI: "+work.doi.replace(/^https?:\/\/doi.org\//,"")):node("span","源记录未提供 DOI"));item.append(doi);
      }else item.append(node("p",`该关联仅有源标识符 ${edge.id}，当前快照无法提供 DOI 和标题。`));
      target.append(item);
    }
    if(!data.results.length && !more)target.append(emptyRelations(direction));edgeCursor=data.meta.next_cursor;moreButton.hidden=!edgeCursor;
  }catch(error){if(ticket===edgeRequest){if(!more)target.replaceChildren(node("p","关系查询失败："+error.message,"relation-empty"));message(error.message);}}
  finally{if(ticket===edgeRequest){moreButton.disabled=false;target.setAttribute("aria-busy","false");}}
}

async function loadTaxonomy() {
  const ticket=++taxonomyRevision;
  for(const [entity,id] of Object.entries(taxonomyControls)) {
    if(!hasDirectory(entity))continue;
    const filter=entity==="fields" && $("domain-filter").value ? "domain.id:"+$("domain-filter").value : entity==="subfields" && $("field-filter").value ? "field.id:"+$("field-filter").value : entity==="subfields" && $("domain-filter").value ? "domain.id:"+$("domain-filter").value : "";
    const records=[];let cursor="*";
    try {
      do {const {data}=await request(`/v1/${entity}`,{per_page:"100",cursor,filter,select:"id,display_name"});records.push(...data.results);cursor=data.meta.next_cursor;if(records.length>10000)throw new Error("学科目录超出预期规模。");}while(cursor);
      if(ticket!==taxonomyRevision)return;
      const select=$(id),current=select.value,first=select.options[0].cloneNode(true);
      select.replaceChildren(first,...records.map(r=>{const option=node("option",r.display_name);option.value=directoryValue(entity,r.id);return option;}));
      if([...select.options].some(option=>option.value===current))select.value=current;
    }catch(error){if(ticket===taxonomyRevision)message("学科目录加载失败："+error.message);return;}
  }
}
function navigateWorks(entity,id) {
  $("data-source").value="openalex";
  for(const name of stateFields) {if(["entity","data-source","search-field","search-scope"].includes(name))continue;const el=$(name);if(el.type==="checkbox")el.checked=false;else if(el.options)el.selectedIndex=0;else el.value="";}
  const control=directoryWorkControls[entity] || {authors:"author-id-filter",institutions:"institution-filter",sources:"source-filter",topics:"topic-filter"}[entity];
  const value=directoryWorkControls[entity] ? directoryValue(entity,id) : id.split("/").pop();
  if($(control).options && ![...$(control).options].some(option=>option.value===value)){const option=node("option",value);option.value=value;$(control).append(option);}
  $(control).value=value;chooseEntity("works");invalidateQuery();loadTaxonomy();search(null);
}
$("domain-filter").addEventListener("change",()=>{$("field-filter").value="";$("subfield-filter").value="";loadTaxonomy();});
$("field-filter").addEventListener("change",()=>{$("subfield-filter").value="";loadTaxonomy();});

for(const [value,label] of Object.entries(labels)){const option=node("option",label);option.value=value;$("entity").append(option);}
$("entity").addEventListener("change",()=>{chooseEntity($("entity").value);invalidateQuery();});
$("connect-form").addEventListener("submit",connect);$("search-form").addEventListener("submit",event=>search(event));$("more-results").addEventListener("click",()=>search(null,true));
$("refresh-status").addEventListener("click",()=>refreshStatus().then(()=>message()).catch(error=>message(error.message)));
$("disconnect").addEventListener("click",()=>{sessionStorage.removeItem("openalex-key");location.reload();});
restoreQuery();
if ([...$("advanced-filters").querySelectorAll("input,select")].some(el=>el.type === "checkbox" ? el.checked : el.value)) $("advanced-filters").open = true;
async function initializeConnection() {
  if(key) return connect().then(refreshExports);
  try {
    const response = await fetch("/status");
    if(response.ok) await connect().then(refreshExports);
  } catch { message("暂时无法连接文献库，请稍后重试。"); }
}
initializeConnection();
setInterval(()=>{if(!$("workspace").hidden && $("export-panel").open && !document.hidden)refreshExports();},5000);
$("export-panel").addEventListener("toggle",()=>{if($("export-panel").open)refreshExports();});

for (const [inputId, listId, entity] of [["topic-filter","topic-options","topics"],["source-filter","source-options","sources"],["author-id-filter","author-options","authors"],["institution-filter","institution-options","institutions"],["country-filter","country-options","countries"],["license-filter","license-options","licenses"],["language-filter","language-options","languages"]]) {
  let timer, revision = 0;
  $(inputId).addEventListener("input", () => {
    clearTimeout(timer); const ticket = ++revision, value = $(inputId).value.trim();
    if (value.length < 2 || (!hasDirectory(entity) && (!status?.search_ready || status?.search_coverage?.entities?.[entity]?.ready === false))) return;
    timer = setTimeout(async () => {
      try {
        const {data} = await request(`/v1/${entity}`, {search:value,search_field:"title",per_page:"10",select:"id,display_name"});
        if (ticket !== revision) return;
        $(listId).replaceChildren(...data.results.map(r=>{const option=node("option");option.value=directoryValue(entity,r.id);option.label=r.display_name;return option;}));
      } catch { if (ticket === revision) $(listId).replaceChildren(); }
    }, 300);
  });
}

function saveQuery(){const params=new URLSearchParams();for(const id of stateFields){const el=$(id),v=el.type==="checkbox"?(el.checked?"1":""):el.value;if(v)params.set(id,v);}history.replaceState(null,"","?"+params);}
function restoreQuery(){const params=new URLSearchParams(location.search);for(const id of stateFields){const value=params.get(id);if(value!==null){const el=$(id);if(el.options && ![...el.options].some(option=>option.value===value)){if(Object.values(taxonomyControls).includes(id)){const option=node("option",value);option.value=value;el.append(option);}else continue;}if(el.type==="checkbox")el.checked=value==="1";else el.value=value;}}}
$("search-form").addEventListener("input",invalidateQuery);
$("clear-filters").addEventListener("click",()=>{
  for(const id of stateFields) {
    if(["entity","query","data-source"].includes(id))continue;
    const el=$(id);if(el.type === "checkbox")el.checked=false;else if(el.options)el.selectedIndex=0;else el.value="";
  }
  chooseEntity($("entity").value);invalidateQuery();saveQuery();message();$("query").focus();
});
updateSelection();
$("select-page").addEventListener("click",()=>{for(const id of loadedWorks)selectedWorks.add(id);for(const box of $("results").querySelectorAll("input[type=checkbox]"))box.checked=true;updateSelection();});
async function createExport(selected){try{if($("entity").value!=="works")throw new Error("请先查询文献结果。");const payload={source:$("data-source").value,formats:[$("export-format").value]};if(selected){if(!selectedWorks.size)throw new Error("请勾选需要导出的文献。");payload.identifiers=[...selectedWorks];}else{if(!lastQuery)throw new Error("请先完成一次文献查询。");payload.query=lastQuery;}if($("export-select").value.trim())payload.select=$("export-select").value.trim();if($("export-columns").value.trim())payload.csv_fields=$("export-columns").value.split(",").map(x=>x.trim());await request("/v1/exports",null,payload);$("export-panel").open=true;await refreshExports();}catch(error){message(error.message);}}
$("export-query").addEventListener("click",()=>createExport(false));$("export-selected").addEventListener("click",()=>createExport(true));$("refresh-exports").addEventListener("click",refreshExports);
async function downloadExport(job,name){try{const res=await fetch(`/v1/exports/${job}/files/${encodeURIComponent(name)}`,{headers:key ? {"X-API-Key":key} : {}});if(!res.ok)throw new Error("文件下载失败，请刷新任务后重试。");const url=URL.createObjectURL(await res.blob()),a=node("a");a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}catch(error){message(error.message);}}
async function refreshExports(){if($("workspace").hidden)return;try{const {data}=await request("/v1/exports");const root=$("export-jobs");root.replaceChildren();const names={queued:"排队",running:"运行",paused:"暂停",cancelled:"已取消",failed:"失败",completed:"完成"};for(const job of data.results){const row=node("article",undefined,"edge-item");row.dataset.jobId=job.id;row.append(node("p",`任务 ${job.id.slice(-8)} · ${new Date(job.created*1000).toLocaleString("zh-CN")} · ${names[job.state]||job.state} · ${fmt(job.exported)} 篇 · 缺 DOI ${fmt(job.missing_doi)} · ${job.generation}`));if(job.error)row.append(node("p",job.error,"subtle"));if(!["completed","cancelled","failed","paused"].includes(job.state))row.append(button("取消",async()=>{try{await request(`/v1/exports/${job.id}/cancel`,null,{});refreshExports();}catch(error){message(error.message);}}));if(["cancelled","paused","failed"].includes(job.state))row.append(button("恢复",async()=>{try{await request(`/v1/exports/${job.id}/resume`,null,{});refreshExports();}catch(error){message(error.message);}}));for(const f of job.files)row.append(button(f.name,()=>downloadExport(job.id,f.name),"quiet"));if(job.state==="completed")row.append(button("下载 manifest",()=>downloadExport(job.id,"manifest.json"),"quiet"));root.append(row);}}catch(error){$("export-note").textContent=error.message;}}

function updateSource() {
  const arxiv=$("data-source").value === "arxiv";
  $("arxiv-category-label").hidden=!arxiv;$("arxiv-author-label").hidden=!arxiv;
  $("arxiv-category").disabled=!arxiv;$("arxiv-author").disabled=!arxiv;
  $("collections").closest("aside").hidden=arxiv;
  if(arxiv) {
    $("entity").value="works";$("entity").disabled=true;
    $("search-scope").disabled=true;
    $("advanced-filters").hidden=true;
    for(const id of ["year","sort","author-query"]) $(id).disabled=false;
    for(const option of $("search-field").options) option.disabled=!["all","title","abstract","author"].includes(option.value);
    if($("search-field").selectedOptions[0].disabled) $("search-field").value="all";
    $("sort").options[1].textContent="首次提交：从新到旧";
    $("sort").options[2].disabled=true;
    if($("sort").value === "cited_by_count:desc") $("sort").value="";
    $("query").placeholder="输入 arXiv 题名、作者或 arXiv ID";
    $("search-note").textContent="arXiv 独立检索；年份指 v1 首次提交年份。切换来源不会混合计数和排序。";
    $("coverage-note").textContent=status?.arxiv?.ready ? `arXiv ${fmt(status.arxiv.count)} 篇 · ${status.arxiv.min_year} 年及以后 · ${status.arxiv.release}` : "arXiv 来源尚未发布或不可用；OpenAlex 可继续使用。";
  } else {
    $("entity").disabled=false;$("advanced-filters").hidden=false;
    $("sort").options[1].textContent="年份：从新到旧";$("sort").options[2].disabled=false;
    $("query").placeholder="输入题名、DOI 或 OpenAlex ID";
  }
}
$("data-source").addEventListener("change",()=>{chooseEntity("works");invalidateQuery();selectedWorks.clear();loadedWorks.clear();$("results").replaceChildren();$("result-count").textContent="";updateSelection();saveQuery();});
async function searchArxiv(more,revision) {
  if(!status?.arxiv?.ready) throw new Error("arXiv 来源尚未发布或暂不可用。");
  const value=$("query").value.trim();
  if(!more){selectedWorks.clear();loadedWorks.clear();lastQuery=null;saveQuery();if(/^(arxiv:|https?:\/\/arxiv.org\/abs\/|\d{4}\.\d{4,5}(v\d+)?$|[a-z-]+\/\d{7})/i.test(value)){await loadArxiv(value,true);return;}}
  const params=more ? {...nextResults.params,cursor:nextResults.cursor} : {search:value,search_field:$("search-field").value,year:$("year").value.trim(),category:$("arxiv-category").value.trim(),author_search:$("arxiv-author").value.trim(),per_page:"20",sort:$("sort").value ? "submitted:desc":"relevance"};
  const route="/v1/arxiv/works",{data}=await request(route,params);
  if(revision!==searchRevision)return;
  lastQuery={route,params:Object.fromEntries(Object.entries(params).filter(([k])=>!["per_page","cursor"].includes(k)))};
  if(!more)$("results").replaceChildren();
  for(const r of data.results)$("results").append(card(r,"works"));
  if(!data.results.length&&!more)$("results").append(node("p","没有匹配的 arXiv 记录。","empty"));
  $("result-count").textContent=`${fmt(data.meta.count)} 条 · arXiv`;
  nextResults=data.meta.next_cursor?{route,params,cursor:data.meta.next_cursor}:null;
  $("more-results").hidden=!nextResults;
}
async function loadArxiv(value,replaceResults=false,preferPublication=true) {
  const ticket=++detailRequest;edgeRequest++;
  try {
    const response=await request("/v1/arxiv/lookup",{identifier:value});if(ticket!==detailRequest)return;
    const preprint=response.data,preferred=preprint.version_links?.preferred_openalex_id;
    if(preferPublication&&preferred) {
      await loadDetail("works",preferred,"openalex",false,false);
      if(record?.id?.endsWith(preferred)) $("detail").prepend(button("当前为已确认发表版 · 切换到 arXiv 预印本",()=>loadArxiv(value,false,false)));
    } else {record=preprint;recordText=response.text;recordEntity="arxiv";edgeCursor=null;renderDetail();}
    if(replaceResults){$("results").replaceChildren(card(preprint,"works"));$("result-count").textContent="1 条 · arXiv 精确定位";}
    updateSelection();
  }catch(error){if(ticket===detailRequest)message(error.message);}
}
