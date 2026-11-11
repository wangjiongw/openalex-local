# 检索与批量导出增强版

[English](SEARCH-EXPORT.md) · [文档导航](INDEX.zh-CN.md)

稳定版本为 v0.3.0（包与 API 为 0.3.0），沿用 v0.3.0-rc.1 已验收的检索与导出实现。新功能及已有数据上的目录构建、物理恢复、查询／导出混合验收已通过；验收范围见 [验证说明](VALIDATION.zh-CN.md)。使用者的目录仍须单独构建、验收和发布。数据、私有配置、任务与输出不加入 Git。

## 查询与网页

继续使用 `/works`、`/authors`、`/institutions`、`/sources`、`/topics` 及对应 `/v1` 路由。标题、摘要、关键词、作者姓名可以与年份、OA、作者／机构身份、来源、Topic、类型和语言组合；支持范围以 `/capabilities` 为准。网页新增作者身份、机构与语言筛选、查询 URL、文献勾选及导出任务面板。

`/v1` 正向作者／机构条件要求落在同一 authorship，官方风格路由保持作品层面的交集；不改变既有语义。姓名检索不推断身份，选择作者 ID 不自动合并同名作者。实体名称检索包含源记录的别名；作者目录限于已发布地学候选文献关联作者，机构目录覆盖已导入机构。标识符详情与文本覆盖不同。

## 导出接口

所有接口使用现有 API 认证。创建任务：

```http
POST /v1/exports
Content-Type: application/json
X-API-Key: <local-key>
```

```json
{
  "query": {
    "route": "/v1/works",
    "params": {
      "search": "climate",
      "search_field": "title",
      "filter": "year:2020..2026,is_oa:true"
    }
  },
  "formats": ["jsonl.gz", "csv", "doi.txt", "ids.txt"]
}
```

或用 `"identifiers": ["W1", "10.1234/example"]` 替代 `query`。这两种输入互斥。`query.route` 仅接受 `/v1/works` 或 `/works`，不访问外部 URL。查询不接收分页、cursor、分组、抽样或认证参数；输出为完整匹配集合，按稳定 ID 顺序排列，与网页显示排序独立。官方风格的纯元数据查询覆盖导入的元数据，文本查询及 `/v1` 查询覆盖已发布索引。

- 可选 `select`：逗号分隔的顶层原始字段，用于 JSONL；留空保存完整原记录。
- 可选 `csv_fields`：列名数组。默认 `id,doi,title,publication_year,authors,abstract,abstract_status,source,is_oa,oa_status,oa_url,cited_by_count`。
- CSV 多值字段使用 JSON 单元格；`authors`、`abstract`、`source` 和 OA 列为源字段的方便投影。缺失／无效摘要留空，`abstract_status` 区分 missing／invalid／present；完整 JSON 不改写源数据。
- 缺 DOI 不丢弃论文；单独生成 missing-doi 分块。清单输入中的无效、未找到或歧义记录在 manifest 中报告，不自动选取候选。
- 原始引用 ID 会随完整 JSON 导出；本轮不批量展开引用目标全文记录。

| 接口 | 功能 |
| --- | --- |
| `GET /v1/exports?offset=0&limit=25` | 任务列表 |
| `GET /v1/exports/{id}` | 进度、错误、已提交文件 |
| `POST /v1/exports/{id}/cancel` | 提交当前检查点后取消 |
| `POST /v1/exports/{id}/resume` | 校验已提交结果后恢复；运行中须先取消 |
| `GET /v1/exports/{id}/files/{name}` | 下载已提交分块或完成后的 manifest.json |

任务位于 workspace 的 `exports/query/`。

`exported` 是已提交分块中的记录数；大分块写入期间可能暂时不变，恢复只从已提交检查点继续。每个任务固定 generation、适用的 search build、查询、字段和格式。分块回执记录计数、读取位置、哈希和文件；恢复不依赖网页 cursor。

默认每块约 10 万条或 256 MiB 未压缩输出，以批次为边界；单条超大记录可能超过大小目标。状态为 queued/running/paused/cancelled/failed/completed。资源不足只暂停该任务，持久文件默认保留。不要手动修改回执或删除仍被任务引用的数据和索引。

## CLI 与运行

```bash
oa --workspace /path/to/workspace start export-worker
oa --workspace /path/to/workspace export create --request request.json
oa --workspace /path/to/workspace export list
oa --workspace /path/to/workspace export status --job <id>
oa --workspace /path/to/workspace export cancel --job <id>
oa --workspace /path/to/workspace export resume --job <id>
```

`start all` 包含 export-worker；单独重启 API 不终止导出。默认两个任务并行；目录提取默认最多 8 个进程、每批 10,000 篇，可配置 `catalog_workers` 和 `catalog_batch_rows`。导出可配置 `export_workers`、`export_batch_rows`、`export_shard_rows`。不增加查询 QPS 配额。worker 停止时任务排队；服务的元数据可用状态不因此失效。

只读 adopt 继续复用源引擎及冻结数据，但新导出写入自己的 workspace，不写入源 workspace 的导出目录。

## 可复用实体索引扩建

```bash
oa --workspace /path/to/workspace catalogs build --generation <generation> --build <new_build> --parent <published_earth_build> --background
oa --workspace /path/to/workspace catalogs status --generation <generation> --build <new_build>
oa --workspace /path/to/workspace catalogs validate --generation <generation> --build <new_build>
oa --workspace /path/to/workspace catalogs publish --generation <generation> --build <new_build>
```

先新增全量机构索引，再从冻结 works 分段提取作者 ID，去重后按互不重叠的 ID 区间并行构建作者名称索引，每段预读下一批、保留独立检查点；已完成前缀封存复用。父进程串行封存快照，避免并发封存争用。不会重跑地学分类或整体重建 works。

同时从这些 works 提取可恢复的作者／机构 ID → 文献 ID 辅助索引，存放在同一代 ClickHouse 的派生表中。查询和批量导出先使用必需的正向身份条件缩小候选集，再执行原来的署名条件，保持同一 authorship 语义。

大候选集通过同一 Manticore HTTP 服务的 SQL 入口强制 docid 查找，避免优化器退回整表扫描；HTTP API、总数和分页 token 对调用者保持一致。

单个身份超过 25 万候选，或只有否定／可选 OR 条件时，保留完整扫描，不截断结果；此类宽泛条件可能较慢。辅助表和 identity-progress.json 属于持久数据，不能单独清理；缺失时必须恢复或在新 build 重建。

新增快照保留原分段的绝对依赖，不能删除父索引；进度和估算见新 build 目录下 progress.json。失败后使用相同参数继续；已有回执可重放至丢失的未封存目录。

验证复用父版本全量恢复证据，只检查新增分段的物理与恢复结果；完整快照执行常用查询、实体查询及 30 分钟查询／导出混合验收，之后显式发布。小样本原生测试不替代真实规模验收。发布不要求固定观察 24 小时。

本轮不包含全量作者目录、PDF 下载、版本自动关联、语义检索、任意学科 profile、快照更新或独立 ClickHouse 灾备。
