# 可选的 arXiv 数据来源

[English](ARXIV.md) · [文档目录](INDEX.zh-CN.md)

arXiv 是独立的可选来源，默认仍为 OpenAlex。发布 arXiv 数据版本后，**全部入选记录**都可检索，未关联到 OpenAlex 的预印本也保留。计数、排序和游标各自独立。原 OpenAlex 元数据、引用关系、索引及原始 JSON 保持不变。

## 准备输入

从 [Cornell arXiv Kaggle 数据集](https://www.kaggle.com/datasets/Cornell-University/arxiv/data)自行下载并解压元数据，将本地 `arxiv-metadata-oai-snapshot.json` 路径传给工具。虽然扩展名为 `.json`，内容是**每行一个 JSON 对象**，不是数组。支持普通或 gzip 压缩的 JSONL；项目不代下载、不处理 Kaggle 凭据。这份元数据不包含论文的参考文献列表和全文。

社区标准流程直接读取该文件。已有二次处理数据库时，可安装 `pip install 'openalex-local-api[arxiv-duckdb]'`，使用 `--input-format duckdb` 读取含 `arxiv_metadata` 表的本地数据库。兼容适配器只读，字段要求见 `arxiv_source.normalize`，不依赖原处理环境或外部 Parquet。它不能恢复已丢弃的结构化作者、完整版本历史或原始未知字段，输入明确标为 `derived_record`，不会冒充 Kaggle 原始 JSON。JSONL 和默认 OpenAlex 流程均不需要 DuckDB。

## 处理并显式发布

先启动工作区的 ClickHouse、Manticore，并发布通过验收的 OpenAlex 元数据代次。输入、筛选条件、处理代码或 OpenAlex 代次改变时，使用新的来源版本名。

```bash
oa --workspace /path/to/workspace arxiv plan --release arxiv_2010_v1 \
  --generation your_generation --input /path/to/arxiv-metadata-oai-snapshot.json \
  --input-format jsonl --min-year 2010
oa --workspace /path/to/workspace arxiv run --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv status --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv validate --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv publish --release arxiv_2010_v1
```

`run` 到校验结束，不修改已发布来源。可用 `--until ingest|link|store|index|validate` 分阶段执行。长任务放在持久终端运行。`stop` 请求在下个批次边界停止，`resume` 清除停止请求并根据持久检查点和回执继续。

```bash
oa --workspace /path/to/workspace arxiv stop --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv resume --release arxiv_2010_v1
```

以 **v1 首次提交时间**筛选，默认包含 2010 年及以后全部 arXiv 学科；更新时间和构建时间不作为替代。相同重复项去重并计数；冲突 ID、无效必需日期或字段会使任务失败并阻止发布，应修正来源并创建新版本。可选 DOI 无效时保留原输入并标记 `doi_status=invalid_source_value`。

## 模块与持久依赖

| 阶段 | 模块 | 输入 → 输出及验收 |
| --- | --- | --- |
| 盘点 | `arxiv_pipeline.plan` | 本地文件 → 固定文件哈希、筛选规则、代码签名、OpenAlex 校验证明 |
| 规范化 | `arxiv_source` | JSONL 或只读 DuckDB → 统一记录、保留输入、明确字段可用性 |
| 入库准备 | `arxiv_pipeline.ingest` | 来源记录 → SQLite 暂存、唯一 ID、事务检查点及去重计数 |
| 关联 | `arxiv_pipeline.exact_links/link` | 复用批量 DOI 解析和详情 → 已确认、歧义、未匹配证据 |
| 存储 | `arxiv_pipeline.store` | 暂存结果 → 独立 ClickHouse 记录及版本关联，逐批核验 |
| 索引 | `arxiv_index`、现有 `manticore_build` 写入及封存 | 全部入选记录 → 独立文本索引、校验回执、持久快照及恢复证明 |
| 验证与发布 | `arxiv_pipeline` | 全量元数据和索引核对 → 显式切换独立发布指针 |
| 服务与导出 | `arxiv_api`、现有 API/导出 worker/网页 | 固定来源 → 独立查询、详情、版本关联及下载 |

持久文件在 `var/sources/arxiv/<release>/`，包括来源清单、SQLite 暂存与检查点、索引回执、快照、校验报告。ClickHouse 使用独立的 `oa_arxiv_<release>` 表。`var/arxiv-active.json` 选择来源版本并保留前一个指针。保留已发布版本及导出任务依赖的文件。输入只读且保持本地，所有生成数据均不进入 Git。

Manticore 启动时独立恢复可选来源；arXiv 恢复失败会标记该来源不可用，现有 OpenAlex 检索仍可启动。切换 OpenAlex 元数据代次后旧映射失效，需要针对新代次创建和验收来源版本。

## 查询、版本及导出

- `GET /v1/arxiv/works`：支持 `search`、`search_field=all|title|abstract|author`、`author_search`、`year=YYYY|YYYY..YYYY`、`category`、`sort=relevance|submitted:desc|submitted:asc|id:asc`、`per_page` 和签名 `cursor`。
- `GET /v1/arxiv/lookup?identifier=arxiv:2304.02948`：返回规范化字段、保留来源输入、出处和精确关联。支持版本后缀及旧式斜杠 ID；公开 ID 固定为 `arxiv:<base_id>`。
- `GET /v1/works/W123/versions`：返回已确认关联的来源记录及证据。
- `/status`、`/capabilities`：增加可选 `arxiv` 的就绪状态、范围、版本。
- `POST /v1/exports`：复用现有 worker 和格式，指定 `source: "arxiv"`，传 `identifiers` 或 `query: {"route":"/v1/arxiv/works","params":{"search":"climate"}}`。导出固定来源、映射和代次，按稳定索引 ID 排序，报告未找到标识符和缺失 DOI。

网页可切换来源。arXiv 下隐藏或禁用 OpenAlex 专用筛选；年份指首次提交，分类使用 arXiv 代码，不是 OpenAlex topic/domain。搜索卡片保留来源身份；已确认关联的详情优先展示发表版，并提供切回预印本的按钮。

只有唯一的精确 DOI 证据可以建立关联，歧义保留，不按标题相似度自动合并。arXiv 版本数组描述版本编号和日期，不提供每个历史版本的标题、摘要或全文。发表版的参考文献属于该发表版，不写入预印本原始元数据。DOI 导出仅使用来源明确给出的发表 DOI，不合成 DOI 填补空值。arXiv 链接或许可本身不用于推断 OpenAlex OA 状态。

在线同步、Crossref、Semantic Scholar、全文解析/下载、混合来源排序均不在此流程内。代码实现、合成验收、全量发布及软件部署是不同里程碑，实际可用范围以安装后的 `/status` 为准。


精确关联先按全局 DOI 顺序批量解析，再按已确认的 OpenAlex ID 顺序读取详情，最后从持久证据应用同一套精确匹配规则。各子步骤分别保存检查点，减少反复随机读取元数据。工作区可配置 `arxiv_link_workers`（默认 8）和 `arxiv_link_batch_rows`（默认 2000）；这是离线任务设置，与 API 请求准入无关。SQLite 暂存使用有界的 128 MiB 页缓存，保留 FULL 同步提交。关联证据与暂存随来源版本保留，不因索引发布而丢弃。
