# OpenAlex Local 项目功能清单

[English](FEATURES.md) · [文档导航](INDEX.zh-CN.md)

本项目将使用者自行下载的公开 OpenAlex snapshot 转换为本地可用的文献元数据、引用关系和检索服务，提供 CLI、HTTP API 和网页。完整保留源数据，按需要构建文本检索索引；地学是内置的可选范围示例，不是整个项目的数据边界。

本清单结合内部版本的规模化运行验证、社区版本实现，以及官方仓库的职责划分。`0.3.2` 的检索、实体目录与导出增补见 [SEARCH-EXPORT.md](SEARCH-EXPORT.zh-CN.md)；具体目录是否上线以当前服务 capabilities 为准。稳定版范围及命令边界见 [发布与运维](RELEASE.zh-CN.md)。用于区分可复用能力和扩展候选，不表示下列待扩展功能已经实现。

## 1. 状态和边界

- **已实现**：已有对应实现，不表示所有输入和组合均完成规模测试。
- **已验证**：已有内部运行、交付或社区测试证据；仅对注明的版本和范围成立。具体边界见每行说明。
- **部分支持**：存在实现，但覆盖、查询组合、性能或质量仍有限制。
- **待扩展**：不属于当前交付功能；下面给出参考来源和预期产物。
- 历史内部基线为 `0.1.0`；社区工具链及已升级内部服务使用 `0.3.2`。部署使用同一安装包，保留本地数据、配置与回滚入口；社区发布不自动替换其他使用者的服务。
- 内部地学候选索引已完成 76,850,149 篇 works、4,516 个 topics、280,687 个 sources 的发布与验收。索引快照约 501 GiB，空搜索引擎恢复及跨 Pod 各通过 784 项功能检查。
- 社区默认元数据流程及可选地学流程已通过 Linux x86_64／ARM64 原生 CI。社区小样本验证不替代全量生产性能证明。
- **已有索引构建和恢复流程是可复用资产，不重新列为待开发。** 后续工作应以补接口、支持更多范围或输入类型、维护兼容性为主。

## 2. 从公开 snapshot 到本地服务

```text
官方公开 snapshot
    │ 使用者按官方指引下载；本项目不实现下载器
    ▼
指定本地路径 → 清点／校验 → 导入完整元数据与引用关系
                              │
                              ├─ 默认：DOI／ID、完整详情、引用关系 → API／网页
                              │
                              └─ 可选：选择文本检索范围 → 构建／验收／发布索引
                                                        │
                                                        └─ 标题、摘要、作者姓名等文本检索
```

分类和文献列表导出是范围处理的可选模块。文本索引对元数据服务可选，但对标题／摘要等高效文本检索必需。当前自动化文本流程内置 `earth_sciences` 示例；通用全学科或任意学科 profile 尚未交付。

### 2.1 数据获取指引与输入

| 编号 | 功能 | 状态与边界 |
| --- | --- | --- |
| A01 | 官方下载入口、格式、目录、许可和 manifest 指引 | 已提供；下载由使用者完成，参考 [SNAPSHOT.md](SNAPSHOT.zh-CN.md) |
| A02 | 指定数据位置 | 已实现：`oa init --snapshot /path/to/snapshot`；原始数据可在 workspace 外、可只读挂载 |
| A03 | 新旧 JSONL 输入适配 | 已实现：旧 `data/<entity>/manifest`、新 `data/jsonl/manifest.json`，以及直接 JSONL prefix 根目录 |
| A04 | 数据清点和完整性校验 | 已实现：实体、分片、记录数、字节数、缺失文件；深度校验 gzip、JSON 和实际记录数 |
| A05 | 数据版本绑定 | 已实现：manifest 指纹绑定 generation；输入变化拒绝静默恢复；部分数据不标记为全量 |
| A06 | Parquet 输入 | 待扩展；目前识别并提示不支持，不能将“官方有 Parquet”理解为本项目已能导入 |
| A07 | 快照追更 | 待扩展；当前按冻结输入构建，不自动从官方获取或应用新版本 |

### 2.2 元数据、标识符与引用关系

| 编号 | 功能 | 状态与边界 |
| --- | --- | --- |
| B01 | 全实体原始记录保存 | 已实现：读取受支持 manifest 所列实体，保留完整 JSON 和未知字段；缺失字段不编造 |
| B02 | 常用文献信息 | 已实现：DOI、title、作者／机构、年份、来源、Topic、关键词、OA、位置链接等；以源记录实际内容为准 |
| B03 | 摘要读取 | 已实现：保留 `abstract_inverted_index`，网页和文献列表可还原文本；源摘要缺失不自动补齐 |
| B04 | 标识符映射 | 已实现：DOI／OpenAlex ID；相关实体路由支持 PMID、PMCID、ORCID、ROR、ISSN 等已建映射 |
| B05 | 批量元数据查询 | 已实现：`POST /v1/works/batch`，返回 found／not_found／ambiguous；当前单次最多 100 个输入 |
| B06 | 正向／反向引用表 | 已实现并验证：从源引用关系构建 references 与 citations；可展开成 DOI、标题等完整记录 |
| B07 | 引用缺失和计数语义 | 已实现基本区分：缺失目标保留标识；本地反向边数与源 `cited_by_count` 不保证相等；空数组不等于真实论文没有参考文献 |
| B08 | 原始相关文献列表 | 已实现：展开 `related_works`；不是另行计算的语义推荐 |
| B09 | 导入检查点和数据审计 | 已实现并验证：断点恢复、源记录一致性、标识符和双向引用审计；更换输入需新 generation |
| B10 | 删除和合并 ID | 待扩展：应用 `deleted_ids`、`merged_ids` 并重定向标识符和引用边；与论文版本归组分开处理 |

### 2.3 可选集合和范围处理

| 编号 | 功能 | 状态与边界 |
| --- | --- | --- |
| C01 | 广义地学分类示例 | 已实现并验证：组合 Topic／学科层级、Concept、期刊、文本证据和受约束作者扩展 |
| C02 | 分批文献集合 | 已实现：核心期刊、泛地学／交叉学科、作者证据扩展三组；不是把全部候选都认定为严格地学论文 |
| C03 | 文献列表导出 | 已实现并验证：JSONL.gz／CSV，含 DOI、title、authors、abstract、OA、分类理由及缺失标记 |
| C04 | 复用已有元数据 | 已实现：无需为了分类或新索引重新导入 snapshot；可在已有 generation 上单独运行集合和索引模块 |
| C05 | 任意范围 profile | 待扩展：将全学科、任意 Topic／期刊集合或用户 work ID 清单做成明确入口；复用现有投影和索引构建器，不更换引擎 |
| C06 | 重新运行官方分类模型 | 非主流程必需；snapshot 已含上游标签，只有给新增记录打标签或重分类时才需要 |

### 2.4 文本索引和发布流程

| 编号 | 功能 | 状态与边界 |
| --- | --- | --- |
| D01 | 检索字段投影 | 已实现：从源元数据生成文本、标识符及常用过滤字段；索引是加速层，不替代原始 JSON |
| D02 | Manticore 索引构建 | 已实现并完成内部规模验证；当前主检索路线不依赖旧 Lucene／OpenSearch 索引 |
| D03 | 分块、批次和恢复 | 已实现：批次回执、校验、进度、断点续跑，已有分段及复用实现；继续构建时按冻结数据和布局处理 |
| D04 | 构建资源设置 | 已实现：配置构建并发、CPU／内存预算和空间保留；性能取决于数据规模及实际存储 |
| D05 | 持久化索引快照 | 已实现并验证：分片文件、manifest、批次回执、哈希及父分段依赖；跨部署复用不需要重建同一份兼容索引 |
| D06 | 验收与显式发布 | 已实现并验证：功能、并发、恢复检查后再发布；不要求固定观察 24 小时 |
| D07 | 版本切换与历史依赖 | 已有发布指针、版本绑定和旧版本保留机制；回退目标需通过相应版本校验，不能任意删除父分段 |
| D08 | 空搜索引擎恢复 | 内部约 501 GiB 已恢复并通过查询验收；社区同类流程通过原生小样本测试；不等于 ClickHouse 灾难备份已完成 |
| D09 | 全学科文本索引 | 待扩展范围入口和相应构建；不能将地学候选索引的完成视为全部学科都已建立文本索引 |
| D10 | 向量／语义索引 | 待扩展；现有标题／摘要搜索是词法检索，不产生 embedding |

### 2.5 HTTP API 与查询工具

| 编号 | 功能 | 状态与边界 |
| --- | --- | --- |
| E01 | DOI／ID 及实体详情 | 已实现；覆盖导入数据，不依赖该文献是否进入地学文本索引 |
| E02 | 标题、摘要、关键词文本检索 | 已实现；受已发布索引范围限制 |
| E03 | 作者姓名检索论文 | 已实现；搜索论文中的作者字段，不等同于独立作者实体检索 |
| E04 | 年份、OA、类型、作者／机构／来源／Topic 等筛选 | 已有支持；以 `/capabilities` 和已支持字段注册表为准；不是每个原始 JSON 字段都能高效过滤 |
| E05 | 来源、Topic、作者与机构名称检索 | 已有 sources／topics；0.3 增加全量机构及地学文献关联作者的目录扩建、恢复、验收与发布；实际开放范围以发布 build 为准 |
| E06 | 排序、分页、cursor、字段选择 | 已有实现；官方 REST 风格路由与本地 `/v1` 扩展并存，不能宣称完全兼容官方 |
| E07 | 元数据统计与抽样 | 部分支持：已有 `group_by`、`sample`／`seed` 等实现；文本检索与统计／抽样的任意组合未完成 |
| E08 | 引用图查询 | 已实现：references、citations、related-works 及相关计数；返回可读文献信息和缺失目标状态 |
| E09 | 参考文献匹配 | 部分支持：DOI 精确匹配；标题、作者、年份、来源候选排序；文本分数未校准，不能据此自动确认或合并论文 |
| E10 | 并发调用与跨 Pod | 已支持且内部已验证；通过可达 IP＋端口调用；默认不启用调用额度或入口并发拒绝，处理资源仍有实际容量 |
| E11 | 官方完整查询语义 | 待逐项扩展：完整字段／组合、全部实体名检索、OQL、语义检索、官方同等排序等不能默认视为已交付 |
| E12 | Python SDK／MCP | 待扩展：当前可直接使用 HTTP；独立 SDK 和本地 MCP 接口为扩展候选 |

### 2.6 网页前端

| 编号 | 功能 | 状态与边界 |
| --- | --- | --- |
| F01 | 连接本地 API | 已实现：API key、服务状态、元数据和文本索引覆盖提示 |
| F02 | 文献查询与过滤 | 已实现：DOI／ID、已索引范围的标题／作者姓名等，年份、OA、来源、Topic、类型等控件 |
| F03 | 结果和详情 | 已实现：可读标题、DOI、作者／机构、摘要、来源、OA、分类标签和原始 JSON |
| F04 | 引用浏览 | 已实现：参考文献／引用本文的文献／相关文献分开显示，分页和记录跳转 |
| F05 | 保存元数据与打开 OA 链接 | 已实现：下载单条记录 JSON、打开源记录提供的 OA 链接；**不是批量全文下载器** |
| F06 | 结果集导出、收藏和下载队列 | 0.3 已实现查询／清单导出、批量选择、持久任务与文件下载；收藏、PDF 下载队列仍待扩展 |
| F07 | 版本归组与发表版优先 | 待扩展；当前保留源记录，不保证预印本与正式版已跨记录合并 |
| F08 | 页面状态说明 | 已实现基础提示；继续使控件与 capabilities 对齐，区分未建索引、范围未覆盖、源数据缺失和服务故障 |

### 2.7 CLI、部署与运维

| 编号 | 功能 | 状态与边界 |
| --- | --- | --- |
| G01 | 代码与 workspace 分离 | 已实现：数据、索引、私有配置、日志、报告不入 Git；输入 snapshot 可独立存放 |
| G02 | 原生引擎安装 | 已实现：版本／哈希固定、离线缓存；默认 ClickHouse，`--search` 追加 Manticore；支持两种 Linux 架构及显式 ARM 兼容 profile |
| G03 | 计划、运行、恢复、状态 | 已实现：`pipeline plan/run/resume/status`；默认 metadata，地学示例显式选择 profile |
| G04 | 服务生命周期与诊断 | 已实现：启动／停止／重启、日志、status、healthz、readyz、capabilities；不允许自动接管其他主机仍占用的 workspace |
| G05 | 既有数据接入 | 已有受控只读 adopt；绑定成功与完整验收分开记录，不自动重写旧数据或替换生产后台 |
| G06 | 交付和恢复验证 | 已有内部完整搜索恢复、跨 Pod 及性能证据，社区测试验证可移植主流程；不要求从头重建已验证索引 |
| G07 | 完整独立灾难备份 | 待补强：ClickHouse 数据、私有配置、原始 snapshot、完整索引依赖的独立备份与恢复演练；已有搜索恢复不是全部数据库的灾备 |
| G08 | 社区稳定版收敛 | 稳定版固定现有功能、公共入口和验证边界；根据使用反馈维护稳定版，不重做既有导入／索引流程 |

## 3. 官方仓库可参考的扩展

以下均为扩展候选，除上表注明已实现的部分外，不表示本项目已经集成。引入时优先复用现有数据、批量查询、回执、任务和发布能力，避免复制官方整套生产基础设施。

| 扩展 | 用户获得的能力 | 官方参考 | 接入方式及主要边界 |
| --- | --- | --- | --- |
| X01 通用结果集导出（0.3 已实现） | 将搜索／过滤结果保存为 work ID、DOI、JSONL、CSV，供下载或外部分析使用 | [openalex-api-tutorials](https://github.com/ourresearch/openalex-api-tutorials)、[openalex-official](https://github.com/ourresearch/openalex-official) | 已复用既有查询、批量读取及分块回执，提供 API／CLI／网页入口；固定 generation、查询与索引版本，按稳定 ID 输出 |
| X02 实际文献下载 | 根据查询结果或 DOI／ID 清单获取 PDF，可选 TEI XML | [openalex-official](https://github.com/ourresearch/openalex-official)、[openalex-content-api](https://github.com/ourresearch/openalex-content-api) | 先支持清单导出及官方 CLI 对接，再考虑原生下载；下载权限和可用格式以官方服务及实际文件为准 |
| X03 本地 OA 下载 | 使用 snapshot 的 `pdf_url`／OA locations 获取可访问 PDF | [openalex-official](https://github.com/ourresearch/openalex-official)、[openalex-http](https://github.com/ourresearch/openalex-http) | 与官方内容接口分为不同来源；记录链接失效、返回 HTML、无全文等情况，不承诺每篇都有 PDF |
| X04 内容库与下载任务 | 本地文件、元数据与 work ID 对齐，支持续传／重试、去重和失败列表 | [openalex-official](https://github.com/ourresearch/openalex-official) | 索引库与全文文件分开存储；保存文件类型、哈希、来源、获取时间和下载状态；凭据不入 Git |
| X05 PDF／XML 结构化解析 | 抽取摘要、作者／机构和参考文献，供后续增补 | [openalex-grobid](https://github.com/ourresearch/openalex-grobid)、[parseland-lib](https://github.com/ourresearch/parseland-lib)、[openalex-pdf-parser](https://github.com/ourresearch/openalex-pdf-parser) | 提取结果单独存储并保留来源／置信信息；不直接覆盖原始 snapshot；解析与匹配是不同步骤 |
| X06 跨记录版本关联 | 连接预印本、正式版和仓储副本，优先展示发表版 | [openalex-work-matcher](https://github.com/ourresearch/openalex-work-matcher) | 优先用明确 DOI／arXiv ID／版本关系；文本候选再评分，歧义保留。官方工具部分步骤需要在线 API 或模型资源，不能原样当成离线本地插件 |
| X07 更好的文献匹配 | 对没有 DOI 的书目或图谱文献提高候选召回和匹配质量 | [openalex-work-matcher](https://github.com/ourresearch/openalex-work-matcher) | 复用现有文本索引及匹配 API，增加评测集、阈值和歧义处理；现有未校准分数不直接升级成自动确认 |
| X08 快照更新、删除、合并 | 保持本地数据新鲜，处理失效 ID 和引用重定向 | [openalex-help](https://github.com/ourresearch/openalex-help)、[openalex-walden snapshot](https://github.com/ourresearch/openalex-walden/tree/main/notebooks/snapshot) | 先做新 generation 的可重复更新／切换，再评估细粒度更新；更新要覆盖反向引用和索引，不能只追加 JSON |
| X09 官方查询能力对齐 | 更完整的过滤、统计、实体检索、返回语义和新查询语言 | [openalex-elastic-api](https://github.com/ourresearch/openalex-elastic-api)、[openalex-help](https://github.com/ourresearch/openalex-help) | 借鉴查询契约和测试案例，保留现有 ClickHouse＋Manticore；官方 ES 后端代码不直接等于可运行的本地工具链 |
| X10 新文本标签 | 为新增文献生成 Topic、关键词、SDG 等 | [openalex-topic-classification](https://github.com/ourresearch/openalex-topic-classification)、[openalex-keywords](https://github.com/ourresearch/openalex-keywords)、[openalex-sdgs](https://github.com/ourresearch/openalex-sdgs) | 仅在需要时运行；读取 snapshot 现成标签无需安装这些模型；新标签须记录模型版本 |
| X11 AI 工具接入 | 用 MCP 客户端调用本地搜索、详情和引用查询 | [openalex-mcp-server](https://github.com/ourresearch/openalex-mcp-server) | 适配本地 API，明确本地覆盖范围；官方在线 MCP 的语义／配额不能直接套到本地服务 |

### 3.1 文献下载的最小完整流程

```text
本地搜索／筛选或用户 DOI 清单
    → 冻结待下载清单（work ID、DOI、版本与来源信息）
    → 选择来源：官方内容服务 或 本地记录中的可访问 OA 链接
    → 获取 PDF／TEI（按来源支持情况）
    → 验证类型和文件完整性，保存文件、哈希与状态
    → 返回清单、文件位置和失败原因
    → 可选：PDF／TEI 解析 → 参考文献匹配／版本关联
```

需要特别区分三种“下载”：

1. **snapshot 下载**：使用者按指引获取原始数据库，本项目不实现下载器。
2. **元数据导出**：从本地服务保存 JSONL.gz／CSV／DOI／ID，支持查询结果与标识符清单、分块回执和断点恢复。
3. **实际文献文件下载**：PDF／TEI 内容获取，是可以新增的独立功能，不属于当前索引构建。

官方 CLI 输出常为每篇一个 JSON／PDF／XML 文件，与 snapshot manifest＋JSONL 分片不是相同输入格式。若以后导入这些增补元数据，需要单独适配、去重并保留来源，不能直接将目录传给现有 snapshot reader。

### 3.2 官方参考如何用于本项目

| 参考 | 职责 | 本地设计用途 |
| --- | --- | --- |
| [openalex-help](https://github.com/ourresearch/openalex-help) | 官方数据、访问与查询文档 | 输入格式、术语及兼容目标 |
| [openalex-api-tutorials](https://github.com/ourresearch/openalex-api-tutorials) | API 分析示例 | 使用流程和查询／导出示例 |
| [openalex-elastic-api](https://github.com/ourresearch/openalex-elastic-api) | Elasticsearch 支撑的 API 入口 | 查询契约和测试参考，不意味着迁移引擎 |
| [openalex-walden](https://github.com/ourresearch/openalex-walden) | 上游数据流水线 | 输入／更新语义，不要求部署其处理基础设施 |
| 上表内容、匹配、分类和 MCP 项目 | 可选下游能力 | 独立接入并明确验收，分别检查依赖与许可 |

## 4. 建议的落地顺序

| 阶段 | 应做的工作 | 不需要重复做的工作 |
| --- | --- | --- |
| 当前核心清单收敛 | 以本清单标明边界、验证证据和命令入口，使前端能力提示与 API 一致；整理稳定版交付 | 不重新选技术栈，不把已验证的导入、索引、发布、恢复流程当成待实现 |
| 第一批实用扩展 | 通用结果集与 DOI／ID 导出已交付；后续考虑官方全文下载对接、下载状态与本地文件关联 | 无需为下载 PDF 重建现有文本索引 |
| 第二批查询扩展 | 其他实体名称索引、用户清单／其他范围 profile、官方常用查询组合；按需要做版本关联和匹配评测 | 不必重跑官方分类模型，不必先建全学科向量库 |
| 长期维护 | 新 snapshot 版本处理、删除／合并 ID、独立灾备、性能回归；按需求增加语义检索、解析和 MCP | 不必部署官方 Databricks／Airflow 全套上游采集链路 |

## 5. 相关入口

- [原生部署和命令](QUICKSTART.zh-CN.md)
- [快照下载与格式指引](SNAPSHOT.zh-CN.md)
- [查询覆盖与限制](API-COVERAGE.zh-CN.md)
- [验证范围](VALIDATION.zh-CN.md)
- [官方快照文档](https://help.openalex.org/access/snapshot/)
- [官方全文 CLI](https://github.com/ourresearch/openalex-official)

本清单的说明及代码参考基于 2026-11-10 的读取；3.2 节仓库职责于 2026-11-11 按公开仓库元数据再次核对。上游服务、访问方式和模型会变化，真正接入前应重新核对行为与许可。本次功能梳理不改变生产数据，不将独立 A/D 图谱任务并入主项目。
