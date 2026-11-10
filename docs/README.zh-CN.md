# OpenAlex Local

使用者按[下载指引](SNAPSHOT.md)自行获取 OpenAlex snapshot，指定本地存放路径后，由本项目读取、校验、导入和处理，并部署网页及服务器 API。项目不负责快照下载。

默认流程与学科无关：使用 ClickHouse 保存完整原始元数据、DOI／ID 映射和引用关系。地学分类、文献列表导出和 Manticore 文本索引作为**可选示例**，需要显式指定 `--profile earth_sciences`。不做这一步也能按 DOI／ID 查询元信息、参考文献及被引关系；标题、摘要、作者姓名等文本检索需要另外构建并发布索引。

```bash
oa --workspace /path/to/workspace init --snapshot /path/to/openalex-snapshot
oa --workspace /path/to/workspace engines ensure
oa --workspace /path/to/workspace snapshot verify --deep
oa --workspace /path/to/workspace pipeline run --generation local_metadata
oa --workspace /path/to/workspace pipeline resume --generation local_metadata --until published
```

可选地学示例使用独立 generation：

```bash
oa --workspace /path/to/workspace engines ensure --search
oa --workspace /path/to/workspace pipeline run --generation earth_example --profile earth_sciences
oa --workspace /path/to/workspace pipeline resume --generation earth_example --profile earth_sciences --until published
```

已按默认流程导入的数据也可以复用；原生部署指南说明了直接从已有 generation 运行地学分类和索引的方式。

数据、索引、私有配置、引擎和运行产物保留本地，不提交 Git。源码采用 MIT；数据及第三方引擎遵守各自许可证。A/D 图谱处理不属于本项目主线。当前为 v0.2.0 开发版，不宣称与官方在线 API 全部功能或实时数据一致。

入口文档：[部署与命令](QUICKSTART.md)、[模块边界](ARCHITECTURE.md)、[API 范围](API-COVERAGE.md)、[验证状态](VALIDATION.md)。
