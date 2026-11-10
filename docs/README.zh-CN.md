# OpenAlex Local

从 OpenAlex snapshot 下载、校验、导入、分类和构建索引，部署本地网页与查询 API。

当前是社区开发基线，正在将现有实现整理为可复现工具链。完整元数据与引用使用 ClickHouse，文本检索使用 Manticore，网页与 API 使用 FastAPI。地学为第一个索引范围；不宣称与官方在线 API 全部功能或实时数据一致。

数据、索引、私有配置、引擎和运行产物保存在独立本地工作空间，不提交 Git。源码采用 MIT；数据及第三方引擎遵守各自许可证。A/D 图谱处理不属于本项目主线。

入口文档：[README](../README.md)、[快照格式](SNAPSHOT.md)、[模块边界](ARCHITECTURE.md)、[交付路线](ROADMAP.md)。
