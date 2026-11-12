# 小型实体目录与文献导航

[English](DIRECTORIES.md) · [查询覆盖](API-COVERAGE.zh-CN.md)

M2 首批补齐 domain、field、subfield、国家、大洲、语言、许可、机构／来源／作品类型和 SDG 的元数据名称浏览。它们是已导入来源中的小型词表，不是新的作品文本索引。未导入的词表返回空结果，不从上游补造取值。

`domain → field → subfield → topic` 是 OpenAlex 学科层级。domain 指四个广义学科领域之一；地球与行星科学是物理科学领域下的 field。arXiv 分类是另一套词表。选中学科条目会筛选已发布的 OpenAlex 作品范围，不会让该学科全库作品自动具备文本检索能力。

`/fields?search=Earth` 和 `/v1/fields?search=Earth` 均按显示名称做**不区分大小写的字面子串**匹配，不使用论文布尔全文检索语法。可不填检索词浏览，通过 `filter=domain.id:domains/3` 或 `filter=field.id:fields/19` 约束来源层级，通过 `per_page=100&cursor=*` 使用带签名、固定元数据代次的分页。保留源记录与 `select` 行为。`/capabilities.small_directories` 声明可用词表及文献导航字段。

网页增加 domain／field／subfield 层级选择、国家／许可／语言名称候选及目录到文献导航。保存的查询 URL 保留所选学科 ID；国家过滤指作者署名机构所属国家。同一套文献过滤直接用于查询结果导出：

```bash
curl -G "$BASE_URL/v1/fields" --data-urlencode 'search=Earth' --data-urlencode 'filter=domain.id:domains/3'
curl -G "$BASE_URL/v1/works" --data-urlencode 'filter=domain:domains/3,field:fields/19' --data-urlencode 'count=exact'
curl "$BASE_URL/v1/exports" -H 'Content-Type: application/json' -d '{"query":{"route":"/v1/works","params":{"filter":"domain:domains/3,field:fields/19"}},"formats":["jsonl.gz","csv"]}'
```

启用认证的部署须添加对应认证头。仅元数据安装无需 Manticore 即可浏览目录；作品文本检索、导航结果和相应导出仍需要已发布的作品索引。普通目录浏览不会启动构建、修改来源或请求上游。

复用模块 `directories` 声明小词表和名称字面匹配条件；现有 `official.execute` 在计数与分页之前执行 SQL 过滤，`/v1` 复用这一路径，现有查询／导出投影负责文献过滤。不增加数据处理阶段，也不要求重建作品索引。来源记录中缺失的父级字段继续保持缺失。

出版商／资助方名称目录及项目到文献关联属于 M2 后续批次，不在小词表检索范围内。已有作者／机构／期刊／Topic 目录继续复用。在线同步、Crossref 和新学科作品索引仍是独立的后续里程碑。
