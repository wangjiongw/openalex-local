# OpenAlex 快照

[English](SNAPSHOT.md) · [文档导航](INDEX.zh-CN.md)

官方参考：https://help.openalex.org/access/snapshot/

公开 S3 bucket 为 `s3://openalex`，可以通过 AWS CLI v2 加 `--no-sign-request` 匿名读取。
下载前先检查 manifest；完整格式的数据压缩后可达数百 GB，还需为数据库和索引预留额外空间。

当前格式包含对应的实体记录：

| 格式 | 文件 | Manifest |
| --- | --- | --- |
| JSON Lines | `data/jsonl/<entity>/updated_date=.../part_*.gz` | 总清单及实体 `manifest.json` |
| Parquet | `data/parquet/<entity>/updated_date=.../part_*.parquet` | 总清单及实体 `manifest.json` |
| 旧版 JSON Lines | `data/<entity>/updated_date=.../part_*.gz` | 使用 `entries` 的实体 `manifest` |

新清单使用 `files`，总清单使用 `entities`；旧清单使用 `entries`。
`updated_date` 分区表示更新日期，不是发表年份。JSONL 每行是一条完整实体 JSON。
未知字段应保留，缺失摘要和引用数组不能凭空补造。

本工具链适配用户提供的本地新旧 JSONL 清单。
当前会识别 Parquet 并提示，但不提供 Parquet 导入；没有相应需要时不必下载两份完整格式。
工具链不下载快照，也不更新源目录。

## 使用者自行下载

单独安装 AWS CLI v2，按照[官方快照指引](https://help.openalex.org/access/snapshot/)获取数据。
传输前检查当前总清单、发布说明、下载大小和磁盘空间，每版数据使用独立目录。
下面的命令下载完整 JSONL 前缀，数据量很大，不是小型试用样例：

```bash
export SNAPSHOT=/absolute/path/to/openalex-snapshot
mkdir -p "$SNAPSHOT/data/jsonl"
aws s3 cp s3://openalex/RELEASE_NOTES.txt "$SNAPSHOT/RELEASE_NOTES.txt" --no-sign-request
aws s3 cp s3://openalex/LICENSE.txt "$SNAPSHOT/LICENSE.txt" --no-sign-request
aws s3 cp s3://openalex/data/jsonl/manifest.json "$SNAPSHOT/manifest.before.json" --no-sign-request
aws s3 sync s3://openalex/data/jsonl "$SNAPSHOT/data/jsonl" --no-sign-request
aws s3 cp s3://openalex/data/jsonl/manifest.json "$SNAPSHOT/manifest.after.json" --no-sign-request
cmp "$SNAPSHOT/manifest.before.json" "$SNAPSHOT/data/jsonl/manifest.json"
cmp "$SNAPSHOT/manifest.before.json" "$SNAPSHOT/manifest.after.json"
```

比较不一致说明获取期间上游版本发生变化。不要导入混合版本；应在单独核对的版本目录中完成一致下载。
再次运行 AWS sync 可以继续传输，但不能保证上游版本冻结。
目录含其他数据时避免使用 `--delete`。保留清单、许可、发布说明和分片相对目录。
可随前缀下载删除／合并 ID 等辅助日志，但本版不会将其作为增量更新应用。

## 指定本地输入路径

```bash
oa --workspace /path/to/workspace init --snapshot "$SNAPSHOT"
oa --workspace /path/to/workspace inventory
oa --workspace /path/to/workspace snapshot verify --deep
```

`--snapshot` 指向含 `data/jsonl/manifest.json` 或旧 `data/<entity>/manifest` 的目录。
也接受直接包含总 `manifest.json` 的当前 JSONL 前缀目录。
校验会检查清单中所有分片；`--deep` 进一步检查 gzip 完整性、JSON 和记录数，全量数据上可能耗时较长。
不能用未修改的完整清单声明只有部分分片的数据：缺文件会被拒绝。
有意使用子集时应提供反映实际文件的清单，且该数据始终属于部分数据集。

输入数据不加入 Git，可在 workspace 外只读挂载。
处理开始后更改输入清单会被拒绝；新的输入应建立新的 generation。
源数据许可与项目代码许可分别管理。
