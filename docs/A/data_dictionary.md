# 数据字典（A 负责；实现后同步更新）

## 原始数据（data.gov.sg collection 2279，2017–2024 八个年度 CSV）

12 列，Spark 以显式 schema 全部按字符串读取（`src/common.py:raw_schema`），坏行进 `_corrupt_record`：

`date, timestamp, update_timestamp, station_id, station_name, station_device_id, location_longitude, location_latitude, reading_update_timestamp, reading_value, reading_type, reading_unit`

数据集 ID 见 `configs/project.yaml`；下载记录见 `data/manifest.csv`。集合中还有一个 2016 年数据集（仅 2016-12-02 至 12-03），不使用。

## `data/clean/clean_rainfall`（Parquet，按 `year`/`month` 分区）

主键 `(station_id, timestamp_sgt)`，由 `clean_data.py` 校验唯一。

| 列 | 类型 | 说明 |
|---|---|---|
| station_id | string | 站点 ID |
| raw_timestamp | string | 原始 `timestamp` 字符串，未修改 |
| timestamp_sgt | timestamp | 对齐后的五分钟时隙（SGT），见 `time_semantics.md` |
| rainfall_mm | decimal(10,3) | 五分钟雨量；0 是真实无雨观测；缺测时隙**没有行**，不补 0 |
| station_name, station_device_id | string | 该条记录上的站点名与设备 ID |
| latitude, longitude | double | 该条记录上的经纬度（站点迁移见 `stations`/`station_phases`） |
| update_timestamp, reading_update_timestamp | timestamp | 解析后的两个更新时间 |
| available_at | timestamp | `greatest(timestamp_sgt, update_timestamp, reading_update_timestamp)`，**保守的可用时间代理**；档案只有最终版本，不能视为真实首次发布时间 |
| available_at_basis | string | `update_timestamps` 或 `slot_only`（两个更新时间都缺失） |
| align_shift_s | int | 对齐修正秒数（0 表示原本就在整五分钟） |
| value_rounded | boolean | 原始值超过 3 位小数、已四舍五入到 3 位（`excess_precision: round` 时） |
| n_raw_versions | long | 同键原始版本数（去掉完全重复行之后） |
| quality_flag | string | `ok` / `merged_same_value` / `conflict_resolved_latest` |
| source_file, source_year | string, int | 来源文件 |
| year, month | int | 分区列，取自 `timestamp_sgt` |
| event_time | timestamp | C 的 Task 2 契约名，等于 `timestamp_sgt`（五分钟区间末端） |
| quality_ok | boolean | C 的 Task 2 契约名；`quality_flag` 属于 ok / merged_same_value / conflict_resolved_latest 时为 true，主表中恒为 true（无效行已隔离） |

C 的 `build_future_30m_base` 直接使用 `station_id`、`event_time`、`rainfall_mm`、`quality_ok`；`rainfall_mm` 保持 decimal(10,3)。C 已修复标签精度：`labels.py` 按规范数据的 0.001 mm 精度转 decimal(20,3) 后求和，不再转 double；只有机器学习特征使用 double。

## 其他输出

| 路径 | 内容 |
|---|---|
| `data/interim/parsed` | 全部原始行 + 解析列 + `reject_reason`（可重建，不提交） |
| `data/interim/fated` | 全部原始行 + 唯一 `fate`，清洗流水对账的依据 |
| `data/clean/quarantine` | 被拒行（原始字段 + `fate` 原因） |
| `data/clean/conflicts` | 所有"同键不同值"组的全部版本 |
| `data/clean/stations`, `station_phases` | 站点汇总；按（名称、设备、经纬度）划分的阶段及首末观测时间 |
| `data/clean/quality/station_{day,month,year}_coverage` | 覆盖率表（见下） |
| `reports/tables/*.csv` | 小型汇总表，可提交 Git |

## 覆盖率口径（`data_quality.py --stage clean`）

- 每天预期 288 个时隙；月/年按实际日历天数（含闰年）。
- `coverage_calendar = n_valid / (日历天数 × 288)`：完整日历区间口径，半年站点不会被当成全年站点。
- `coverage_active = n_valid / (活跃天数 × 288)`：活跃天 = 站点首次到末次观测之间的日期。**首末观测只是投运/撤站的代理**，不是官方运行记录。
- `active_days_no_data`：活跃区间内整天无数据的天数。
- 只有有效观测计入 `n_valid`；被隔离的行不计入。

## 清洗流水中的 fate

| fate | 去向 | 含义 |
|---|---|---|
| kept | clean_rainfall | 保留 |
| exact_duplicate | 删除 | 12 个原始字段完全相同的重复行 |
| same_value_merged | 删除 | 同站同时隙同数值的旧版本 |
| conflict_superseded | 删除（留在 conflicts） | 同站同时隙不同数值，被更新时间更晚的版本取代 |
| conflict_unresolved | quarantine | 最新版本之间数值仍不一致，无法判断 |
| corrupt_record, station_missing, ts_unparseable, unaligned_timestamp, out_of_window, type_unit_unexpected, value_missing, value_non_numeric, value_negative, value_precision | quarantine | 按此顺序取第一个命中的原因；`value_precision` 只在 `excess_precision: quarantine` 时启用 |

`reports/tables/cleaning_log.csv` 每年一行，`raw_rows` = 所有 fate 之和，可与 Spark 计得的原始行数对账。
