# 时间语义与五分钟对齐（P1-02）

状态：**规则已实现，参数待全量审计后冻结。** 本文件中的"假设"尚未被数据或官方文档证实。

## 三个时间字段

| 字段 | 用途 | 处理 |
|---|---|---|
| `timestamp`（原始观测时间） | 唯一用于确定时隙的字段 | 原样保留为 `raw_timestamp`；按 ISO 8601 带 `+08:00` 解析，**不再额外加 8 小时** |
| `update_timestamp` / `reading_update_timestamp` | 数据发布/更新时刻 | 解析后保留；只用于去重排序和 `available_at` 代理 |
| `date` | 原始日期列 | 仅审计：统计其与 `timestamp` 的 SGT 日期不一致的行数（`raw_inventory.csv` 的 `date_col_mismatch`） |

Spark 会话、JVM 和 Python 进程的时区都在 `src/common.py:get_spark` 中固定为 `Asia/Singapore`。

## 区间语义（假设）

读数类型为 "TB1 Rainfall 5 Minute Total F"，即五分钟累计量。**假设** `timestamp` 标记累计区间的**末端**：标准时间 `t` 的值 `r(s,t)` 表示 `(t-5min, t]` 内的降雨。官方页面未明确说明，报告中须写成假设。

日历归属：时隙按 `timestamp_sgt` 自身的日期归入某一天，即每天 00:00…23:55 共 288 个时隙。在末端语义下，00:00 的值物理上属于前一天 23:55–24:00；对日/月汇总影响极小，但在报告中说明。

## 对齐规则（`configs/project.yaml` → `cleaning`）

设 `r = epoch_seconds % 300`（SGT 偏移 8h 是 300s 的整数倍，所以 UTC 与 SGT 下 r 相同）：

- `r == 0`：已对齐，原样使用。
- `r >= 300 - snap_up_max_seconds`（默认 1，即 `xx:x4:59`）：向上归到下一个整五分钟，如 08:04:59 → 08:05:00。
- `r <= snap_down_max_seconds`（默认 0）：向下归。
- 其他：**隔离**为 `unaligned_timestamp`，不四舍五入。

每行记录 `align_shift_s`（修正秒数）和 `raw_timestamp`。

## 冻结前要做的检查

1. 运行 `python src/data_quality.py --stage raw`，查看 `reports/tables/time_phase_by_year.csv`、`time_phase_by_station_year.csv` 和 `time_seconds_by_year.csv`。
2. 若某年大量出现 r=0、299 以外的相位（2024 年 API 预览的摘要中出现过 `00:05:26`，尚未确认属于哪个字段），先确认是观测时间还是更新时间，再决定是否放宽规则；修改参数后重跑，并把依据记入 `docs/A/decisions.md`。
3. 若 `unaligned_by_rule` 占比不可忽略，不能直接丢弃了事：报告影响行数和年份分布，并考虑敏感性分析。
