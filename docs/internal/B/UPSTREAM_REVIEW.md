> 历史记录：本页保留 2026-10-06 的执行状态。C 精度缺陷和真实模型交付已完成，最新验收见 docs/B/STATUS.md 与 reports/b_model_review/validation.json。

# B 对 A/C 交接的独立检查

## 已获取的版本

- origin/main c5c003c：C 基础 PR 已合并（实现提交 60169d1）。
- origin/feat/A-P1-data-pipeline d90f400：A 数据管道和审计表，含 C 合并及文档分目录。
- 以上内容已合并到本地 B 功能分支；没有更改远端分支。

## 已解决的接口差异

A 的 quality_flag 是 ok / merged_same_value / conflict_resolved_latest，B 旧接口用 valid；适配器明确映射且拒绝未知值。A 站点字段 latest_latitude/latest_longitude、n_locations/n_devices 用于可追溯的稳定站点筛选。不更改 A 的清洗表。

C 的 v0.1 表没有 B 曾拟定的 run_id/base_sample_id/probability；适配器使用不可变 model_id、站点时刻生成标识，并把 raw/calibrated 分开。校准概率为空时只计入排除审计，不能用 raw 顶替。decision_threshold 按每个指标组验证一致，且复核 prediction 与 probability_source 是否匹配。

C 原始四行夹具已独立复算：raw Brier=0.075，TN=2、TP=2、FP=0、FN=0；4 行 raw 保留、4 行 calibrated 排除（无校准器）。这些是合成数据，不是模型结果或 H2 正式验收。

## C 必须处理的数值边界

B 增加 `tests/test_c_precision_review.py`：A 的 Decimal(10,3) 六个未来值为 0.2、0.4、0、0、0、0，精确累计 0.600；q=0.6 时严格 > 的标签应为 0。C `build_future_30m_base` 将数值转 double 后，累计得到 0.6000000000000001，`expand_thresholds` 给出 1。

该回归测试标记 strict xfail（已知问题预期失败），并保存强制执行失败日志 `logs/b_c_precision_failure.log`。不能把它算成通过。应由 C 与 A 确认十进制累计和阈值表示方式后修复；B 没有另写第二套标签或以容差偷偷改变 > 的定义。此问题影响连续阈值 API，q=0.6 并非必须来自候选训练网格才需正确。

本地 E6 仅使用 C 窗口实现做描述性分布和极端候选，不据此宣称阈值标签通过或展开正式预测实验。候选极值也未通过物理合理性核查。

## 仍需成员交付/确认

A：时间末端语义证据或明确签认假设；确认稳定站点代理及 B 的参考站点分析；极值候选与缺测机制的交叉检查。
C：修复精确阈值边界；交付真实模型、自然分布的验证/测试预测、基线、校准状态与冻结阈值、模型加载和切分证据。当前只有契约和合成夹具。
全体：正式成员信息、课程模板与页数、AI 规则和准确提交时刻。

未向任何组员自动发送消息。本文件可供用户转发或组内 review（复核）。

## B 补充极值证据（非自动清洗规则）

F8.1 的 S107 锚点为 2022-08-21 19:55 SGT，未来六值为 85.600 mm。
以记录坐标为准、经纬度各相差不超过 0.06 度的矩形邻域中，8 个其他站点全部具有未来六条记录，48 条雨量全部为零。
另外两个已核查候选的邻近记录有正雨量。请 A 优先核查 S107 该时段的仪器/更新/原始记录；孤立性不足以自动认定其错误。

B 保留主结果原值，仅做了明确的替代敏感性计算：排除这六个时隙后（分子分母同时扣除，不补零），20 站点参考集合的 2022 年每有效时隙平均雨量下降约 0.150%。
证据：reports/b_results/csv/extreme_neighbor_review.csv、extreme_sensitivity.csv 及 figures/F8_extreme_1.png。
