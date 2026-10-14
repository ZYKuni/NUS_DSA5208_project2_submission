# B 开发与复现指南（远端集成版）

已集成 A `d90f400` 和 C `60169d1`；输入契约实际适配见 `src/b_analysis/adapters.py`。不再停留在自拟字段的人工演示阶段。当前状态与阻塞清单见 `docs/B/STATUS.md`，独立审查见 `docs/B/UPSTREAM_REVIEW.md`。

## 本机环境与测试

```bash
source scripts/b_env.sh
python -m pytest -q
```

Python 3.11.16、PySpark 3.5.8、JDK 17，使用已有 .venv/.runtime。测试包含 A/C 原有测试、B 统计语义、A/C 适配器以及一个 C 数值边界已知问题（strict xfail，不能视为通过）。

## 完整真实数据复现

```bash
source scripts/b_env.sh
python src/download_data.py --config configs/b_reproduction.yaml
python src/clean_data.py --config configs/b_reproduction.yaml
bash scripts/run_b_real.sh reports/b_results
```

A 原始代码和清洗规则保持不变。B 复现配置仅修改本机输出路径和并行度，原 A `reports/tables` 审计结果不会被覆盖。下载文件 SHA-256 必须与原 A `data/manifest.csv` 一致；本次八年均匹配。重复分析须指定新输出目录，禁止覆盖证据。

`run_b_real.sh` 执行：适配 A → Spark 聚合 → 图表 → 调用 C 的窗口构造 → 描述性窗口分布 → 与 A 的 8,736 个站月及年度统计逐项对账 → 导出小型 CSV、源码/脚本校验值与运行记录，补充月份×小时、极值上下文图和孤立极值敏感性。大表与中间数据留在本地，不进入普通 Git。

## C 预测表接口复现

```bash
source scripts/b_env.sh
spark-submit --master 'local[2]' --conf spark.sql.shuffle.partitions=2 \
  src/verify_predictions.py --input-contract c --config configs/b_c_fixture.yaml \
  --predictions data/samples/predictions_fixture.csv --fixture \
  --output reports/b_c_handoff_new/tables
python src/plots.py --tables reports/b_c_handoff_new/tables --output reports/b_c_handoff_new/figures
```

正式预测需使用 fixture_only=false 的配置、真实数据版本、去掉 --fixture，且输入 is_fixture=false。模型/概率版本分开核验；缺失校准概率不回填 raw；input_audit 表记录保留/排除原因；告警概率阈值使用 C 表中的 decision_threshold，组内必须固定。

## 分析口径

- 雨频 = 正降雨有效时隙 / 有效时隙；平均强度 = 观测雨量和 / 有效时隙，单位 mm/5 min；湿强度的分母只含湿时隙。
- 缺测不是零降雨，整月无观测时雨量为 null，覆盖率为 0。
- 完整日历月分母含闰日，每天 288 个时隙；B 日表只列有观测的日，不宣称完整日历日审计。
- 严格各月 90/95/98% 集合为空。保留空结果，补充固定参考站点分析：每年 >=80%，75/82% 敏感性；仅选数据内位置与设备各唯一的站点。详见 `B_COVERAGE_DECISION.md`。
- 年度报告有效观测频率/强度；不把观测累计量当完整全年总量。月图遮蔽参考站点最低覆盖率 <90% 的年月；原表保留所有覆盖率。
- 跨站使用等权站点均值，不是面积加权全国降雨。八年结果只作描述，不推出气候趋势或因果关系。
- 所有大表计算使用 Spark；绘图最多读取 20,000 行小型聚合表。
- E6 调用 C 原窗口函数，B 不另写标签。当前 C 有十进制边界问题，窗口极值仅作候选和描述，不作为正式模型标签验收。

## 产物

- `reports/b_results/figures`：真实 Task 1 图，标明待团队复核。
- `reports/b_results/csv`：有界聚合 CSV（包含窗口分布/极值候选）。
- `reports/b_results/validation.json`：源代码哈希、上游提交、对账结果。
- `reports/report.md`：完整小组报告入口；B 真实模型复核与 PDF 已完成，最新状态见 B/STATUS.md。
- `reports/b_c_handoff`：C 合成夹具独立复算，仅接口证据。

原 B 人工演示仍可运行：`bash scripts/run_b_demo.sh reports/demo_b_new`。禁止混入正式报告。


2026-10-12 收尾：模型复核、PDF 和提交候选包命令见 README 的“B 完整复核与 PDF 复现”。
