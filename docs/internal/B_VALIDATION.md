> 历史记录：本页保留 2026-10-06 的执行状态。C 精度缺陷和真实模型交付已完成，最新验收见 docs/B/STATUS.md 与 reports/b_model_review/validation.json。

# B 验证记录 — 2026-10-06

## 实际运行环境

- 本地仓库：/Users/arlo/nus/DSA5208/project2/NUS_DSA5208_project2
- 功能分支：feat/B-parallel-analysis；修改尚未提交或推送。
- 克隆基线 commit：2e0ea3a93175a47b70c6093b13a4d0e5df0616a4
- Python 3.11.16；PySpark 3.5.8；Temurin JDK 17.0.20.1+1。
- 本地 Spark local[2]、shuffle partitions=2、SGT。
- Python 依赖沿用仓库 requirements.txt。项目隔离环境在 .venv；JDK 在 .runtime；均已 Git 忽略。

## 已执行的验证

1. `source scripts/b_env.sh` 后 `python -m pytest tests/test_b_semantics.py -q`：**13 passed**；最近一次 12.28 秒。包含分母/干时隙、闰日、缺失月份、迁移站、SGT、非法雨量/时间、重复键、手算概率指标、单类 AUC、概率箱端点、跨集泄漏和重复预测点。存在一条 PySpark 自身 distutils 弃用警告。
2. `bash scripts/run_b_demo.sh`：完整人工样本流程成功，包含 52,080 条有效观测、32 行预测、4 个元数据站点、2 个月份。输出 Spark Parquet 汇总表和 8 张 PNG 图，所有图均标记人工测试，不能用于课程真实结果。
3. 端到端断言：90%/95%/98% 覆盖率筛选分别保留 3/2/2 个站点；完全缺测站月雨量为 null、覆盖率为 0；全干站条件湿强度为 null；demo_raw 在 q=0 时 Brier=0.095、TP/FP/TN/FN=4/0/4/0，与手算一致。可复现脚本：`python scripts/check_b_demo.py reports/demo_b`。
4. 图表可视检查后修正年份小数刻度、经度 offset 显示、空间标签间距和 PR-AUC 纵轴。最终预览位于 `reports/demo_b/eda_figures_reviewed` 与 `evaluation_figures_reviewed`。以后使用当前脚本生成的图自动带这些修正。
5. `git diff --check` 无空白错误。代码可编译性、最终源码 SHA-256 清单与依赖快照见 logs/b_*。

完整流程先通过，随后增加了同一预测点换 ID 的拒绝校验并重跑所有 13 项语义测试；绘图用已有聚合表重生成并检查。统计计算公式未在此轮修改。未重复全量样例聚合。

## 文件与证据

- `logs/b_tests.log`：测试输出。
- `logs/b_demo.log`：完整 Spark 样例运行日志。
- `logs/b_environment.txt`：Python 包版本快照。
- `logs/b_source_sha256.json`：最终代码与配置的内容校验值，用于在未提交状态下追踪此次版本。
- 每套聚合表的 manifest.json：输入路径、数据版本、配置及 SHA-256、生成时间、人工数据标识。

logs 和 demo 产物不自动进入 Git；需要交付复核时可以单独打包。仓库保留可重建它们的代码、配置、夹具生成器和说明。

## 未完成的验收

没有真实八年数据、真实模型结果、A/C 接口确认或人工组员复核。未验证全量吞吐、真实样本覆盖公平性、标签构造、实时可用性、模型重载或课程最终提交要求。这些应在交接后由对应成员验收，不能从人工样例通过推断。
