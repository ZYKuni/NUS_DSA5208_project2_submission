> 历史记录：本页保留 2026-10-06 的执行状态。C 精度缺陷和真实模型交付已完成，最新验收见 docs/B/STATUS.md 与 reports/b_model_review/validation.json。

# B 收尾验证 — 2026-10-06

完整实际命令：

```bash
source scripts/b_env.sh
bash scripts/run_b_real.sh reports/b_reproduction/verification_run
```

结果：从已有 A 清洗层出发，Spark EDA、绘图、C 窗口汇总、极值上下文核验、A/B 对账、CSV 导出、补充图、工作稿全部执行成功；本次链路耗时 **91 秒**，不包含下载和清洗时间。初次 85 秒记录属于此前链路，不能当成本次完整流程耗时。

- 8 个年度源文件 SHA-256 与 A 一致。
- 8,736 站月有效时隙、湿时隙和覆盖率一致；48,538,627 清洗行与年度站点数一致。
- 新目录生成的年度参考结果、极值敏感性、邻站审计、月份×小时表，与发布目录在数值容差内一致（rtol=1e-10，atol=1e-12）。
- 每个月份×小时单元有20个参考站点，共288个单元；没有把空单元当成零雨量。
- 另用 Python Decimal 由单站年度分子/分母独立复算极值排除敏感性，与结果一致。
- 自定义输出目录不会覆盖原 reports/report.md（前后 SHA-256 一致）。
- 报告引用的图片均存在；F4b、三个F8及此前核心图经过视觉检查。
- Python 编译、shell 脚本语法和 git diff --check 通过。
- 核心 A/B/C 测试仍为51通过、1个明确的C边界问题预期失败；本轮只修改补充绘图、工作稿与复现脚本，未把已知问题标为修复。

待解问题与真实模型交接见 STATUS.md、UPSTREAM_REVIEW.md；不能由B的描述分析通过推断模型标签/正式实验已验收。
