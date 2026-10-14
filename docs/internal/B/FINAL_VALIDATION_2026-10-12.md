# B 收尾验证（2026-10-12）

- 远端同步基线 main 75b93bb；本次变更仅 B 适配、独立复核、图表、报告与交付辅助，不修改 C 已冻结代码/模型/特征。
- `python -m pytest tests -q`：73 passed，2 dependency deprecation warnings，28.54 秒。
- `python -m scripts.review_b_models`：12 组导出数据、2,684,632 行（不同模型/阈值的重复 origin，不是独立事件总量）；128 组概率指标。计数和混淆矩阵完全一致；最大连续指标差异 6.04e-14。
- v2 31,798 个测试 origin / 254,384 个阈值行：领域校验通过；222,586 对相邻阈值概率单调性违反数 0（v1 校准后为 104）；六训练阈值 mean Brier=0.019245697789368393。明确是事后补充评估。
- 3 个 B 本地 clean history + 已保存模型的 v2 重载案例匹配批量结果（误报、漏报、回退）；保留源概率与警告证据。
- 真实错误案例未来六个 Decimal 值逐项求和等于 C 的目标；历史图分开显示预测时可用和不可用观测。
- 模型 ZIP 认证下载、SHA-256 与 CRC 均通过；v1 冻结 fingerprints 前后完全相同。
- PDF 候选版 16 页，姓名学号、AI 声明、两任务和逐 q 完整指标/混淆矩阵均已包含。渲染逐页检查；当前 output/pdf 与 reports 的 PDF 内容一致。
- 提交候选 ZIP 在 output/submission，逐文件 SHA-256 和 CRC 验证通过，附清单与校验文件。该目录不进 Git，组员可用 `python -m scripts.package_group_submission` 重建。
- 没有执行 Canvas 上传；人工认可及官方未规定上限的 q 与当前 v2 0-10 mm 适用域仍需团队处理。详见 OFFICIAL_COMPLETION.md。
