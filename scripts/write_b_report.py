"""Generate B's report draft from reconciled bounded aggregate artifacts."""
import argparse,json
from pathlib import Path
import pandas as pd
p=argparse.ArgumentParser();p.add_argument('output',default='reports/b_results',nargs='?');args=p.parse_args()
root=Path(args.output);c=root/'csv'
def read(name):return pd.read_csv(c/(name+'.csv'))
def md(frame):
    cols=list(frame.columns)
    lines=['| '+' | '.join(cols)+' |','| '+' | '.join(['---']*len(cols))+' |']
    for row in frame.itertuples(index=False,name=None):
        lines.append('| '+' | '.join(str(x) for x in row)+' |')
    return '\n'.join(lines)
annual=read('reference_annual_80').sort_values('year')
refs=read('analysis_stations').sort_values('station_id')
monthly=read('year_month');hours=read('hourly').sort_values('hour')
spatial=read('spatial');sens=read('reference_sensitivity');overview=read('overview_year')
windows=read('window_status');dist=read('window_distribution');top=read('top_30m_candidates').sort_values('future_30m_mm',ascending=False)
peak=hours.loc[hours.station_mean_wet_frequency.idxmax()];low=hours.loc[hours.station_mean_wet_frequency.idxmin()]
wetmax=annual.loc[annual.station_mean_wet_frequency.idxmax()];wetmin=annual.loc[annual.station_mean_wet_frequency.idxmin()]
spmax=spatial.loc[spatial.wet_frequency.idxmax()];spmin=spatial.loc[spatial.wet_frequency.idxmin()]
valid_windows=int(windows.loc[windows.label_available.astype(str).str.lower()=='true','count'].sum())
invalid_windows=int(windows['count'].sum())-valid_windows
mask=int((monthly.min_coverage<.9).sum())
neighbor=read('extreme_neighbor_review').sort_values('target_future_mm',ascending=False).iloc[0]
change=read('extreme_sensitivity').iloc[0]
annual_print=pd.DataFrame({'年':annual.year.astype(int),'站点数':annual.station_count.astype(int),
    '最低年度覆盖率':annual.min_coverage.map(lambda x:f'{x:.2%}'),
    '站点均值雨频':annual.station_mean_wet_frequency.map(lambda x:f'{x:.3%}'),
    '平均雨量(mm/有效5分钟)':annual.station_mean_mm_per_valid_slot.map(lambda x:f'{x:.5f}')})
text=f'''# DSA5208 Project 2 — B 的真实数据分析工作稿

状态：Task 1 已有真实结果并完成与 A 的汇总对账；仍待团队人工复核。Task 2 的独立真实预测复核和图表见 ../b_model_review/B_MODEL_RESULTS.md；全文统稿见 ../GROUP_REPORT.md。本稿是 Task 1 专题材料。

## 数据来源与运行范围

直接复用 A d90f400 的代码和规则。下载 2017—2024 年八个文件共 8,047,044,701 字节，所有 SHA-256 与 A 清单一致。48,538,713 原始行经 A 管道得到 **48,538,627** 清洗行，差额 86 行由 A 的清洗流水解释。91 个站点在八年中曾出现；不是每年都有 91 个有效站点。B 的 8,736 个站点×月份有效/湿时隙数和日历覆盖率与 A 表逐项一致，年度行数/站点数也一致。详见 validation.json 和 csv/overview_year.csv。

主机沿用本地 Python 3.11.16、Spark 3.5.8、JDK 17；本次 A 清洗和 B 分析使用 local[4]、6g driver。原始/清洗/中间数据不进入 Git；可按 scripts/run_b_real.sh 重建分析。A 负责原始清洗解释，B 没有重写清洗或标签。

## 统计口径与可比性

雨频（wet frequency，有雨频率）= 正雨量有效时隙数/有效时隙数；强度用 mm/有效五分钟时隙和湿时隙条件均值分别描述。缺测不是零雨量。日历覆盖率每完整日预期 288 个五分钟时隙，含闰日，按时间戳本身归属年月。

A 目前把五分钟累计的时间戳当作区间末端，仍属需明确披露的假设。2017 的 :59 按 A 规则向上修正一秒。B 使用同一时间口径，不额外加八小时。坐标/设备唯一仅是数据内稳定性代理，不等于官方站点历史核验。

### F1/F3：原定严格站点集合不可用

八年每月都达到 90%、95% 或 98% 的站点数均为 **0**。2018 年 8 月所有站点中的最高覆盖率仅约 **7.53%**。这说明共同缺测本身足以使严格方案不可用，不能将低累计雨量解释为当月干旱。F3 保留“没有符合条件站点”的结果，未暗自放宽门槛后沿用原方案名称。

![F1 覆盖率](figures/F1_coverage.png)

![F3 严格集合为空](figures/F3_annual.png)

### 固定参考站点补充方案

在查看雨量聚合前，仅根据 A 覆盖率冻结：每年覆盖率 >=80%，且位置与设备各唯一。共 **{len(refs)} 个固定站点**：{', '.join(refs.station_id)}。75% 和 82% 作敏感性分析（sensitivity analysis）。这不是原先每月 95% 质量门槛的替代验收，属于透明的补充探索。

{md(sens.rename(columns={'coverage_threshold':'年度门槛','station_count':'固定站点数','required_years':'要求年数'}))}

## Task 1 结果

### F2：月份变化

在固定参考站点上按站点等权汇总有雨频率。96 个年月中有 **{mask} 个**由于至少一个参考站点当月覆盖率不足 90% 而在图中遮蔽，原值仍保留于 year_month.csv。空白不是零降雨。该图只支持对观测较完整年月的描述，不应把不同可用月份简单比较成季风效应或年际气候变化。

![F2 月雨频](figures/F2_monthly.png)

### F3b：年际频率与强度

{md(annual_print)}

在这一固定参考集合中，{int(wetmax.year)} 年有效时隙雨频最高（{wetmax.station_mean_wet_frequency:.3%}），{int(wetmin.year)} 年最低（{wetmin.station_mean_wet_frequency:.3%}）。这不是完整全年总降雨量的排名。以有效观测时长归一化缓解了暴露量不同，但缺测可能依赖时段或天气，不能证明消除了偏差。门槛敏感性曲线同时给出各集合站点数，不逐年更换站点。

![F3b 参考站点敏感性](figures/F3b_reference.png)

### F4：日内模式

固定参考站点有效观测中，等权平均雨频最高的小时为 SGT **{int(peak.hour):02d}:00–{int(peak.hour):02d}:59**（{peak.station_mean_wet_frequency:.3%}），最低为 **{int(low.hour):02d}:00–{int(low.hour):02d}:59**（{low.station_mean_wet_frequency:.3%}）。频率、所有有效时隙平均雨量和湿时隙条件均值并列，避免把“更常下雨”与“下雨时更强”混为一谈。结果未控制各小时的非随机缺测，不能推断物理机制。

![F4 日内频率强度](figures/F4_diurnal.png)

月份×小时的补充图使用全期该月该小时的有效观测，站点等权，未做 F2 的逐年月遮蔽；各单元分母见 month_hour_plot.csv。

![F4b 月份小时模式](figures/F4b_month_hour.png)

### F5：空间差异

固定参考站点观测雨频范围为 {spmin.wet_frequency:.3%}（{spmin.station_id}）至 {spmax.wet_frequency:.3%}（{spmax.station_id}）。使用 A 元数据中的稳定位置作站点散点图，未插值成全国降雨场。站点等权均值不是面积加权全国平均；选站、覆盖率、局部仪器与环境差异都可能影响空间比较，待 A 复核。

![F5 空间散点](figures/F5_spatial.png)

### E6：完整未来 30 分钟窗口与极值候选

直接调用 C 的 build_future_30m_base，48,538,627 个已观测锚点中 **{valid_windows:,}** 个有完整有效的未来六个时隙，**{invalid_windows:,}** 个不完整。分母是已有观测锚点，不是完整日历预测网格。见 window_status.csv 和 window_distribution.csv；跨年上下文来自完整清洗层。

观测到的最高 30 分钟累计候选为 **{top.iloc[0].future_30m_mm:.3f} mm**，站点 **{top.iloc[0].station_id}**，预测锚点 **{top.iloc[0].prediction_time}**。top_30m_candidates.csv 中相邻锚点可能来自同一场降雨，不能算作互相独立的 30 次事件。B 另对排名最高的三个站点日候选提取过去 60 分钟及未来 30 分钟观测和邻近坐标范围的站点上下文，见 case_context.csv；六个未来值按 A 的 Decimal 精度独立求和，与 C 候选值在 1e-9 内相符，见 case_window_check.csv。这只核验算术，不自动证明传感器记录的物理真实性，仍待 A/C 复核。

以下三图以显式 SGT 锚点划分历史观测和未来六时隙，同时展示坐标邻域的记录；历史观测不代表预测时一定可用，且这些不是模型错误案例。

![F8.1 极值上下文](figures/F8_extreme_1.png)

![F8.2 极值上下文](figures/F8_extreme_2.png)

![F8.3 极值上下文](figures/F8_extreme_3.png)

最高候选的矩形邻域有 **{int(neighbor.neighbor_station_count)} 个站点**，其中 **{int(neighbor.complete_neighbor_count)} 个**拥有全部未来六时隙；邻站的 **{int(neighbor.neighbor_valid_slots)} 条**未来观测中有雨记录为 **{int(neighbor.neighbor_wet_slots)} 条**（extreme_neighbor_review.csv）。这构成需 A 优先检查的孤立极值证据，不能据此直接断言故障或删除原值。

E7 极值敏感性：仅在一个明确的替代计算中排除该候选的六个目标站时隙（当作未知，而不是补零），固定20站点{int(change.year)}年每有效时隙平均雨量从 {change.before_mean_mm_per_slot:.6f} 变为 {change.after_mean_mm_per_slot:.6f} mm，相对变化 {change.relative_mean_change:.3%}；有雨频率从 {change.before_wet_frequency:.4%} 变为 {change.after_wet_frequency:.4%}。该局部检查不代表其他异常都已排除，原始主结果仍保留原值（extreme_sensitivity.csv）。

早期发现的 Decimal 严格阈值边界错误已由 C 修复，相关回归测试已转为正常通过。原始失败证据作为历史记录保留。本节是极值描述材料，模型验收见 B 的独立模型复核。

## Task 2：真实结果已独立复核

C 已交付基线、LR/GBT、校准、冻结测试预测和 v2 接口。B 在 2026-10-12 校验模型包并独立重算全部 12 组导出验证/测试预测，计数完全一致、指标差异低于 1e-12；还独立重载了三个真实接口案例。详见 reports/b_model_review/validation.json 和 B_MODEL_RESULTS.md。

v1 原始冻结成绩与 v2 后处理补充评估分开记录。v2 使用已查看的 2024 样本，不能声称新未触碰测试。合成夹具仅用于测试，不作为模型成绩或正式图表。

## 讨论与交接限制

- 严重共同缺测和固定站点选择偏差限制跨年/跨月解释；频率归一化不保证消除偏差。
- 更新时间只是可用时间代理，真实历史发布版本和实时可用性还需 C 的回测协议说明。
- 相邻未来窗口重叠；不能把每行视为独立样本给过窄置信区间。
- 八年描述统计不证明长期气候变化，更不支持 ENSO、城市化等因果解释。

## 可复现证据

- 原始来源与 SHA-256：data/manifest.csv（A）；B 下载校验 logs/b_download_validation.txt。
- A 方法：docs/A/data_dictionary.md、docs/A/time_semantics.md、docs/A/decisions.md。
- B 配置与规则：configs/b_real.yaml、docs/B_COVERAGE_DECISION.md。
- 命令：source scripts/b_env.sh 后 bash scripts/run_b_real.sh <新输出目录>。
- 表、图与源码校验：本目录 csv/、figures/、validation.json。
'''
(root/'B_RESULTS.md').write_text(text)
print(root/'B_RESULTS.md')
