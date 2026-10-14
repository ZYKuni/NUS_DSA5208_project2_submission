"""Render independent B aggregates and bounded case windows; never collect raw data."""
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT=Path('reports/b_model_review');FIG=ROOT/'figures';FIG.mkdir(exist_ok=True)
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
def read(name):return json.loads((ROOT/name).read_text())
def metric(name,source):
    return sorted([r for r in read(name)['metrics'] if r['model_name'].endswith(':'+source)],key=lambda r:r['threshold_mm'])
def save(fig,name):fig.savefig(FIG/name,dpi=180,bbox_inches='tight');plt.close(fig)

# Same 91 x 96 coverage values, with sparse axis labels for readable PDF sizing.
coverage=pd.read_csv('reports/b_results/csv/station_month.csv')
coverage['ym']=coverage.year*100+coverage.month
grid=coverage.pivot(index='station_id',columns='ym',values='coverage').sort_index()
fig,ax=plt.subplots(figsize=(11,4.6),layout='constrained')
im=ax.imshow(grid.to_numpy(),aspect='auto',vmin=0,vmax=1,cmap='Blues',interpolation='nearest')
ax.set(xticks=np.arange(0,96,12),xticklabels=range(2017,2025),
       yticks=np.arange(0,len(grid),8),yticklabels=grid.index[::8],
       xlabel='Monthly observations, January 2017 to December 2024',ylabel='91 stations (selected labels)',
       title='Monthly coverage: observed valid slots / calendar slots')
fig.colorbar(im,ax=ax,label='Coverage fraction');save(fig,'F1_report_coverage.png')

v1=metric('test_serving.json','calibrated');v2=metric('test_serving_v2_posthoc.json','monotone')
fig,axes=plt.subplots(1,2,figsize=(11,4),layout='constrained')
for name,source in [('M0','raw'),('M1_0.01','calibrated'),('M2_0.01','calibrated'),('M3_5','calibrated'),('serving','calibrated')]:
    rows=metric('test_'+name+'.json',source)
    for ax,k in zip(axes,['brier','pr_auc']):ax.plot([str(r['threshold_mm']) for r in rows],[r[k] for r in rows],marker='o',label=name)
for ax,title in zip(axes,['Brier score (lower is better)','Spark PR-AUC (higher is better)']):
    ax.set(xlabel='Rainfall threshold q (mm; categorical spacing)',ylabel=title);ax.grid(alpha=.2)
axes[0].legend(fontsize=8);fig.suptitle('B independent reproduction: frozen v1, 2024 sampled origins')
save(fig,'F6_v1_models.png')

fig,axes=plt.subplots(2,3,figsize=(12,6.4),layout='constrained',gridspec_kw={'height_ratios':[1.5,1]})
series=[('v1 raw','test_serving.json','raw'),('v1 calibrated','test_serving.json','calibrated'),
        ('v2 post-hoc','test_serving_v2_posthoc.json','monotone')]
for j,q in enumerate([0.,1.,5.]):
    for i,(label,name,source) in enumerate(series):
        rows=sorted([r for r in read(name)['reliability'] if r['threshold_mm']==q and r['model_name'].endswith(':'+source)],key=lambda r:r['bin'])
        axes[0,j].plot([r['mean_probability'] for r in rows],[r['event_rate'] for r in rows],marker=['o','s','^'][i],label=label)
        counts={r['bin']:r['n'] for r in rows};x=np.arange(10)
        axes[1,j].bar(x+(i-1)*.26,[counts.get(k,0) for k in x],width=.25,label=label)
    axes[0,j].plot([0,1],[0,1],'--',color='gray',lw=1)
    axes[0,j].set(xlim=(0,1),ylim=(0,1),title=f'q = {q:g} mm',xlabel='Mean predicted probability',ylabel='Observed frequency')
    axes[1,j].set(yscale='log',xlabel='Probability bin lower edge',ylabel='Bin count (log)',xticks=range(10),xticklabels=[f'{x/10:.1f}' for x in range(10)])
    axes[1,j].tick_params(axis='x',labelsize=8)
axes[0,0].legend(fontsize=8);fig.suptitle('F7: reliability with support counts; sparse bins are uncertain\nv2 is a post-hoc evaluation on the already-opened 2024 sample')
save(fig,'F7_reliability_counts.png')
# Exact counts alongside the graph, including empty bins.
(ROOT/'reliability_counts.csv').write_text('version,q,bin_lower,n,mean_probability,event_rate\n'+''.join(
 f"{label},{q},{b/10},{next((r['n'] for r in read(name)['reliability'] if r['threshold_mm']==q and r['bin']==b and r['model_name'].endswith(':'+source)),0)},"
 +','.join(str(next((r[k] for r in read(name)['reliability'] if r['threshold_mm']==q and r['bin']==b and r['model_name'].endswith(':'+source)),'')) for k in ['mean_probability','event_rate'])+'\n'
 for label,name,source in series for q in [0.,1.,5.] for b in range(10)))

fig,axes=plt.subplots(1,2,figsize=(10,3.8),layout='constrained')
for label,rows in [('v1 frozen',v1),('v2 post-hoc',v2)]:
    for ax,key in zip(axes,['brier','pr_auc']):ax.plot([str(r['threshold_mm']) for r in rows],[r[key] for r in rows],marker='o',label=label)
for ax,key in zip(axes,['Brier','Spark PR-AUC']):ax.set(xlabel='q (mm; categorical spacing)',ylabel=key);ax.grid(alpha=.2);ax.legend()
fig.suptitle('Same origins, different output versions; no refitting or threshold selection')
save(fig,'F9_v1_v2.png')

strata=read('stratified_metrics.json')
fig,axes=plt.subplots(1,2,figsize=(11,4),layout='constrained')
for ax,dim in zip(axes,['known_15m_state','history_group']):
    rows=sorted([r for r in strata if r['model_name']=='serving_v1:calibrated' and r['threshold_mm']==1 and r['group_dimension']==dim],key=lambda r:r['group_value'])
    ax.bar(range(len(rows)),[r['brier'] for r in rows],color='#237eab')
    for i,r in enumerate(rows):ax.annotate(f"n={r['n']:,}\nrate={r['positive_rate']:.1%}",(i,r['brier']),xytext=(0,5),textcoords='offset points',ha='center',fontsize=8)
    ax.set(xticks=range(len(rows)),xticklabels=[r['group_value'].replace('_','\n') for r in rows],ylabel='Brier at q=1 mm',ylim=(0,max(r['brier'] for r in rows)*1.42))
fig.suptitle('v1 subgroups use information available at prediction time\nKnown dry slots do not imply missing slots were dry; weather mix differs between groups')
save(fig,'F10_subgroups.png')

fig,ax=plt.subplots(figsize=(9,3.8),layout='constrained')
monthly=read('test_serving.json')['evaluation_month']
for q in [0.,1.,5.]:
 rows=sorted([r for r in monthly if r['threshold_mm']==q and r['model_name']=='serving:calibrated'],key=lambda r:r['month'])
 ax.plot([r['month'] for r in rows],[r['brier'] for r in rows],marker='o',label=f'q={q:g} mm')
ax.set(xticks=range(1,13),xlabel='Month (2024)',ylabel='Brier',title='v1 monthly errors: event frequencies and available stations also vary');ax.legend();ax.grid(alpha=.2)
save(fig,'F11_monthly.png')

for i,case in enumerate(read('error_cases.json'),1):
    fig,ax=plt.subplots(figsize=(10,3.8),layout='constrained')
    records={r['offset_minutes']:r for r in case['records']}
    for label,color,offset in [('Available history','#237eab',-1.5),('Unpublished/unknown history','#b2bec5',0),('Future outcome','#e18431',1.5)]:
        rr=[r for r in records.values() if (r['offset_minutes']>0 if label=='Future outcome' else
             r['offset_minutes']<=0 and r['available_at_prediction']==(label=='Available history'))]
        ax.bar([r['offset_minutes'] for r in rr],[r['rainfall_mm'] for r in rr],width=3.5,color=color,label=label,
               hatch='///' if label=='Unpublished/unknown history' else None)
    missing=[m for m in range(-60,31,5) if m not in records]
    if missing:ax.scatter(missing,[0]*len(missing),marker='x',color='red',label='Missing observation')
    ax.axvline(0,color='black',ls='--');ax.axvspan(0,30,color='#e18431',alpha=.07)
    ax.set(xlabel='Minutes relative to prediction time (SGT); future is (0,30]',ylabel='Rainfall per 5-minute slot (mm)',xticks=range(-60,31,10),
       title=f"{case['kind'].replace('_',' ').title()}: {case['station_id']} | {case['prediction_time']}\nq={case['threshold_mm']:g} mm, future={case['future_30m_mm']} mm; p(v1)={case['v1_probability']:.4f}, p(v2)={case['v2_probability']:.4f}")
    ax.legend(fontsize=8,loc='upper left');ax.grid(axis='y',alpha=.2)
    save(fig,f'F12_error_{i}.png')

qs=[0.,.2,1.,2.,5.,10.]
macro=lambda rows:sum(r['brier'] for r in rows if r['threshold_mm'] in qs)/6
lines=['# B 独立模型复核与 Task 2 报告材料','',
 '真实 C 交付包已下载并校验；本材料来自 B 本机自动复算，不冒充组员已经完成人工签字验收。',
 '','## 版本与复核范围','',
 'v1 是 C 原始冻结实验；所有候选验证集和最终测试集的导出抽样预测均复算。v2 使用 C 已固定的六结点单调后处理；本次在已经查看过的 2024 样本上补充评估，没有拟合、调参或更换告警阈值，不能解释为新的未触碰留出集。',
 '',f"v1 六训练阈值宏平均 Brier={macro(v1):.8f}；v2 补充评估={macro(v2):.8f}。这两个数字必须标明各自版本。",'',
 '| q (mm) | N | 正例 | v1 Brier | v2 Brier | v1 PR-AUC | v2 PR-AUC | v1 Recall | v2 Recall |',
 '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for a,b in zip(v1,v2):lines.append(f"| {a['threshold_mm']:g} | {a['n']} | {a['positive_count']} | {a['brier']:.6f} | {b['brier']:.6f} | {a['pr_auc']:.6f} | {b['pr_auc']:.6f} | {a['recall']:.3%} | {b['recall']:.3%} |")
lines += ['', '## 图表与解释','',
 '![F6](figures/F6_v1_models.png)','',
 '![F7](figures/F7_reliability_counts.png)','',
 '可靠性图同时展示校准前、校准后及 v2 的概率箱样本量；精确数值见 reliability_counts.csv。少量样本的箱不能支持稳定校准结论。',
 '', '![版本比较](figures/F9_v1_v2.png)','',
 '![分层诊断](figures/F10_subgroups.png)','',
 '分组只读取预测时可用特征；known_15m_state 用最近 15 分钟已知观测描述有雨/无雨/未知，不代表未发布的当前时隙。recent_state 另记录 lag5 可用状态。完整性按已知历史时隙数分组，feature_complete=false 不被剔除。原始分组数值见 stratified_metrics.json，含 N、正例、TP/FP/TN/FN、Brier 和召回率。',
 '', '![月份误差](figures/F11_monthly.png)','',
 '月份和站点组的天气发生率、观测支持不同，不能只凭 Brier 排名解释为模型跨组表现的因果差异。','',
 '## 真实错误案例','']
for i,c in enumerate(read('error_cases.json'),1):
 lines += [f"### {c['kind']}：{c['station_id']}，{c['prediction_time']}",'',f"未来实际累计 {c['future_30m_mm']} mm；q={c['threshold_mm']:g} mm。v1 概率 {c['v1_probability']:.6f}，v2 概率 {c['v2_probability']:.6f}。告警阈值 p>=0.5。",'',f'![错误案例 {i}](figures/F12_error_{i}.png)','']
lines += ['案例依据预先写入脚本的描述规则选择：q=10 的漏报中未来累计最大者，以及 q=0 的误报中概率最高者；按站点和时间确定并列顺序。用于解释错误，不用于调参。六个未来值用 Decimal 独立相加核对 C 的目标。灰色历史观测在预测时未发布，不能拿来解释为模型已经看到了它。',
 '', '## 复现与交接','',
 '运行 `source scripts/b_env.sh`、`python -m scripts.review_b_models`、`python -m scripts.plot_b_model_review`。需要已校验的 C v2 模型包中的 models/features/predictions 和 A 清洗数据。原始 v1 冻结文件校验保持不变。详细对账见 validation.json，重载见 independent_reload.json。',
 '']
(ROOT/'B_MODEL_RESULTS.md').write_text('\n'.join(lines))
print('Rendered B model review figures and report')
