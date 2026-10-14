"""Finish B's month-hour and extreme-window figures from bounded Spark summaries."""
from pathlib import Path
import argparse,json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def bounded_csv(path):
    frame=pd.read_csv(path,nrows=20001)
    if len(frame)>20000:raise ValueError(f'Not a bounded summary: {path}')
    return frame


def run(root):
    root=Path(root);csv=root/'csv';figures=root/'figures'
    manifest=json.loads((root/'tables'/'manifest.json').read_text())
    cfg=manifest['config'];version=cfg['data_version']
    index=[]
    def save(fig,name,caption,sources):
        fig.tight_layout(rect=(0,0,1,.95))
        fig.savefig(figures/(name+'.png'),dpi=180)
        plt.close(fig)
        (figures/(name+'.md')).write_text(caption+'\n\nSource tables: '+', '.join(sources)+'\nData version: '+version+'\n')
        index.append({'figure':name+'.png','caption':caption,'sources':sources})
    mh=bounded_csv(csv/'month_hour.csv')
    # Averages of already-Spark-aggregated station summaries, not raw-data ETL.
    view=mh.groupby(['month','hour'],as_index=False).agg(
        station_mean_wet_frequency=('wet_frequency','mean'),
        station_count=('station_id','nunique'),valid_slots=('valid_slots','sum'))
    view.to_csv(csv/'month_hour_plot.csv',index=False)
    grid=view.pivot(index='month',columns='hour',values='station_mean_wet_frequency')
    fig,ax=plt.subplots(figsize=(11,5))
    im=ax.imshow(grid.to_numpy()*100,aspect='auto',origin='lower',cmap='Blues')
    ax.set(xticks=np.arange(24),xticklabels=grid.columns,yticks=np.arange(12),yticklabels=grid.index,
           xlabel='SGT hour (timestamp endpoints)',ylabel='Month',title='F4b: Month-by-hour wet frequency')
    fig.colorbar(im,ax=ax,label='Equal-station mean wet frequency (%)')
    save(fig,'F4b_month_hour',
         '固定参考站点的月份×小时有雨频率。先在各站汇总有效/湿时隙，再对站点等权平均；分母不含缺测。'
         '使用整个研究期该月该小时的可用观测，未做逐年月90%筛选，不能与F2遮蔽后口径混用。'
         '各单元站点数和有效时隙数保留于month_hour_plot.csv；颜色差异可能受非随机缺测影响。',
         ['month_hour.csv','month_hour_plot.csv'])
    context=bounded_csv(csv/'case_context.csv');checks=bounded_csv(csv/'case_window_check.csv')
    neighbors=[]
    for row in checks.itertuples():
        f=context[(context.case_id==row.case_id)&(context.observed_station!=row.case_station)&(context.relative_minutes>0)]
        counts=f.groupby('observed_station').size()
        neighbors.append({'case_id':row.case_id,'case_station':row.case_station,'anchor':row.anchor,
            'target_future_mm':row.decimal_future_mm,'neighbor_station_count':len(counts),
            'complete_neighbor_count':int((counts==6).sum()),'neighbor_valid_slots':len(f),
            'neighbor_wet_slots':int((f.rainfall_mm>0).sum()),'max_neighbor_5min_mm':f.rainfall_mm.max()})
    pd.DataFrame(neighbors).to_csv(csv/'extreme_neighbor_review.csv',index=False)
    # Explicit sensitivity, not a revised cleaning rule: remove ONLY the largest
    # candidate's six target observations and recompute its station-year rates.
    largest=checks.sort_values('c_future_30m_mm',ascending=False).iloc[0]
    rows=context[(context.case_id==largest.case_id)&(context.observed_station==largest.case_station)&(context.relative_minutes>0)]
    assert len(rows)==6
    annual=bounded_csv(csv/'station_year.csv')
    refs=set(bounded_csv(csv/'analysis_stations.csv').station_id)
    year=pd.Timestamp(largest.anchor).year
    cohort=annual[(annual.year==year)&annual.station_id.isin(refs)].copy()
    if largest.case_station in refs:
        before_freq=cohort.wet_frequency.mean();before_mm=cohort.mean_mm_per_valid_slot.mean()
        i=cohort.index[cohort.station_id==largest.case_station][0]
        cohort.loc[i,'valid_slots']-=len(rows)
        cohort.loc[i,'wet_slots']-=int((rows.rainfall_mm>0).sum())
        cohort.loc[i,'observed_rainfall_mm']-=rows.rainfall_mm.sum()
        after_freq=(cohort.wet_slots/cohort.valid_slots).mean()
        after_mm=(cohort.observed_rainfall_mm/cohort.valid_slots).mean()
        pd.DataFrame([{'year':year,'excluded_case':largest.case_id,'excluded_slots':6,'station_count':len(cohort),
            'before_wet_frequency':before_freq,'after_wet_frequency':after_freq,
            'before_mean_mm_per_slot':before_mm,'after_mean_mm_per_slot':after_mm,
            'relative_mean_change':after_mm/before_mm-1}]).to_csv(csv/'extreme_sensitivity.csv',index=False)
    for i,row in enumerate(checks.sort_values('c_future_30m_mm',ascending=False).itertuples(),start=1):
        part=context[context.case_id==row.case_id]
        fig,axes=plt.subplots(2,1,figsize=(11,7),sharex=True)
        target=part[part.observed_station==row.case_station].sort_values('relative_minutes')
        colors=np.where(target.relative_minutes>0,'#d76a36','#276b90')
        axes[0].bar(target.relative_minutes,target.rainfall_mm,width=4,color=colors)
        axes[0].set(ylabel='Target rainfall (mm/5 min)',title=f'F8.{i}: {row.case_station} | anchor {row.anchor}')
        axes[0].text(.015,.95,f'Future six slots: {row.decimal_future_mm:.3f} mm\nPast observations are not necessarily available at the anchor',
                     transform=axes[0].transAxes,va='top',fontsize=9)
        for station,observations in part.groupby('observed_station'):
            if station==row.case_station:continue
            series=observations.set_index('relative_minutes').rainfall_mm.reindex(np.arange(-60,31,5))
            axes[1].plot(series.index,series.values,'o-',markersize=3,linewidth=1,label=station)
        axes[1].set(xlabel='Minutes relative to prediction anchor',ylabel='Nearby rainfall (mm/5 min)')
        axes[1].legend(ncol=5,fontsize=8,loc='upper left')
        if (part.loc[part.observed_station!=row.case_station,'rainfall_mm']==0).all():
            axes[1].set_ylim(-.005,.10)
            axes[1].set_yticks([0,.05,.10])
        for ax in axes:
            ax.axvline(0,color='black',linestyle='--',linewidth=1)
            ax.axvspan(0,30,color='#d76a36',alpha=.08)
            ax.set_xticks(np.arange(-60,31,10))
        save(fig,f'F8_extreme_{i}',
             f'站点{row.case_station}，锚点{row.anchor}（显式SGT）；橙色为(t,t+30min]六个实际观测，'
             '蓝色为锚点及之前的观测，不声称这些历史记录在预测时已经发布。下图使用观测记录自身坐标，'
             '经纬度各与目标相差不超过0.06度的站点；该矩形邻域不是精确距离圈。缺失时隙保持断线，'
             '未做空间插值。图仅核验事件上下文，不能验证仪器真实性或代表模型误报/漏报案例。',
             ['case_context.csv','case_window_check.csv'])
    (figures/'supplement_index.json').write_text(json.dumps(index,ensure_ascii=False,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('output',nargs='?',default='reports/b_results')
    run(p.parse_args().output)
