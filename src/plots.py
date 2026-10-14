"""Only bounded Spark aggregates reach Pandas; raw inputs are never loaded here."""
import argparse
import json
from pathlib import Path
import pyarrow.dataset as ds
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np

MAX_ROWS = 20000


def read_small(root, name):
    dataset = ds.dataset(str(root/name), format='parquet', exclude_invalid_files=True)
    if dataset.count_rows() > MAX_ROWS:
        raise ValueError(f'{name} exceeds {MAX_ROWS} rows; aggregate further in Spark')
    return dataset.to_table().to_pandas()


def plot(root, output):
    root, output = Path(root), Path(output)
    manifest = json.loads((root/'manifest.json').read_text())
    output.mkdir(parents=True, exist_ok=False)
    cfg = manifest['config']
    provenance = f"{cfg['start']} to {cfg['end']} (end exclusive) | data: {cfg['data_version']}"
    fixture = manifest['fixture_only']
    index = []
    def save(fig, name, caption, sources):
        label = 'SYNTHETIC FIXTURE - NOT PROJECT RESULTS' if fixture else 'Real observations'
        if fixture:
            fig.text(0.5, 0.015, label+'\n'+provenance, ha='center', fontsize=8, color='firebrick')
        fig.tight_layout(rect=(0, 0.07 if fixture else 0, 1, 1))
        fig.savefig(output/(name+'.png'), dpi=180)
        plt.close(fig)
        (output/(name+'.md')).write_text(caption+'\n\n'+provenance+'\n'+label+'\n')
        index.append({'figure':name+'.png','sources':sources,'caption':caption})
    def heatmap(frame, row, col, value, title, unit):
        pivot = frame.pivot(index=row, columns=col, values=value).sort_index()
        fig, ax = plt.subplots(figsize=(14 if len(pivot.columns)>24 else 10, max(4, len(pivot)*0.20)))
        cmap=plt.get_cmap('Blues').copy();cmap.set_bad('#d9d9d9')
        im = ax.imshow(np.ma.masked_invalid(pivot.to_numpy(dtype=float)), aspect='auto', cmap=cmap)
        step=max(1,len(pivot.columns)//16)
        ax.set(xticks=list(range(0,len(pivot.columns),step)), xticklabels=pivot.columns[::step],
               yticks=range(len(pivot.index)), yticklabels=pivot.index, title=title, xlabel=col, ylabel=row)
        ax.tick_params(axis='x', labelrotation=60)
        fig.colorbar(im, ax=ax, label=unit)
        return fig
    if manifest['kind'] == 'eda':
        monthly = read_small(root, 'station_month')
        monthly['year_month'] = monthly.year.astype(str)+'-'+monthly.month.astype(str).str.zfill(2)
        save(heatmap(monthly, 'station_id', 'year_month', 'coverage', 'F1: Calendar coverage', 'Valid / expected slots'),
             'F1_coverage', 'All metadata stations; full calendar months, 288 expected slots/day. '
             'Zero coverage means no valid observations, not dry weather.', ['station_month'])
        ym = read_small(root, 'year_month')
        rule = f"Fixed comparable stations; every month coverage >= {cfg['primary_coverage']:.0%}. "
        if cfg.get('reference_annual_coverage') is not None:
            n=len(read_small(root,'analysis_stations'))
            rule=f"Supplementary fixed reference stations (n={n}); each year coverage >= {cfg['reference_annual_coverage']:.0%}. "
        if len(ym):
            if cfg.get('reference_annual_coverage') is not None:
                ym=ym.copy()
                ym.loc[ym.min_coverage < cfg['display_min_monthly_coverage'],'station_mean_wet_frequency']=np.nan
                save(heatmap(ym,'year','month','station_mean_wet_frequency','F2: Monthly wet frequency (coverage-screened)','Station mean wet fraction'),
                     'F2_monthly',rule+'Months with any reference station below 90% coverage are masked; raw table retains all values. Missingness may be nonrandom.', ['year_month'])
            else:
                save(heatmap(ym, 'year', 'month', 'station_mean_observed_mm', 'F2: Observed monthly rainfall', 'Station mean (mm)'),
                     'F2_monthly', rule+'Observed totals only; no missing-rain imputation or area weighting.', ['year_month'])
        fig, ax = plt.subplots(figsize=(9,5))
        any_annual = False
        for threshold in cfg['coverage_thresholds']:
            table = read_small(root, 'annual_'+str(round(threshold*100))).sort_values('year')
            if len(table):
                any_annual = True
                ax.plot(table.year, table.station_mean_observed_mm, 'o-',
                        label=f'{threshold:.0%} coverage; n={int(table.station_count.iloc[0])}')
        if any_annual:
            ax.legend()
        else:
            ax.text(.5,.5,'No fixed stations meet coverage rules\n90% / 95% / 98% monthly requirements: all n=0',transform=ax.transAxes,ha='center')
            ax.set_axis_off()
        ax.set(title='F3: Annual observed rainfall (within configured interval)', xlabel='Year', ylabel='Station mean observed rainfall (mm)')
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        save(fig,'F3_annual','Strict fixed stations, every month coverage passes the threshold. '+'90/95/98% sensitivity; partial-year intervals are not full annual totals.',
             ['annual_'+str(round(t*100)) for t in cfg['coverage_thresholds']])
        if cfg.get('reference_annual_coverage') is not None:
            fig,axes=plt.subplots(1,2,figsize=(12,4))
            for threshold in cfg['reference_thresholds']:
                name='reference_annual_'+str(round(threshold*100))
                table=read_small(root,name).sort_values('year')
                if len(table):
                    for ax,col,unit in zip(axes,['station_mean_wet_frequency','station_mean_mm_per_valid_slot'],['Wet fraction','mm per valid 5-minute slot']):
                        ax.plot(table.year,table[col],'o-',label=f'{threshold:.0%} annual coverage; n={int(table.station_count.iloc[0])}')
                        ax.set(xlabel='Year',ylabel=unit)
                        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
                        ax.legend(fontsize=8)
            fig.suptitle('F3b: Supplementary reference-station sensitivity')
            save(fig,'F3b_reference',rule+'Rates normalize observed exposure, not nonrandom missingness; no complete-year rainfall ranking.',
                 ['reference_annual_'+str(round(t*100)) for t in cfg['reference_thresholds']])
        hourly = read_small(root, 'hourly').sort_values('hour')
        fig, axes = plt.subplots(1,3,figsize=(12,4))
        for ax, column, unit in zip(axes, ['station_mean_wet_frequency','station_mean_mm_per_valid_slot','station_mean_wet_mm'],
                                    ['Wet fraction','Mean mm/5 min','Conditional wet mm/5 min']):
            ax.plot(hourly.hour, hourly[column], 'o-')
            ax.set(xlabel='SGT hour', ylabel=unit)
        fig.suptitle('F4: Diurnal frequency and intensity')
        save(fig,'F4_diurnal',rule+'Equal station means. Dry stations have undefined conditional wet intensity; '
             'wet_station_count records its different denominator.', ['hourly','station_hour'])
        spatial = read_small(root,'spatial')
        fig, ax = plt.subplots(figsize=(8,5))
        if len(spatial):
            im=ax.scatter(spatial.longitude,spatial.latitude,c=spatial.wet_frequency,s=100,cmap='Blues')
            fig.colorbar(im,ax=ax,label='Wet fraction per valid slot')
            for row in spatial.itertuples():
                ax.annotate(row.station_id,(row.longitude,row.latitude),xytext=(5,5),textcoords='offset points',fontsize=8)
        ax.ticklabel_format(useOffset=False, style='plain')
        ax.margins(x=0.25,y=0.2)
        ax.set(xlabel='Longitude',ylabel='Latitude',title='F5: Stable station wet frequency')
        save(fig,'F5_spatial',rule+'Coordinates must be confirmed stable by A. Station scatter, not an interpolated national map.', ['spatial'])
    else:
        metrics=read_small(root,'metrics')
        fig,ax=plt.subplots(figsize=(10,5))
        for keys,frame in metrics.groupby(['run_id','model_name','split']):
            frame=frame.sort_values('threshold_mm')
            ax.plot(frame.threshold_mm,frame.pr_auc,'o-',label='/'.join(keys))
        ax.set(xlabel='Rainfall threshold q (mm)',ylabel='Spark areaUnderPR',title='F6: Per-threshold PR-AUC')
        ax.set_ylim(0,1.05)
        ax.set_yticks(np.linspace(0,1,6))
        ax.legend(fontsize=8)
        save(fig,'F6_pr_auc','Each q evaluated separately; single-class AUC is undefined. '
             'Spark exact-threshold trapezoidal PR area, not sklearn average precision.', ['metrics'])
        rel=read_small(root,'reliability')
        for number,(keys,frame) in enumerate(rel.groupby(['run_id','split','threshold_mm'])):
            fig,axes=plt.subplots(1,2,figsize=(10,4))
            axes[0].plot([0,1],[0,1],'--',color='gray')
            for model,part in frame.groupby('model_name'):
                part=part.sort_values('bin')
                axes[0].plot(part.mean_probability,part.event_rate,'o-',label=model)
                axes[1].plot(part.bin,part.n,'o-',label=model)
            axes[0].set(xlabel='Mean predicted probability',ylabel='Observed event fraction',xlim=(0,1),ylim=(0,1))
            axes[1].set(xlabel='Probability bin',ylabel='Sample count')
            axes[0].legend(fontsize=8)
            fig.suptitle(f'F7: Reliability | {keys}')
            save(fig,f'F7_reliability_{number}','Equal-width probability bins [lower,upper); p=1 in final bin. '
                 'Empty bins omitted, no confidence intervals assumed.', ['reliability'])
    (output/'figure_index.json').write_text(json.dumps(index,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--tables',required=True)
    p.add_argument('--output',required=True)
    args=p.parse_args()
    plot(args.tables,args.output)
