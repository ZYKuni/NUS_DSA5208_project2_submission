"""Build the editable English report and PDF from verified, bounded results.

Requires reportlab and Pillow. Run from repo root: python -m scripts.build_group_report
No model fitting.
"""
import csv
import json
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from PIL import Image as PILImage

ROOT=Path('.'); OUT=Path('output/pdf');OUT.mkdir(parents=True,exist_ok=True)
BR=Path('reports/b_model_review'); ER=Path('reports/b_results')
REPORT_DATE='14 October 2026'
MEMBERS=[('Yikun Zhao','A0353894M','A'),('Zhou Jiahao','A0354090J','B'),('Chen Zheke','A0356140M','C')]
REPO='https://github.com/ZYKuni/NUS_DSA5208_project2_submission'
styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name='BodyCustom',fontName='Helvetica',fontSize=10,leading=14,spaceAfter=8,textColor=colors.HexColor('#233344')))
styles.add(ParagraphStyle(name='CaptionCustom',fontName='Helvetica',fontSize=8,leading=10,spaceAfter=8,textColor=colors.HexColor('#526273')))
styles.add(ParagraphStyle(name='TitleCustom',fontName='Helvetica-Bold',fontSize=29,leading=35,spaceAfter=20,textColor=colors.HexColor('#103b56')))
styles.add(ParagraphStyle(name='TableCustom',fontName='Helvetica',fontSize=8,leading=10))
styles['Heading1'].textColor=colors.HexColor('#103b56');styles['Heading1'].fontSize=19
styles['Heading2'].textColor=colors.HexColor('#237eab');styles['Heading2'].fontSize=12
story=[];md=[]
def p(text):
    story.append(Paragraph(escape(text),styles['BodyCustom']));md.extend([text,''])
def h(text):story.append(Paragraph(escape(text),styles['Heading1']));md.extend(['## '+text,''])
def sub(text):story.append(Paragraph(escape(text),styles['Heading2']));md.extend(['### '+text,''])
def page():story.append(PageBreak());md.extend(['---',''])
def table(headers,rows,widths=None):
    cell=lambda x:Paragraph(escape(str(x)),styles['TableCustom'])
    data=[[cell(x) for x in headers]]+[[cell(x) for x in row] for row in rows]
    t=Table(data,colWidths=widths,repeatRows=1,hAlign='LEFT')
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#dfeef5')),('VALIGN',(0,0),(-1,-1),'TOP'),
       ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f2f5f7')]),('LEFTPADDING',(0,0),(-1,-1),6),
       ('RIGHTPADDING',(0,0),(-1,-1),6),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story.extend([t,Spacer(1,10)])
    md.extend(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |'])
    md.extend('| '+' | '.join(str(x) for x in row)+' |' for row in rows);md.append('')
def fig(path,caption,height=215):
    w,hh=PILImage.open(path).size;scale=min(499/w,height/hh)
    story.append(KeepTogether([Image(str(path),width=w*scale,height=hh*scale),Paragraph(escape(caption),styles['CaptionCustom'])]))
    md.extend([f'![{caption}]({Path(path).relative_to("reports")})',''])
def j(path):return json.loads(Path(path).read_text())
def rows(path):return list(csv.DictReader(Path(path).open()))
def metrics(name,source):return sorted([r for r in j(BR/name)['metrics'] if r['model_name'].endswith(':'+source)],key=lambda r:r['threshold_mm'])
def fmt(v):return 'undefined' if v is None else f'{v:.6f}'
v1=metrics('test_serving.json','calibrated');v2=metrics('test_serving_v2_posthoc.json','monotone')
qs=[0.,.2,1.,2.,5.,10.];macro=lambda rr:sum(r['brier'] for r in rr if r['threshold_mm'] in qs)/6
validation=j(BR/'validation.json');cases=j(BR/'error_cases.json')

story.append(Spacer(1,25));story.append(Paragraph('Singapore rainfall<br/>patterns and 30-minute<br/>exceedance prediction',styles['TitleCustom']))
md.extend(['# Singapore rainfall patterns and 30-minute exceedance prediction',''])
p(f'DSA5208 Project 2 | 2017-2024 annual rainfall records | {REPORT_DATE}')
table(['Group member','Student ID'],[[n,i] for n,i,_ in MEMBERS],[260,239])
sub('Abstract')
p('We process eight annual Singapore rainfall datasets with Apache Spark and analyse within-year and across-year variation while explicitly accounting for missing observations and changing station coverage. The cleaned table contains 48,538,627 station-time records. A transparent 20-station reference set supports descriptive comparisons; strict monthly completeness criteria yield no eligible stations. Observed rain frequency peaks in the afternoon, but coverage gaps limit seasonal and long-term interpretations.')
p(f'For 30-minute rainfall exceedance, we compare empirical, calendar, history-based logistic and gradient-boosted models in Spark MLlib with chronological selection and calibration. The frozen v1 serving system has a six-threshold mean test Brier score of {macro(v1):.6f}, compared with 0.027094 for the empirical baseline. A later monotone v2 interface scores {macro(v2):.6f} on a separately labelled post-hoc evaluation of the same already-opened 2024 sample. Strong-rainfall recall remains poor at the fixed 0.5 alert cutoff.')
sub('Submission contents')
p('The submission contains the complete source code with a README explaining how to run it (README.md), the trained Spark MLlib pipeline models and isotonic calibrators (models/final/c_available_v1/), and this report. Every number in the report is traceable to tables, figures and review records under reports/. The original v1 experiment, frozen before the test year was evaluated, and the later v2 prediction interface are distinguished throughout.')
page();h('1. Data and cleaning')
p('Source [1] provides five-minute rainfall measurements, station information and observation times. We use the eight annual datasets from 2017 through 2024. The download manifest records annual dataset identifiers, source URLs, bytes and SHA-256 checksums. Independently reproduced downloads agree with the eight recorded checksums.')
clean=rows('reports/c_results/cleaning/cleaning_log.csv')
table(['Year','Raw records','Clean records','Same-value rows merged'],[[r['source_year'],f"{int(r['raw_rows']):,}",f"{int(r['kept']):,}",r['same_value_merged']] for r in clean],[65,145,145,144])
p('The raw total is 48,538,713 rows. Cleaning merges 86 same-value duplicate records in 2017, producing 48,538,627 rows with unique station/time keys. Rainfall is stored as Decimal(10,3); values with more than three decimals are rounded to 0.001 mm and flagged. Missing rainfall is not replaced by zero. Negative/non-numeric values, missing identifiers, unparseable timestamps and unresolved conflicts are rejected or quarantined by the pipeline; the actual counts are retained in the cleaning log.')
p('If conflicting versions share a station/time key, the latest reading/update timestamps determine precedence; conflicting latest versions are isolated. The eight annual input files remain immutable. The cleaned table and all subsequent target/feature construction reuse this single shared pipeline.')
sub('Time semantics and information availability')
p('All analysis uses Asia/Singapore (UTC+08:00). Early 2017 observations ending in :59 are snapped upward by one second onto the five-minute grid. Other misalignment is not silently rounded. The interpretation of each reading as a five-minute interval ending at its timestamp is an explicit assumption: the supplied brief specifies five-minute measurements but does not independently establish the interval convention.')
p('For historical prediction, available_at is the greatest of event time and the archived update timestamps. A feature is available only if both event time and available_at are no later than the prediction time, with recognised publication provenance. Archived final versions cannot reconstruct every earlier published version, so this is a conservative availability proxy rather than an exact live replay.')
p('Sources: data/manifest.csv; docs/A/data_dictionary.md; docs/A/time_semantics.md; docs/A/decisions.md; reports/c_results/cleaning/cleaning_log.csv.')
page();h('2. Computing environment and reproducibility')
p('The project uses Python and PySpark DataFrame/SQL for processing, and pyspark.ml for feature pipelines, model fitting, calibration and predictive metrics. Matplotlib renders only bounded aggregate results and explicitly limited case windows. No distributed cloud cluster is claimed; all reported timings are local elapsed times.')
table(['Stage / machine','Runtime','Spark configuration'],[
 ['A: Apple M4, 10 logical cores, 16 GiB; macOS 15.7.3','Python 3.11.15; Spark 3.5.8; JDK 17.0.20.1','local[*]; 6g driver; 64 shuffle partitions'],
 ['C: Apple M2 Pro, 10 cores, 16 GiB; macOS 15.7.7','Python 3.11.7; Spark 3.5.8; JDK 17.0.20.1','local[4]; 4g driver; 32 shuffle partitions'],
 ['B: local review environment, recorded in validation.json',f"Python {validation['environment']['python']}; Spark {validation['environment']['spark']}; local JDK 17",'local[4]; 4g driver; 8 shuffle partitions']],[175,164,160])
table(['Operation','Elapsed time / resource note'],[
 ['A downloads / cleaning / clean audit','421 s / 629 s / 41 s; cleaning sampled peak process RSS about 4.7 GB'],
 ['C label-feature preparation / training + validation / calibration','125.6 s / 71.1 s / 22.4 s'],
 ['B independent model review',f"{validation['seconds']:.1f} s; includes saved prediction metrics, subgroup checks and three model reload cases"],
 ['Storage / reproducibility','Raw files about 7.5 GB. Raw and cleaned full data are not redistributed (reproducible with the download/cleaning scripts); trained models, sampled features and predictions are included']],[185,314])
p('These timings are from different recorded runs and machines and must not be interpreted as controlled scalability comparisons. A records machine and peak RSS evidence; C records its estimator timings. B\'s full platform string and exact review source hashes are in reports/b_model_review/validation.json. Task 1 aggregation used local[4] with a 6g driver.')
sub('Independent checks')
p(f"B recomputed all seven exported candidate validation datasets and all five final test datasets. Sample counts and TP/FP/TN/FN agree exactly; the largest absolute difference across checked floating-point metrics is {max(x['max_absolute_difference'] for x in validation['v1_checks']):.3g}. The original frozen source/model/feature hashes remain unchanged. Three real v2 interface calls (false negative, false positive and no-history fallback) match the batch postprocessing within 1e-12 using B's local clean history and saved models.")
p('A previously performed independent v1 model loading on a different machine: 200 sampled serving predictions and 10 interface calls matched the recorded results. These checks establish reproducibility for the cases tested, not universal correctness or causal validity.')
p('Evidence: docs/A/environment.md; docs/A/model_review.md; reports/c_results/environment.md; reports/b_model_review/validation.json and independent_reload.json.')
page();h('3. Task 1: coverage and comparability')
p('We report wet frequency as positive-rain valid slots divided by all valid slots, and intensity both per valid five-minute slot and conditional on wet slots. A complete day has 288 expected slots. Station means are equally weighted; millimetres are not added across stations to invent a national rainfall total. The eight-year union contains 91 stations, with changing annual availability.')
fig(BR/'figures/F1_report_coverage.png','Figure 1. All 91 station-month coverage series (selected labels). Source: reports/b_results/csv/station_month.csv.',205)
p('B independently reconciled all 8,736 station-month counts/coverage values with A. No station reaches even 90% coverage in every month across all eight years; the 95% and 98% sets are also empty. In August 2018, even the maximum station coverage is only about 7.53%. Low observed totals in such a month cannot be interpreted as drought.')
fig(ER/'figures/F2_monthly.png','Figure 2. Monthly wet-frequency pattern within the fixed reference set. Blank cells denote insufficient coverage, not no rain.',205)
p('A supplementary set uses stations with at least 80% annual coverage in each year and a single observed location/device: 20 stations. Thresholds of 75% and 82% yield 26 and 14 stations. This is a transparent alternative descriptive population, not a claim that the original strict monthly standard was met. Of 96 year-month cells, 60 are masked when any reference station has under 90% monthly coverage. Consequently, strong seasonal comparisons require caution.')
page();h('4. Task 1: changes across years')
annual=sorted(rows(ER/'csv/reference_annual_80.csv'),key=lambda r:int(r['year']))
table(['Year','Stations','Min. annual coverage','Mean wet frequency','Mean mm / valid slot','Mean observed mm / station'],[[r['year'],r['station_count'],f"{float(r['min_coverage']):.2%}",f"{float(r['station_mean_wet_frequency']):.3%}",f"{float(r['station_mean_mm_per_valid_slot']):.5f}",f"{float(r['station_mean_observed_mm']):,.0f}"] for r in annual],[40,52,86,96,105,120])
fig(ER/'figures/F3b_reference.png','Figure 3. Fixed-station annual comparison and coverage-threshold sensitivity. Source: reference_annual_*.csv and reference_sensitivity.csv.',235)
p('Among the 20 reference stations, observed wet frequency is lowest in 2019 (1.986%) and highest in 2023 (3.175%). Average rainfall per valid slot is 0.01680 mm in 2019 and 0.02743 mm in 2021. Frequency and intensity therefore describe related but distinct aspects of observed rainfall. These are not rankings of complete annual rainfall totals.')
p('The last column gives the mean rainfall actually recorded per reference station, about 1,670 mm in 2019 against 2,820-2,830 mm in 2021 and 2022. It is shown for scale only: it is not coverage-adjusted, so a year with more missing slots records less rain by construction. 2019 nevertheless stands out as the driest year on all three measures, while 2021-2023 are the wettest.')
p('Dividing by observed time reduces differences in exposure, but does not remove bias if missingness depends on weather, station or time of day. The station set stays fixed across years; sensitivity curves show what happens under nearby coverage rules. Eight years of such conditional observations do not establish a long-term climate trend or a causal explanation involving urbanisation or large-scale climate drivers.')
page();h('5. Task 1: within-year and spatial patterns')
fig(ER/'figures/F4_diurnal.png','Figure 4. Diurnal frequency, unconditional intensity and wet-slot intensity. Times are SGT.',150)
p('In the reference set, station-mean wet frequency peaks at 15:00-15:59 SGT (5.642%) and is lowest at 23:00-23:59 (1.123%). Afternoon observations are therefore more frequently wet in these data. Non-random hourly gaps and station differences remain possible explanations for part of the observed contrast; the analysis does not identify a physical mechanism.')
fig(ER/'figures/F4b_month_hour.png','Figure 5. Month-by-hour wet-frequency pattern, pooled over 2017-2024. Valid-observation denominators are in month_hour_plot.csv.',150)
p('The month-hour panel shows how the daily profile varies within the calendar year. It pools available observations by month/hour and is not subject to Figure 2\'s per-year/month masking; cells can therefore contain a different mix of years. It is a descriptive seasonal view, not an unbiased estimate of complete monthly rainfall or an independently established monsoon effect.')
sub('Seasonal variation within the year')
mh=rows(ER/'csv/month_hour_plot.csv');mon={}
for r in mh:mon.setdefault(int(r['month']),[]).append((int(r['hour']),float(r['station_mean_wet_frequency'])))
mavg={m:sum(f for _,f in v)/len(v) for m,v in mon.items()};mpk={m:max(v,key=lambda t:t[1])[0] for m,v in mon.items()}
table(['Month']+[str(m) for m in range(1,13)],[['Wet freq.']+[f'{mavg[m]:.1%}' for m in range(1,13)],['Peak hour']+[f'{mpk[m]:02d}' for m in range(1,13)]],[55]+[37]*12)
p('Averaging the month-hour cells over the 24 hours gives a clear annual cycle. Observed wet frequency is highest from November to January (3.7-4.3%) and lowest in February and July (2.0% and 1.9%). The timing of rain also changes with the season. From November to April the wettest hour starts between 15:00 and 17:00 SGT, with a pronounced afternoon maximum. From May to September the peak moves to the morning (an hour starting between 08:00 and 10:00 SGT in May, June, August and September) and the afternoon maximum largely disappears. These observations are consistent in direction with the usual description of Singapore\'s climate: a wet early Northeast Monsoon (December-January) followed by a drier February, afternoon convective showers in the inter-monsoon months (April-May, October-November), and early-morning squall lines during the Southwest Monsoon (June-September). Our data alone do not establish those mechanisms.')
page();h('6. Task 1: spatial variation and extreme checks')
fig(ER/'figures/F5_spatial.png','Figure 6. Observed station wet frequencies at stable-position reference sites. No spatial interpolation is applied.',255)
sp=rows(ER/'csv/spatial.csv');low=min(sp,key=lambda r:float(r['wet_frequency']));high=max(sp,key=lambda r:float(r['wet_frequency']))
p(f"Reference-station wet frequency ranges from {float(low['wet_frequency']):.3%} at {low['station_id']} to {float(high['wet_frequency']):.3%} at {high['station_id']}. The map reveals heterogeneity among observed sites. Coordinates with one device/location in the archive are a stability proxy, not externally verified station histories. Equal station weighting is not area weighting over Singapore.")
sub('Extreme-window arithmetic and sensitivity')
p('The clean table contains 47,636,833 observed origins with six complete future slots and 901,794 without a complete future window. Missing future values are unknown, not zero. B independently checked Decimal sums for three distinct station-day extreme candidates and preserved their neighbouring-station context. Adjacent top-ranked windows may belong to one event and must not be counted as independent storms.')
p('One isolated candidate is S107 at 2022-08-21 19:55 SGT, with 85.6 mm in the next 30 minutes. Eight stations in the chosen rectangular neighbourhood have all 48 combined future slots recorded as zero. This is a reason for sensor/data scrutiny, not proof of error. Main results retain the reading. A separate sensitivity calculation excludes the six target slots as unknown and lowers the 2022 reference-set mean intensity by about 0.150%. Other anomalies may still exist.')
p('Full extreme plots, denominators and sensitivity outputs are retained in reports/b_results/B_RESULTS.md and its csv/ and figures/ subdirectories. They describe observed extremes and are distinct from the predictive error cases in Section 11.')
page();h('7. Task 2: target, features and experiment')
p('Given station s, prediction time t and rainfall threshold q, estimate P(R(s,t,t+30 min) > q). The target sums exactly the next six five-minute slots in (t,t+30 min]; the current slot is excluded. A complete valid future is required. Strict equality to q is negative. Decimal-preserving accumulation and integer 0.001-mm comparisons repair the original floating-point boundary defect: 0.200+0.400 does not exceed 0.6.')
table(['Split','Time interval','Sampled origins','Expanded rows'],[
 ['Train','2017-2022','69,149','414,894'],['Validation','Jan-Sep 2023','25,227','201,816'],
 ['Calibration','Oct-Dec 2023','8,500','51,000'],['Test','2024','31,798','254,384']],[80,145,137,137])
p('The whole target horizon must remain within its split. A seeded, label-independent hash selects 0.2% of eligible training origins and 0.5% of other origins; no class balancing changes event prevalence. M0 uses all eligible training origins. Fitting uses q = 0, 0.2, 1, 2, 5 and 10 mm; evaluation also includes held-out threshold values 0.5 and 3 mm. Expanded rows are threshold copies, not independent origins.')
table(['Feature block','Inputs and rationale'],[
 ['Calendar / station','Training-fitted station encoding; sine/cosine hour and day-of-year; log(1+q), allowing prediction to depend on threshold'],
 ['Available history','Exact 5/10/15/30/60-minute lags; missing flags; known rainfall sums, counts, maxima and wet counts over 15/30/60 minutes'],
 ['History condition','Recent change, staleness and no-history flag; unavailable slots are represented explicitly, not silently declared dry']],[120,379])
p('Only observations published by t enter features. Future rainfall, target completeness and labels are not predictors. Under the strict cutoff, all sampled feature_complete flags are false, yet the test sample averages 9.62 known slots out of 12. Excluding those rows would invalidate evaluation. The serving rule uses the history model when history is available and calendar M1 otherwise: 869 of 31,798 test origins (2.73%) fall back.')
page();h('8. Task 2: models, calibration and v1 results')
p('M0 is empirical training event probability by q. M1 is station/calendar logistic regression; M2 adds available history; M3 is a gradient-boosted tree model with the M2 inputs. LR regularisation candidates are 0.01/0.1; GBT depths are 3/5 with 30 iterations. Selection uses validation mean Brier over the six fitting thresholds. M3 depth 5 is selected; calendar information alone gives little improvement over M0.')
p('A separate increasing isotonic calibration map for each family winner is fitted only on October-December 2023, pooling the six fitting thresholds. Model/source/config/feature files were frozen before v1 test evaluation on 6 October. All confusion metrics use the predefined p>=0.5 cutoff; this is not a tuned operational warning policy.')
modelrows=[]
for name in ['M0','M1_0.01','M2_0.01','M3_5','serving']:
 rr=metrics(f'test_{name}.json','raw');cc=metrics(f'test_{name}.json','calibrated')
 modelrows.append([name,f'{macro(rr):.6f}',f'{macro(cc):.6f}' if cc else 'not applicable'])
table(['v1 model / system','Mean Brier, raw','Mean Brier, calibrated'],modelrows,[235,132,132])
fig(BR/'figures/F6_v1_models.png','Figure 7. Independently reproduced v1 per-threshold Brier and Spark trapezoidal PR-AUC, including empirical and learned baselines.',200)
p('The frozen v1 serving system lowers mean Brier by 28.96% relative to M0 on the sampled 2024 test population. This is a descriptive comparison, not a statistical significance claim. At q=1 mm, event prevalence is 2.629%, Brier 0.019265, ROC-AUC 0.868869 and PR-AUC 0.405617. Precision is 65.0%, but recall is only 26.4% at p>=0.5. At q=10 mm, none of 132 positive origins is detected and there are two false alarms. High accuracy would conceal this rare-event weakness.')
page();h('9. v2: a separately evaluated interface revision')
p('After the v1 test had been inspected, C introduced a guarded interface. It rejects unknown stations, dates outside the archive or a station\'s observed span, unaligned five-minute timestamps, and q outside [0,10] mm. Naive times mean SGT. Warnings identify calendar fallback and exact 0/1 estimates. This is an offline historical-data interface, not a future live-weather service.')
p('v2 evaluates the same saved model/calibrator at six fixed q knots, projects them onto a non-increasing curve using equal-weight PAVA, and linearly interpolates in millimetres. Separate queries therefore use a consistent exceedance curve. No weights or calibration maps were refitted. B applied this already-fixed policy to the same 31,798 test origins and checked that all origins lie within the interface\'s supported domain.')
table(['q (mm)','Events','v1 Brier','v2 Brier','v1 PR-AUC','v2 PR-AUC','v1 recall','v2 recall'],[
 [f"{a['threshold_mm']:g}",str(a['positive_count']),fmt(a['brier']),fmt(b['brier']),fmt(a['pr_auc']),fmt(b['pr_auc']),f"{a['recall']:.1%}",f"{b['recall']:.1%}"] for a,b in zip(v1,v2)],
 [42,48,70,70,70,70,64,65])
p(f'The v2 six-fitting-threshold mean Brier is {macro(v2):.8f}, versus {macro(v1):.8f} for v1. v1 has 104 upward probability violations among 222,586 adjacent q comparisons; v2 has zero on the evaluated grid. Structural coherence improves, but it does not imply uniformly better calibration, ranking or decisions. At q=0.5 and 3 mm, interpolation lowers Brier but also lowers recall at p>=0.5. q=10 recall remains zero.')
sub('Evaluation integrity')
p('These v2 results are explicitly post-hoc on an already-opened test sample. They are not fresh untouched-holdout evidence, and no new alert threshold or model was selected from them. v1 remains the original pre-test-frozen experiment. The submission includes both: v1 for frozen reproduction and v2 for the documented current interface, with separate scores and limits. Prospective performance of v2 requires new evaluation data.')
p('The official brief does not specify a q range. The supported range of 0-10 mm is therefore a stated limitation of the delivered interface; we do not claim arbitrary-threshold or future-time coverage. Extending the supported tail would require additional modelling and validation, not silent clamping.')
page();h('10. Probability calibration and subgroup errors')
fig(BR/'figures/F7_reliability_counts.png','Figure 8. Raw v1, calibrated v1 and post-hoc v2 reliability with log-scale bin counts. Empty bins have no estimate; exact counts are in reliability_counts.csv.',300)
p('Sparse high-probability bins can show 0% or 100% outcomes by chance. For example, some v1 calibrated q=5 bins have only one observation. Isotonic calibration can output exact 0 or 1, which must not be interpreted as certainty. Log loss uses numerical clipping at 1e-15 only for scoring; stored probabilities are not altered.')
fig(BR/'figures/F10_subgroups.png','Figure 9. v1 error by prediction-time known rainfall state and history support at q=1. Counts and event rates are shown; full subgroup tables also contain confusion counts and recall.',195)
p('The 15-minute state uses only known observations: observed dry does not mean unobserved slots were dry. An unknown state is retained. The lag5 state, station, month, fallback and source model are also retained in machine-readable results. Different subgroup event frequencies and support mean Brier comparisons alone cannot identify a causal model failure. Monthly error patterns are shown in the supplementary F11 figure.')
page();h('11. Two real predictive failure cases')
for i,c in enumerate(cases,1):
 fig(BR/f'figures/F12_error_{i}.png',f"Figure {9+i}. {c['kind'].replace('_',' ').capitalize()}. Six future Decimal readings independently match the saved target.",205)
 p(f"At {c['station_id']}, {c['prediction_time']}, q={c['threshold_mm']:g} mm, actual next-30-minute rainfall is {c['future_30m_mm']} mm. v1 estimates {c['v1_probability']:.6f}; v2 estimates {c['v2_probability']:.6f}. Both use the same p>=0.5 alert rule. Orange values occur after prediction and cannot be used as features; grey historical readings were not available at the prediction time.")
p('Selection is descriptive: the largest future accumulation among q=10 false negatives, and the highest-probability q=0 false positive, with deterministic station/time tie breaks. These examples explain limitations and were not used for tuning. Missing observations are marked separately from observed zeros. Publication-time restrictions can hide recent rainfall, and rainfall can also start or stop abruptly beyond the prediction origin.')
page();h('12. Discussion, limitations and conclusion')
p('Task 1 demonstrates afternoon wet-frequency concentration, observed interannual differences and station heterogeneity, while coverage analysis prevents gaps from being misrepresented as dry weather. The strict comparable-station failure is itself an important result. The alternative fixed reference set supports cautious description, not complete national totals or causal climate attribution.')
p('Task 2 demonstrates that available recent rainfall is more predictive than calendar information alone in the tested grid. Spark MLlib pipelines and time-separated calibration produce better probability scores than the empirical baseline, but rare-event recall is poor. The v2 curve corrects threshold incoherence without resolving the underlying difficulty of anticipating severe rainfall.')
sub('Limits affecting interpretation')
p('Observed origins and complete future targets define a conditional evaluation population; absent calendar origins are not synthesised. Non-random missingness, changing stations, final-version publication proxies and uncertain interval semantics constrain validity. Nearby stations, overlapping windows and threshold copies are correlated. We report no independent-row confidence intervals or claims that 254,384 threshold rows are independent events.')
p('The tuning grid is small and predictors omit radar and other meteorological information. Station encoding does not establish generalisation to unseen geography. The saved models were evaluated on historical archives, and the current interface restricts thresholds to 0-10 mm. Probability calibration and good aggregate Brier do not guarantee reliable high-rainfall alarms.')
sub('Additional work')
p('Future work could use validation-only cost-sensitive alarm selection, larger label-independent rare-event samples, coherent distributional models, richer available-at-time predictors, and independent prospective v2 evaluation. Day/week block bootstrap and a 30-minute-spaced origin sensitivity check could quantify dependence-related uncertainty.')
sub('Conclusion')
p('The two required analytical tasks are implemented with real 2017-2024 data and reproducible Spark workflows. Independent metric and model-loading checks support the reported evidence. The final interpretation remains deliberately limited to observed-data patterns, documented feature availability and version-specific historical prediction performance.')
page();h('13. Delivery, contributions, AI and references')
sub('Reproducing the work')
p('README.md gives step-by-step commands. In short: install Python 3.11, Java 17 and the pinned requirements; download the eight annual datasets and run the cleaning pipeline (src/download_data.py, src/clean_data.py); run scripts/run_b_real.sh with a new output directory for Task 1; and run python -m src.task2.run prepare/train/calibrate/test to retrain Task 2 from scratch.')
p('The trained models are included in the submission and need no download: models/final/c_available_v1/ holds the Spark PipelineModels (StringIndexer, OneHotEncoder, VectorAssembler and LogisticRegression or GBTClassifier) for every candidate, the isotonic calibrators of the family winners, selection.json and baseline.json. The saved validation, calibration and test predictions are in data/predictions/c_available_v1/ and the sampled feature tables in data/features/c_available_v1/. python -m scripts.review_b_models recomputes all reported metrics from these files without retraining.')
p('python -m scripts.demo_predict_from_features S06 "2024-11-07 20:50:00" 1 loads the saved pipeline and calibrator, applies them to the stored prediction-time features of that sampled test origin and prints the exceedance probability, the observed outcome and the difference from the saved serving prediction (zero). It needs no raw data.')
p('For an arbitrary query, python -m src.predict_safe S50 "2023-01-01 12:00:00" 1 returns the v2 probability for a station, prediction time and threshold. Because features are built from the station\'s recent readings, this command also needs the cleaned rainfall history (data/clean/clean_rainfall), which is produced by the download and cleaning steps above and is not redistributed because of its size.')
p('This PDF and its editable Markdown version reports/GROUP_REPORT.md are generated by python -m scripts.build_group_report, which needs ReportLab and Pillow (requirements-report.txt).')
sub('Contributions and AI usage')
role={'A':'data acquisition, cleaning, quality audits, computing environment, integration and release','B':'Task 1 exploration and figures, independent checks of the model results, and report writing/layout','C':'Task 2 targets, features, Spark MLlib models, calibration, evaluation and the prediction interface'}
p('Throughout the report, A, B and C refer to the three members: '+'; '.join(f'{k} = {n} ({role[k]})' for n,_,k in MEMBERS)+'.')
p('OpenAI Codex assisted task planning, code implementation, debugging, real-data processing and experiment execution, automated tests, independent metric checks, figure generation and report drafting/layout. For B, assistance included coverage reconciliation, threshold-boundary review, v1 reproduction, v2 post-hoc evaluation and this report. C\'s detailed log additionally records feature, calibration and guarded-interface assistance. AI tools were also used for the final review of the submission and the revision of this report. AI-assisted code was verified with automated tests and independent recomputation of the reported metrics, and the group is responsible for the submitted content. Dated records of AI usage are in docs/ai_usage.md and docs/C/ai_usage.md.')
sub('References and evidence index')
for text in [
 '[1] data.gov.sg. Historical Rainfall across Singapore, annual datasets 2017-2024. https://data.gov.sg/collections/2279/view. Exact annual sources and checksums: data/manifest.csv.',
 '[2] DSA5208 Project 2 official assignment description, supplied course screenshot. Two Spark tasks; report contents; code/README, trained-model and named PDF deliverables; deadline 18 October 2026.',
 f'[3] Team implementation and evidence: {REPO} (submitted version). Methods: docs/A/ and docs/C/model_protocol.md. Original frozen experiments: reports/c_results/.',
 '[4] Task 1 aggregates: reports/b_results/csv/ and validation.json. Independent predictive audit and all report figures: reports/b_model_review/. AI records: docs/ai_usage.md and docs/C/ai_usage.md.',
 '[5] Apache Spark MLlib: https://spark.apache.org/docs/3.5.8/ml-guide.html. Runtime and dependency records are included with the repository; the implementation uses Spark 3.5.8.'
]:p(text)

page();h('Appendix A. Per-threshold probability scores')
p('Each row has 31,798 observed origins with complete targets. Eight threshold rows for one origin are dependent copies. Spark trapezoidal PR-AUC is used, not average precision. Log loss clips probabilities only for numerical scoring at 1e-15. These complete tables supplement the model comparison and version discussion in Sections 8-9.')
for title,rr in [('Frozen v1 serving system',v1),('v2 post-hoc on the already-opened 2024 sample',v2)]:
 sub(title)
 table(['q (mm)','N','Events','Rate','Brier','ROC-AUC','PR-AUC','Log loss'],[
  [f"{r['threshold_mm']:g}",str(r['n']),str(r['positive_count']),f"{r['positive_rate']:.3%}",fmt(r['brier']),fmt(r['roc_auc']),fmt(r['pr_auc']),fmt(r['log_loss'])] for r in rr],
  [37,46,46,55,75,80,80,80])
p('These results are descriptive estimates on the sampled population. The high-threshold low event rate explains why small Brier values can coexist with zero recall. The v2 table does not establish prospective generalisation, because the original test sample was already viewed before the interface revision.')
page();h('Appendix B. Counts at the fixed alert cutoff')
p('The probability alarm cutoff is p>=0.5 for every row and version. It is separate from the rainfall amount q. TP/FP/TN/FN denote true positives, false positives, true negatives and false negatives. Counts, not only rates, make the missed-event and false-alarm burden explicit.')
for title,rr in [('Frozen v1 serving system',v1),('v2 post-hoc on the already-opened 2024 sample',v2)]:
 sub(title)
 table(['q (mm)','TP','FP','TN','FN','Precision','Recall','F1'],[
  [f"{r['threshold_mm']:g}",str(r['tp']),str(r['fp']),str(r['tn']),str(r['fn']),fmt(r['precision']),fmt(r['recall']),fmt(r['f1'])] for r in rr],
  [42,47,47,55,47,87,87,87])
p('Full per-station and monthly original model aggregates remain in reports/c_results/. B independently reproduced month aggregates and added prediction-time known-rainfall, history-support, fallback and source-model groups in reports/b_model_review/. An AUC for any single-class subgroup is undefined, rather than zero or one.')

pdf=OUT/'DSA5208_Project2_Report.pdf'
def footer(canvas,doc):
 canvas.saveState();canvas.setStrokeColor(colors.HexColor('#d6e0e6'));canvas.line(48,39,547,39)
 canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor('#526273'))
 canvas.drawString(48,27,f'DSA5208 Project 2 | Group report | {REPORT_DATE}')
 canvas.drawRightString(547,27,str(doc.page));canvas.restoreState()
SimpleDocTemplate(str(pdf),pagesize=A4,rightMargin=48,leftMargin=48,topMargin=42,bottomMargin=52,
 title='DSA5208 Project 2: Singapore Rainfall',author='; '.join(n for n,_,_ in MEMBERS)).build(story,onFirstPage=footer,onLaterPages=footer)
Path('reports/GROUP_REPORT.md').write_text('\n'.join(md))
import shutil
shutil.copy2(pdf, 'reports/DSA5208_Project2_Report.pdf')
print(pdf)
