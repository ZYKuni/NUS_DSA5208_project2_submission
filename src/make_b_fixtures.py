"""Deterministic synthetic fixtures. Never represent observations or model results."""
import argparse
import csv
from pathlib import Path
from datetime import datetime, timedelta, timezone


def write(path, header, rows):
    with path.open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def generate(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    write(output/'stations.csv', ['station_id', 'latitude', 'longitude', 'comparable'],
          [('DEMO_A', 1.30, 103.80, True), ('DEMO_B', 1.36, 103.90, True),
           ('DEMO_DRY', 1.40, 103.75, True), ('DEMO_ABSENT', 1.35, 103.70, True)])
    start = datetime(2023, 12, 1, tzinfo=timezone(timedelta(hours=8)))
    end = datetime(2024, 2, 1, tzinfo=start.tzinfo)
    def clean():
        t, i = start, 0
        while t < end:
            for station in ['DEMO_A', 'DEMO_B', 'DEMO_DRY']:
                if station == 'DEMO_B' and i % 12 == 0:
                    continue  # Missing rows are not zero rainfall.
                rain = 0. if station == 'DEMO_DRY' else (0.2 * (i % 5 + 1) if i % 7 == 0 else 0.)
                yield station, t.isoformat(), rain, 'valid'
            t += timedelta(minutes=5)
            i += 1
    write(output/'clean.csv', ['station_id','timestamp_sgt','rainfall_mm','quality_flag'], clean())
    def predictions():
        for model in ['demo_raw', 'demo_calibrated']:
            for i in range(8):
                for q in [0., 2.]:
                    # Hand-authored labels; deliberately no future-rainfall label builder.
                    y = [0,0,1,1,0,1,0,1][i] if q == 0 else [0,0,0,1,0,0,0,1][i]
                    p = ([0.1,0.4,0.6,0.8,0.2,0.5,0.3,0.9][i] if q == 0
                         else [0.05,0.2,0.3,0.7,0.1,0.25,0.15,0.8][i])
                    if model == 'demo_calibrated':
                        p = 0.9*p + 0.02  # Illustrative scores, not fitted calibration.
                    yield ('FIXTURE', model, f'DEMO_{i}', 'DEMO_A',
                           (start+timedelta(minutes=5*i)).isoformat(), q, y, p,
                           'validation', 'calendar')
    write(output/'predictions.csv', ['run_id','model_name','base_sample_id','station_id',
          'prediction_time','threshold_mm','label','probability','split','inference_mode'], predictions())
    (output/'FIXTURE_ONLY.txt').write_text('SYNTHETIC DATA ONLY. Not rainfall observations or model results.\n')

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--output', required=True)
    generate(p.parse_args().output)
