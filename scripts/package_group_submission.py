"""Prepare (never upload) a group submission candidate with actual saved models.

Run from repo root: python -m scripts.package_group_submission
Includes current Git-visible source/docs/reports plus the verified C model,
prepared feature and prediction artifacts. Raw/clean data remain reproducible.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

from src.task2.run import fingerprints


def main():
    frozen=json.loads(Path('reports/c_results/frozen_manifest.json').read_text())
    assert frozen['hashes']==fingerprints(),'Frozen model/source/feature hashes differ'
    review=json.loads(Path('reports/b_model_review/validation.json').read_text())
    assert review['passed'] and review['v1_frozen_hashes_unchanged']
    assert Path('reports/DSA5208_Project2_Report.pdf').is_file()
    names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z']).decode().split('\0')
    allowed={'.gitignore','.gitattributes','README.md','report.pdf','requirements.txt','requirements-report.txt'}
    folders={'configs','docs','reports','scripts','src','tests'}
    paths={Path(n) for n in names if n and (n in allowed or Path(n).parts[0] in folders
              or n=='data/manifest.csv' or n.startswith('data/samples/'))}
    for root in ['models/final/c_available_v1','data/features/c_available_v1','data/predictions/c_available_v1']:
        paths.update(p for p in Path(root).rglob('*') if p.is_file() and not p.name.startswith('.'))
    paths=sorted(p for p in paths if p.is_file() and '__pycache__' not in p.parts)
    assert paths and all(not p.is_symlink() for p in paths)
    manifest={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    metadata={'git_base':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
              'content_identified_by_file_sha256':True,'canvas_uploaded':False,
              'models':'original c_available_v1 models with current v2 source interface',
              'evaluation':'v1 frozen; v2 separately labelled post-hoc on already-opened 2024 sample',
              'omitted_reproducible_data':['raw annual CSVs','full clean history'],
              'files':manifest}
    out=Path('output/submission');out.mkdir(parents=True,exist_ok=True)
    archive=out/'DSA5208_Project2_submission.zip'
    with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for path in paths:z.write(path,str(path))
        z.writestr('SUBMISSION_MANIFEST.json',json.dumps(metadata,indent=2)+'\n')
        z.writestr('SUBMISSION_README.txt',
            'DSA5208 Project 2 submission\n'
            'Report: reports/DSA5208_Project2_Report.pdf\n'
            'Source and run instructions: README.md, src/, scripts/, configs/, requirements*.txt\n'
            'Trained models + calibrators: models/final/c_available_v1/\n'
            'Quick model demo without raw data: python -m scripts.demo_predict_from_features S06 "2024-11-07 20:50:00" 1\n'
            'Raw/clean data are omitted because of size; README steps 2-3 recreate them.\n'
            'Do not rerun training into the frozen artifact directories.\n'
            'Verify files using SUBMISSION_MANIFEST.json SHA-256 values.\n')
    with zipfile.ZipFile(archive) as z:
        assert z.testzip() is None
        for name,digest in manifest.items():assert hashlib.sha256(z.read(name)).hexdigest()==digest,name
        assert any('/stages/' in n for n in z.namelist())
    checksum=hashlib.sha256(archive.read_bytes()).hexdigest()
    (out/(archive.name+'.sha256')).write_text(f'{checksum}  {archive.name}\n')
    (out/'package_validation.json').write_text(json.dumps({'passed':True,'members':len(paths)+2,
        'bytes':archive.stat().st_size,'sha256':checksum,'all_member_hashes_verified':True,'canvas_uploaded':False},indent=2)+'\n')
    print(archive,archive.stat().st_size,'bytes',checksum)

if __name__=='__main__':main()
