import os
import glob
import argparse
from pathlib import Path

def copy(fpath: Path, copy_dir: Path):
    with open(fpath,"r") as f:
        for line in f:
            path_source = line.strip()
            dir_target = str(copy_dir)
            cmd = f"xrdcp -f {path_source} {dir_target}"
            print(cmd)
            os.system(cmd)
    

def makelist(parent_dir: Path, list_name: str):
    flist = list(parent_dir.glob("*.root"))
    fout = f"{list_name}.txt"
    with open(fout,"w") as f:
        for path in flist:
            f.write(str(path) + "\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-list', required=True, help="Path to input files list")
    parser.add_argument('--copy-dir', default=".", help="Directory that contains the copied root files")
    args = parser.parse_args()
    
    path_input_list = Path(args.input_list).resolve()
    copy_dir = Path(args.copy_dir).resolve()
    copy_dir.mkdir(parents=True, exist_ok=True)
    
    copy(fpath=path_input_list, copy_dir=copy_dir)
    makelist(parent_dir=copy_dir, list_name=copy_dir)
