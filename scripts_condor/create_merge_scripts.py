import argparse
import os
from pathlib import Path
from typing import List


HOME = os.getenv("HOME")

CMSSW_BASE = os.getenv("CMSSW_BASE")
CMSSW_RELEASE = CMSSW_BASE.split("/")[-1]
SCRAM_ARCH = os.getenv("SCRAM_ARCH")

ANALYZER_DIR = Path(CMSSW_BASE).resolve() / "src/run3_llp_analyzer"
PATH_CONVERT_LIST = ANALYZER_DIR / "scripts/convertListMerge.py"


def batch_file_list(n: int, file_dir: Path, replace_eos: bool = True) -> List[List[str]]:
    """Split a txt file that contains many ntupler/NanoAOD file paths,
    one per line, into n groups.
    Note that some elements in the end might be empty.

    :param n: Number of groups to split the files into.
    :type n: int
    :param file_dir: Directory to the txt file to split.
    :type file_dir: Path
    :return: List of n lists of str path to txt file.
    :rtype: List[List[str]]
    """
    with open(file_dir, "r") as f:
        input_files = f.readlines()

    if n >= len(input_files):
        n = len(input_files)
        n_files_per_job = 1
    else:
        n_files = len(input_files)
        n_files_per_job = n_files // n
        if n_files % n != 0:
            # leftover files
            n_files_per_job += 1

    batch_list = []
    for i in range(n):
        start = i * n_files_per_job
        end = (i + 1) * n_files_per_job
        input_files_group = input_files[start:end]
        # remove newline character
        input_files_group = [file.strip() for file in input_files_group]
        batch_list.append(input_files_group)

    # remove empty lists
    final_batch_list = []
    for batch in batch_list:
        if len(batch) == 0:
            break
        else:
            if replace_eos:
                batch = map(lambda p: p.replace("/eos/uscms/", "root://cmseos.fnal.gov//"), batch)
                batch = list(batch)
            final_batch_list.append(batch)

    return final_batch_list


def parse_args():
    arg_parser = argparse.ArgumentParser(
        description="Split input ntupler and NanoAOD files for LPC merging job"
    )
    arg_parser.add_argument(
        "--exe", type=str, required=True, help="Path to the executable"
    )
    arg_parser.add_argument(
        "--ntupler", type=str, required=True, help="Path to the ntupler files list"
    )
    arg_parser.add_argument(
        "--n-ntupler",
        type=int,
        required=True,
        help="Number of groups to split the input ntupler files into",
    )
    arg_parser.add_argument(
        "--NanoAOD", type=str, required=True, help="Path to the NanoAOD files list"
    )
    arg_parser.add_argument(
        "--n-NanoAOD",
        type=int,
        required=True,
        help="Number of groups to split the input NanoAOD files into",
    )
    arg_parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to the resulting merged root file",
    )

    arg_parser.add_argument(
        "--scripts", type=str, required=True, help="Directory of the job scripts"
    )
    arg_parser.add_argument(
        "--memory", type=int, default=2048, help="Memory to request"
    )

    arg_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Whether to set the logging level to DEBUG"
    )

    args = arg_parser.parse_args()
    return args


if __name__ == "__main__":
    import logging

    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logging.debug(f"CMSSW: {CMSSW_RELEASE}")
    logging.debug(f"SCRAM_ARCH: {SCRAM_ARCH}")
    logging.debug(f"args: {args}")

    path_ntupler = Path(args.ntupler).resolve()
    n_ntupler = args.n_ntupler
    logging.debug(f"Path to the ntupler files list: {path_ntupler}")

    path_NanoAOD = Path(args.NanoAOD).resolve()
    n_NanoAOD = args.n_NanoAOD
    logging.debug(f"Path to the NanoAOD files list: {path_NanoAOD}")

    dir_output = Path(args.output).resolve()
    # make sure that parent folder exists
    dir_output.mkdir(parents=True, exist_ok=True)
    # batch jobs have no access to eos
    dir_output = str(dir_output).replace("/eos/uscms/", "root://cmseos.fnal.gov//")
    logging.debug(f"Path to the resulting merged file: {dir_output}")

    dir_scripts = Path(args.scripts)
    dir_scripts.mkdir(parents=True, exist_ok=True)

    batch_ntupler = batch_file_list(n_ntupler, path_ntupler, replace_eos=True)
    batch_NanoAOD = batch_file_list(n_NanoAOD, path_NanoAOD, replace_eos=True)
    # update to the true number of batches
    n_ntupler = len(batch_ntupler)
    n_NanoAOD = len(batch_NanoAOD)

    # batche ntuplers and NanoAODs
    for batch_list, batch_type in zip(
        (batch_ntupler, batch_NanoAOD),
        ("ntupler", "NanoAOD"),
    ):
        dir_batch_list = dir_scripts / f"batch_{batch_type}"
        dir_batch_list.mkdir(parents=True, exist_ok=True)
        for i, batch in enumerate(batch_list):
            output_file = dir_batch_list / f"batch_{batch_type}_{i}.txt"
            with open(output_file, "w") as f:
                for j, line in enumerate(batch):
                    if j != len(batch) - 1:
                        f.write(line + "\n")
                    else:
                        f.write(line)

    # write job script (runjob.sh)
    exe_name = Path(args.exe).name
    path_exe = Path(args.exe).resolve()  # absolute path

    script_sh = f"""
#!/bin/bash
date
MAINDIR=`pwd`
ls
voms-proxy-info --all
#CMSSW from scratch (only need for root)
export CWD=${{PWD}}
export PATH=${{PATH}}:/cvmfs/cms.cern.ch/common/
export SCRAM_ARCH={SCRAM_ARCH}
echo "PATH: $PATH"
echo "SCRAM_ARCH: $SCRAM_ARCH"
scramv1 project CMSSW {CMSSW_RELEASE}
echo "Inside $MAINDIR:"
ls -lah
cp {exe_name} {CMSSW_RELEASE}/src
cd {CMSSW_RELEASE}/src
eval `scramv1 runtime -sh` # cmsenv
echo "Untar JEC:" 
echo "After Untar: "
# jobs
# create lists and copy relevant files
python3 ${{MAINDIR}}/{PATH_CONVERT_LIST.name} --input-list ${{MAINDIR}}/batch_ntupler_${{1}}.txt --copy-dir ntupler
python3 ${{MAINDIR}}/{PATH_CONVERT_LIST.name} --input-list ${{MAINDIR}}/batch_NanoAOD_${{2}}.txt --copy-dir NanoAOD
echo "ls -lha"
ls -lha
echo "ls -lha ntupler"
ls -lha ntupler
echo "ls -lha NanoAOD"
ls -lha NanoAOD
echo "Running job..."
# start merging
./{exe_name} ntupler.txt NanoAOD.txt ntupler_${{1}}_NanoAOD_${{2}}.root ""
echo "Inside $MAINDIR:"
ls -lah
echo "coping to eos: +xrdcp -f ntupler_${{1}}_NanoAOD_${{2}}.root {dir_output}"
xrdcp -f ntupler_${{1}}_NanoAOD_${{2}}.root {dir_output}
echo "DELETING..."
rm -rf {CMSSW_RELEASE}
rm -rf *.pdf *.C core*
cd $MAINDIR
echo "remove output local file"
rm -rf *.root
ls
date
"""
    script_sh = script_sh.strip()
    path_sh = dir_scripts / "runjob.sh"
    with open(path_sh, "w") as f:
        f.write(script_sh)
    os.system(f"chmod +x {path_sh}")

    # create jdl file
    paths_batch_ntupler = [
        f"batch_ntupler/batch_ntupler_{i}.txt" for i in range(n_ntupler)
    ]
    paths_batch_NanoAOD = [
        f"batch_NanoAOD/batch_NanoAOD_{i}.txt" for i in range(n_NanoAOD)
    ]

    dir_logs = dir_scripts / "logs"
    dir_logs.mkdir(parents=True, exist_ok=True)

    script_jdl = f"""
universe = vanilla
request_memory = {args.memory}
Executable = runjob.sh
Should_Transfer_Files = YES
WhenToTransferOutput = ON_EXIT_OR_EVICT
Transfer_Input_Files = runjob.sh,{path_exe},{PATH_CONVERT_LIST},{",".join(paths_batch_ntupler)},{",".join(paths_batch_NanoAOD)}
+JobQueue = "Normal"
RequestCpus = 1
RequestDisk = 4
+RunAsOwner = True
+InteractiveUser = true
+SingularityImage = "/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7"
+SingularityBindCVMFS = true
run_as_owner = true
n1 = {n_ntupler}
n2 = {n_NanoAOD}
N = $(n1) * $(n2)
I = ($(Process) / $(n2))
J = ($(Process) % $(n2))
Output = logs/merge-$(Cluster)_$(Process)-$INT(I)_$INT(J).stdout
Error  = logs/merge-$(Cluster)_$(Process)-$INT(I)_$INT(J).stderr
Log    = logs/merge-$(Cluster)_$(Process)-$INT(I)_$INT(J).log
Arguments  = "$INT(I) $INT(J)"
Queue $(N)
"""
    script_jdl = script_jdl.strip()    
    with open(dir_scripts / "runjob.jdl", "w") as f:
        f.write(script_jdl)
    
    logging.info(
        f"Job scripts produced at {dir_scripts} with {n_ntupler} ntupler batches and {n_NanoAOD} NanoAOD batches"
    )
