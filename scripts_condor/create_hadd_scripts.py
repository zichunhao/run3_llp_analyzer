from pathlib import Path
from typing import List
import argparse
import os

HOME = os.getenv("HOME")

CMSSW_BASE = os.getenv("CMSSW_BASE")
CMSSW_RELEASE = CMSSW_BASE.split("/")[-1]
SCRAM_ARCH = os.getenv("SCRAM_ARCH")

def parse_args():
    arg_parser = argparse.ArgumentParser(
        description="Hadd input ntupler merged files into a single file"
    )

    arg_parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Directory of the ntupler files to hadd",
    )
    arg_parser.add_argument(
        "--pattern", type=str, default="*.root", help="File pattern of the input files"
    )
    arg_parser.add_argument(
        "--output", type=str, required=True, help="Path to the output root file"
    )
    arg_parser.add_argument(
        "--scripts", type=str, required=True, help="Directory of the job scripts"
    )
    arg_parser.add_argument(
        "--verbose",
        action="store_true",
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

    path_output = Path(args.output).resolve()
    dir_input = Path(args.input).resolve()
    dir_scripts = Path(args.scripts).resolve()
    path_output.parent.mkdir(parents=True, exist_ok=True)
    dir_scripts.mkdir(parents=True, exist_ok=True)

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
cd {CMSSW_RELEASE}/src
eval `scramv1 runtime -sh` # cmsenv
echo "Untar JEC:" 
echo "After Untar: "
# jobs
"""
    file_paths = list(dir_input.glob(args.pattern))
    str_file_paths = [
        str(f).replace("/eos/uscms/", "root://cmseos.fnal.gov//") for f in file_paths
    ]
    logging.debug(f"There are {len(str_file_paths)} root files with pattern '{args.pattern}' in {dir_input}")
    str_file_paths = " ".join(str_file_paths)
    str_dir_output = str(path_output).replace("/eos/uscms/", "root://cmseos.fnal.gov//")
    script_sh += f"hadd -f {str_dir_output} {str_file_paths}\n"
    script_sh = script_sh.strip()

    # write to file
    path_script_sh = dir_scripts / "runjob.sh"
    path_script_sh.write_text(script_sh)
    os.system(f"chmod +x {path_script_sh}")

    script_jdl = """
universe = vanilla
Executable = runjob.sh
Should_Transfer_Files = YES
WhenToTransferOutput = ON_EXIT_OR_EVICT
+JobQueue = "Normal"
RequestCpus = 1
RequestDisk = 4
+RunAsOwner = True
+InteractiveUser = true
+SingularityImage = "/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7"
+SingularityBindCVMFS = true
run_as_owner = true
Output = hadd-$(Cluster)_$(Process).stdout
Error  = hadd-$(Cluster)_$(Process).stderr
Log    = hadd-$(Cluster)_$(Process).log
Queue 1
"""
    script_jdl = script_jdl.strip()

    path_script_jdl = dir_scripts / "runjob.jdl"
    path_script_jdl.write_text(script_jdl)

    logging.info(f"Scripts written to {dir_scripts}")
