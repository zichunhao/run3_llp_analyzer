from pathlib import Path
import argparse
import os
import logging

HOME = os.getenv("HOME")

CMSSW_BASE = os.getenv("CMSSW_BASE")
CMSSW_RELEASE = CMSSW_BASE.split("/")[-1]
SCRAM_ARCH = os.getenv("SCRAM_ARCH")

ANALYZER_DIR = Path(CMSSW_BASE).resolve() / "src/run3_llp_analyzer"
PATH_CONVERT_LIST = ANALYZER_DIR / "scripts/convertListMerge.py"

def replace_eos(p):
    return p.replace("/eos/uscms/", "root://cmseos.fnal.gov//")

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Generate a bash script.')
parser.add_argument('--inputs', nargs='+', required=True, help='List of input txt files.')
parser.add_argument('--output', type=str, required=True, help='Path to the output root file.')
parser.add_argument('--scripts', type=str, required=True, help='Path to the directory storing the script.')
parser.add_argument("--verbose", action="store_true")
args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

# Read the txt files and create a list of paths
paths = []
for input_file in args.inputs:
    logging.debug(f"Opening file {input_file}")
    with open(input_file, 'r') as file:
        paths.extend(file.read().splitlines())

logging.debug(f"{len(paths)} root files to hadd")

paths = map(replace_eos, paths)
output = replace_eos(args.output)
logging.info(f"hadd as {output}")

dir_scripts = Path(args.scripts)
dir_scripts.mkdir(parents=True, exist_ok=True)

# Generate the bash script
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
hadd -f {output} {' '.join(paths)}
"""

path_script_sh = dir_scripts / "runjob.sh"
path_script_sh.write_text(script_sh.strip())

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
path_script_jdl = dir_scripts / "runjob.jdl"
path_script_jdl.write_text(script_jdl.strip())

logging.info(f"Script generated successfully at {dir_scripts}")