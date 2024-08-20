import glob
import os
from optparse import OptionParser, OptionGroup

def exec_command(command, dry_run=False):
    print(command)
    if not dry_run:
        os.system(command)

def write_condor_submit_file(n_jobs, exe='runjob', files=[], dry_run=True):
    fname = f'{exe}.jdl'
    content = f"""universe = vanilla
Executable = {exe}.sh
Should_Transfer_Files = YES 
WhenToTransferOutput = ON_EXIT_OR_EVICT
Transfer_Input_Files = {exe}.sh,{','.join(files)}
Transfer_Output_Files = done.txt
+JobQueue = "Normal"
RequestCpus = 1
RequestDisk = 4
+RunAsOwner = True
+InteractiveUser = true
+SingularityImage = "/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7"
+SingularityBindCVMFS = true
run_as_owner = true
Output = {exe}.$(Process).$(Cluster).stdout
Error = {exe}.$(Process).$(Cluster).stdout
Log = {exe}.$(Process).$(Cluster).log
Arguments = $(Process) {n_jobs}
Queue {n_jobs}
"""
    with open(fname, 'w') as f:
        f.write(content)
    if not dry_run:
        os.system(f"condor_submit {fname}")

def create_input_tar(tar_output, paths, dry_run=False):
    print("Tarring local input files ... ")
    files = [f for path in paths for f in glob.glob(path)]
    for f in files:
        exec_command(f"cp {f} .")
    
    file_names = [os.path.split(f)[1] for f in files]
    tar_command = f"tar -czvf {tar_output} {' '.join(file_names)}"
    exec_command(tar_command, dry_run)

def write_bash_script(temp='runjob.sh', command='', cmssw="", eos_cp=""):
    content = f"""#!/bin/bash -xe
date
MAINDIR=`pwd`
ls
voms-proxy-info --all
export CWD=${{PWD}}
export PATH=${{PATH}}:/cvmfs/cms.cern.ch/common
export SCRAM_ARCH=slc7_amd64_gcc700
scramv1 project CMSSW {cmssw}
mv {command.split()[0]} {cmssw}/src
cd {cmssw}/src
eval `scramv1 runtime -sh`
echo "MAINDIR=${{MAINDIR}}"
echo "{{`ls ${{MAINDIR}}`}}"
python ${{MAINDIR}}/convertList.py -i ${{MAINDIR}}/tmp_input_list_$1.txt
echo "{{`ls ${{MAINDIR}}`}}"
{command}
echo "{{`ls ${{MAINDIR}}`}}"
"""
    if eos_cp:
        content += f"""echo "copying to eos: {eos_cp}"
{eos_cp}
"""
    content += f"""echo "Inside ${{MAINDIR}}:"
ls
echo "DELETING..."
rm -rf {cmssw}
rm -rf *.pdf *.C core*
"""
    if eos_cp:
        content += """cd ${MAINDIR}
echo "remove output local file"
rm -rf *.root
"""
    content += """ls
date
touch ${MAINDIR}/done.txt"""

    with open(temp, 'w') as f:
        f.write(content)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('--clean', action='store_true', default=False, help='clean submission files')
    parser.add_option('--dryRun', action='store_true', default=False, help='write submission files only')
    parser.add_option('-o', '--odir', default='./', help='directory to write histograms/job output')
    parser.add_option('-i', '--inputList', default='./lists/test.txt', help='txt file for list of input')
    parser.add_option('--njobs', type="int", default=50, help='Number of jobs to split into')
    parser.add_option('--exe', default="Runllp_hnl_analyzer", help='Executable name to run')

    script_group = OptionGroup(parser, "script options")
    script_group.add_option("-l", dest="optionLabel", default="Razor2018_17SeptEarlyReReco", help="optionLabel for JEC")
    script_group.add_option("-m", dest="MuonHit", default="HeavyNeutralLepton_Tree.root", help="muon hit merged")
    parser.add_option_group(script_group)

    options, args = parser.parse_args()

    # Setup paths and files
    outpath = options.odir
    exe = options.exe
    eos_outpath = f'/eos/uscms/store/user/amalbert/MDSTriggerEff/{outpath}'
    eos_cp_path = f'root://cmseos.fnal.gov//store/user/amalbert/MDSTriggerEff/{outpath}'

    transfer_files = [
        f"{os.getcwd()}/{exe}",
        f"{os.getcwd()}/convertList.py",
    ]

    # Create output directories
    exec_command(f"mkdir -p {outpath}", False)
    exec_command(f"mkdir -p {eos_outpath}", False)
    os.chdir(outpath)

    # Split input list
    with open(options.inputList, 'r') as f:
        all_inputs = f.readlines()
    
    n_files_per_job = len(all_inputs) // options.njobs
    remainder = len(all_inputs) % options.njobs

    for i in range(options.njobs):
        start = i * n_files_per_job + min(i, remainder)
        end = start + n_files_per_job + (1 if i < remainder else 0)
        tmp_inputs = all_inputs[start:end]
        
        tmp_file = f"tmp_input_list_{i}.txt"
        transfer_files.append(tmp_file)
        with open(tmp_file, 'w') as fout:
            fout.writelines(tmp_inputs)

    # Prepare command and EOS copy command
    cut_string = "(cscRechitClusterNRechitChamberPlus11+cscRechitClusterNRechitChamberPlus12+cscRechitClusterNRechitChamberMinus11+cscRechitClusterNRechitChamberMinus12)==0 && (nCscRechitClusters>=1)"
    command = f'{exe} local_list.txt ${{MAINDIR}} "skim" "{cut_string}"'
    eos_cp = f'xrdcp -f ${{MAINDIR}}/displacedJetMuon_ntupler_*_skim.root {eos_cp_path}'

    # Write bash script and condor submit file
    write_bash_script("runjob.sh", command, "CMSSW_10_6_20", eos_cp)
    write_condor_submit_file(options.njobs, "runjob", transfer_files, options.dryRun)
