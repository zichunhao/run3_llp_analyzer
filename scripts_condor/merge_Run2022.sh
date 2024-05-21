CURR_DIR=$(pwd);

PATH_ANALYZER="${CMSSW_BASE}/src/run3_llp_analyzer"
PATH_EXE="${PATH_ANALYZER}/MergeNtuples"

N_NTUPLER=50;
N_NANOAOD=20;
MEMORY=2048;
SINGULARITY_IMAGE="EL8";

for era in "E" "F" "G"; do
    path_ntupler="${PATH_ANALYZER}/lists/displacedJetMuonNtuple/V1p19/Data2022/DisplacedJet-EXOCSCCluster_Run2022${era}-PromptReco-v1.txt"
    path_NanoAOD="${PATH_ANALYZER}/lists/nanoAOD/2022/DisplacedJet-Run2022${era}-22Sep2023-v1.txt"
    dir_output="/eos/uscms/store/group/lpclonglived/MergedNtuples_Run3/batch_Run2022${era}"
    dir_scripts="${PATH_ANALYZER}/condor_jobs/merging_Run2022${era}"

    # remove the old job script directory
    # rm -rf ${dir_scripts};

    python3 ${PATH_ANALYZER}/scripts_condor/create_merge_scripts.py \
    --exe ${PATH_EXE} \
    --ntupler ${path_ntupler} \
    --n-ntupler ${N_NTUPLER} \
    --NanoAOD ${path_NanoAOD} \
    --n-NanoAOD ${N_NANOAOD} \
    --output ${dir_output} \
    --scripts ${dir_scripts} \
    --memory ${MEMORY} \
    --verbose;

    # submit the job
    cd ${dir_scripts};
    # condor_submit runjob.jdl;
done

cd ${CURR_DIR};