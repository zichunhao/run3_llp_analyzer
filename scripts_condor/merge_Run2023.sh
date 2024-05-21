set -xe;

CURR_DIR=$(pwd);

PATH_ANALYZER="${CMSSW_BASE}/src/run3_llp_analyzer"
PATH_EXE="${PATH_ANALYZER}/MergeNtuples"

N_NTUPLER=50;
N_NANOAOD=20;
MEMORY=2048;

for muon in "0" "1"; do
    # 2023B
    era="B";
    for v in "1"; do
        PATH_NTUPLER="${PATH_ANALYZER}/lists/displacedJetMuonNtuple/V1p19/Data2023/Muon${muon}-EXOCSCCluster_Run2023${era}-PromptReco-v${v}.txt"
        PATH_NANOAOD="${PATH_ANALYZER}/lists/nanoAOD/2023/Muon${muon}-Run2023${era}-22Sep2023-v${v}.txt"
        DIR_OUTPUT="/eos/uscms/store/group/lpclonglived/MergedNtuples_Run3/batch_Muon${muon}_Run2023${era}_v${v}"
        DIR_SCRIPTS="${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023${era}_v${v}"

        # remove the old job script directory
        rm -rf ${DIR_SCRIPTS};

        python3 ${PATH_ANALYZER}/scripts_condor/create_merge_scripts.py \
        --exe ${PATH_EXE} \
        --ntupler ${PATH_NTUPLER} \
        --n-ntupler ${N_NTUPLER} \
        --NanoAOD ${PATH_NANOAOD} \
        --n-NanoAOD ${N_NANOAOD} \
        --output ${DIR_OUTPUT} \
        --scripts ${DIR_SCRIPTS} \
        --memory ${MEMORY} \
        --verbose;

        # submit the jobs
        cd ${DIR_SCRIPTS};
        condor_submit runjob.jdl;
    done;

    

    # 2023C
    era="C";
    for v in "1" "2" "3" "4"; do
        PATH_NTUPLER="${PATH_ANALYZER}/lists/displacedJetMuonNtuple/V1p19/Data2023/Muon${muon}-EXOCSCCluster_Run2023${era}-PromptReco-v${v}.txt"
        if [ "${muon}" == "1" ] && [ "${v}" == "4" ]; then
            # Muon1-Run2023C-22Sep2023_v4-v2.txt
            PATH_NANOAOD="${PATH_ANALYZER}/lists/nanoAOD/2023/Muon1-Run2023C-22Sep2023_v4-v2.txt"
        else 
            PATH_NANOAOD="${PATH_ANALYZER}/lists/nanoAOD/2023/Muon${muon}-Run2023${era}-22Sep2023_v${v}-v1.txt"
        fi
        DIR_OUTPUT="/eos/uscms/store/group/lpclonglived/MergedNtuples_Run3/batch_Muon${muon}_Run2023${era}_v${v}"
        DIR_SCRIPTS="${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023${era}_v${v}"

        # remove the old job script directory
        rm -rf ${DIR_SCRIPTS};

        python3 ${PATH_ANALYZER}/scripts_condor/create_merge_scripts.py \
        --exe ${PATH_EXE} \
        --ntupler ${PATH_NTUPLER} \
        --n-ntupler ${N_NTUPLER} \
        --NanoAOD ${PATH_NANOAOD} \
        --n-NanoAOD ${N_NANOAOD} \
        --output ${DIR_OUTPUT} \
        --scripts ${DIR_SCRIPTS} \
        --memory ${MEMORY} \
        --verbose;

        # submit the jobs
        cd ${DIR_SCRIPTS};
        condor_submit runjob.jdl;
    done;

    # 2023D
    era="D";
    for v in "1" "2"; do
        PATH_NTUPLER="${PATH_ANALYZER}/lists/displacedJetMuonNtuple/V1p19/Data2023/Muon${muon}-EXOCSCCluster_Run2023${era}-PromptReco-v${v}.txt"
        PATH_NANOAOD="${PATH_ANALYZER}/lists/nanoAOD/2023/Muon${muon}-Run2023${era}-22Sep2023_v${v}-v1.txt"
        DIR_OUTPUT="/eos/uscms/store/group/lpclonglived/MergedNtuples_Run3/batch_Muon${muon}_Run2023${era}_v${v}"
        DIR_SCRIPTS="${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023${era}_v${v}"

        # remove the old job script directory
        rm -rf ${DIR_SCRIPTS};

        python3 ${PATH_ANALYZER}/scripts_condor/create_merge_scripts.py \
        --exe ${PATH_EXE} \
        --ntupler ${PATH_NTUPLER} \
        --n-ntupler ${N_NTUPLER} \
        --NanoAOD ${PATH_NANOAOD} \
        --n-NanoAOD ${N_NANOAOD} \
        --output ${DIR_OUTPUT} \
        --scripts ${DIR_SCRIPTS} \
        --memory ${MEMORY} \
        --verbose;

        # submit the jobs
        cd ${DIR_SCRIPTS};
        condor_submit runjob.jdl;
    done;
done;

cd ${CURR_DIR};