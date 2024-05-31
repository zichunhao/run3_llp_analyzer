if [ -z $CMSSW_BASE ]; then
    echo "CMSSW_BASE is not set. Exiting.";
    exit 1;
fi

set -xe;

CURR_DIR=$(pwd);

DIR_EOS="/eos/uscms/store/group/lpclonglived/MergedNtuples_Run3"
DIR_ANALYZER="${CMSSW_BASE}/src/run3_llp_analyzer"

for muon in "0" "1"; do
    # 2023B
    era="B";
    for v in "1"; do
        DIR_SCRIPTS="${DIR_ANALYZER}/condor_jobs/merged_hadd_Muon${muon}_Run2023${era}_v${v}"
        rm -rf ${DIR_SCRIPTS}/*;
        mkdir -p ${DIR_SCRIPTS};

        python3 ${DIR_ANALYZER}/scripts_condor/create_hadd_scripts.py \
        --input ${DIR_EOS}/batch_Muon${muon}_Run2023${era}_v${v} \
        --pattern "*.root" \
        --output "${DIR_EOS}/merged/merged_Muon${muon}_Run2023${era}_v${v}.root" \
        --scripts "${DIR_SCRIPTS}" \
        --verbose;

        # submit the job
        cd ${DIR_SCRIPTS};
        condor_submit runjob.jdl;
    done;

    

    # 2023C
    era="C";
    for v in "1" "2" "3" "4"; do
        DIR_SCRIPTS="${DIR_ANALYZER}/condor_jobs/merged_hadd_Muon${muon}_Run2023${era}_v${v}"
        rm -rf ${DIR_SCRIPTS}/*;
        mkdir -p ${DIR_SCRIPTS};

        python3 ${DIR_ANALYZER}/scripts_condor/create_hadd_scripts.py \
        --input ${DIR_EOS}/batch_Muon${muon}_Run2023${era}_v${v} \
        --pattern "*.root" \
        --output "${DIR_EOS}/merged/merged_Muon${muon}_Run2023${era}_v${v}.root" \
        --scripts "${DIR_SCRIPTS}" \
        --verbose;

        # submit the job
        cd ${DIR_SCRIPTS};
        condor_submit runjob.jdl;
    done;

    # 2023D
    era="D";
    for v in "1" "2"; do
        DIR_SCRIPTS="${DIR_ANALYZER}/condor_jobs/merged_hadd_Muon${muon}_Run2023${era}_v${v}"
        rm -rf ${DIR_SCRIPTS}/*;
        mkdir -p ${DIR_SCRIPTS};

        python3 ${DIR_ANALYZER}/scripts_condor/create_hadd_scripts.py \
        --input ${DIR_EOS}/batch_Muon${muon}_Run2023${era}_v${v} \
        --pattern "*.root" \
        --output "${DIR_EOS}/merged/merged_Muon${muon}_Run2023${era}_v${v}.root" \
        --scripts "${DIR_SCRIPTS}" \
        --verbose;

        # submit the job
        cd ${DIR_SCRIPTS};
        condor_submit runjob.jdl;
    done;
done;

cd ${CURR_DIR};


