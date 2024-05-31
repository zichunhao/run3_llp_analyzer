if [ -z $CMSSW_BASE ]; then
    echo "CMSSW_BASE is not set. Exiting.";
    exit 1;
fi

set -xe;

CURR_DIR=$(pwd);

DIR_EOS="/eos/uscms/store/group/lpclonglived/MergedNtuples_Run3"
DIR_ANALYZER="${CMSSW_BASE}/src/run3_llp_analyzer"

for era in "E" "F" "G"; do

    DIR_SCRIPTS="${DIR_ANALYZER}/condor_jobs/merged_hadd_Run2022${era}"
    rm -rf ${DIR_SCRIPTS}/*;
    mkdir -p ${DIR_SCRIPTS};

    python3 ${DIR_ANALYZER}/scripts_condor/create_hadd_scripts.py \
    --input ${DIR_EOS}/batch_Run2022${era} \
    --pattern "*.root" \
    --output "${DIR_EOS}/merged/merged_Run2022${era}.root" \
    --scripts "${DIR_SCRIPTS}" \
    --verbose;

    # submit the job
    cd ${DIR_SCRIPTS};
    condor_submit runjob.jdl;
done

cd ${CURR_DIR};


