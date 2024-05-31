if [ -z $CMSSW_BASE ]; then
    echo "CMSSW_BASE is not set. Exiting.";
    exit 1;
fi

set -xe;

CURR_DIR=$(pwd);

DIR_EOS="/eos/uscms/store/group/lpclonglived/MergedNtuples_Run3"
DIR_ANALYZER="${CMSSW_BASE}/src/run3_llp_analyzer"

for era in "E" "F" "G"; do

    DIR_SCRIPTS="${DIR_ANALYZER}/condor_jobs/skimmed_hadd_Run2022${era}"
    rm -rf ${DIR_SCRIPTS}/*;
    mkdir -p ${DIR_SCRIPTS};

    python3 ${DIR_ANALYZER}/scripts_condor/create_hadd_from_list_scripts.py \
    --inputs "${DIR_ANALYZER}/lists/displacedJetMuonNtuple/V1p19/Data2022/DisplacedJet-EXOCSCCluster_Run2022${era}-PromptReco-v1.txt" \
    --output "${DIR_EOS}/skimmed/skimmed_Run2022${era}.root" \
    --scripts "${DIR_SCRIPTS}" \
    --verbose;

    # submit the job
    cd ${DIR_SCRIPTS};
    condor_submit runjob.jdl;
done

cd ${CURR_DIR};


