set -xe;

PATH_ANALYZER="${CMSSW_BASE}/src/run3_llp_analyzer"

python3 "${PATH_ANALYZER}/scripts_condor/merge_jobs_error_check.py" \
--dirs \
"${PATH_ANALYZER}/condor_jobs/merging_Run2022E" \
"${PATH_ANALYZER}/condor_jobs/merging_Run2022F" \
"${PATH_ANALYZER}/condor_jobs/merging_Run2022G" \
--rerun;
