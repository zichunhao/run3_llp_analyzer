set -xe;

PATH_ANALYZER="${CMSSW_BASE}/src/run3_llp_analyzer"

for muon in "0" "1"; do
    python3 "${PATH_ANALYZER}/scripts_condor/merge_jobs_error_check.py" \
    --dirs \
    "${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023B_v1" \
    "${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023C_v1" \
    "${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023C_v2" \
    "${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023C_v3" \
    "${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023C_v4" \
    "${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023D_v1" \
    "${PATH_ANALYZER}/condor_jobs/merging_Muon${muon}_Run2023D_v2" \
    --rerun;
done