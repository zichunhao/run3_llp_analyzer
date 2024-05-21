from typing import List, Tuple
from tqdm import tqdm
from pathlib import Path
import argparse
import logging
logging.basicConfig(level=logging.INFO)


def get_bad_jobs(log_dir: Path) -> List[Tuple[int, int]]:
    paths_lists = list(log_dir.glob("*.stderr"))
    bad_jobs = []
    is_bad_job_dict = {}

    for log_file_path in tqdm(paths_lists):
        # merge--idx_ntupler_idx_nanoaod.stderr
        idx_ntupler, idx_nanoaod = (
            log_file_path.name.split(".")[0].split("-")[-1].split("_")
        )
        idx_ntupler = int(idx_ntupler)
        idx_nanoaod = int(idx_nanoaod)
        
        is_bad_job = False

        # get the info
        with open(log_file_path, "r") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                if "error" in line.lower() and ("error in <t" not in line.lower()):
                    is_bad_job = True
        
        job_key = (idx_ntupler, idx_nanoaod)
        if job_key not in is_bad_job_dict:
            # first encounter
            is_bad_job_dict[job_key] = is_bad_job
            if is_bad_job:
                bad_jobs.append(job_key)
        else:
            # already encountered
            if is_bad_job and is_bad_job_dict[job_key]:
                # bad job again -> need to rerun
                pass
            elif is_bad_job and not is_bad_job_dict[job_key]:
                # new run already okay -> skip
                pass
            if not is_bad_job and is_bad_job_dict[job_key]:
                # okay this time -> update bad_jobs list
                is_bad_job_dict[job_key] = False
                bad_jobs.remove(job_key)
            elif not is_bad_job and not is_bad_job_dict[job_key]:
                # okay this time and before -> skip
                pass

    return bad_jobs


def rerun_jobs(job_dir: Path, bad_jobs: List[Tuple[int, int]]) -> None:
    # read the runjob.jdl file
    path_jdl = job_dir / "runjob.jdl"
    with open(path_jdl, 'r') as file:
        lines = file.readlines()
        
    queue_line_index = None
    for i, line in enumerate(lines):
        if "Queue $(N)" in line or "Queue I J from" in line:
            queue_line_index = i
            break
    if queue_line_index is None:
        raise RuntimeError("No line with 'Queue'")

    # Update the last line with the new Queue block
    lines[queue_line_index] = 'Queue I J from (\n'
    for job in bad_jobs:
        lines[queue_line_index] += f'{job[0]} {job[1]}\n'
    lines[queue_line_index] += ')\n'

    # Find the highest level of runjob_rerun file
    n = 0
    while True:
        rerun_jdl_path = job_dir / f'runjob_rerun{n}.jdl'
        if not rerun_jdl_path.exists():
            break
        n += 1

    # Write the updated contents to the next level of runjob_rerun file
    output_jdl_path = job_dir / f'runjob_rerun{n}.jdl'
    with open(output_jdl_path, 'w') as file:
        file.writelines(lines)
        
    return output_jdl_path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Get the list of bad jobs from the stderr files in a job directory"
    )
    # get the list of directories to check
    parser.add_argument(
        "--dirs", required=True, type=str, nargs="+", help="List of directories to check"
    )
    parser.add_argument(
        "--rerun",
        action="store_true", 
        help="Whether to create scripts for rerun"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    for dir in args.dirs:
        job_dir = Path(dir)
        if not job_dir.exists():
            logging.error(f"Directory {job_dir} does not exist")
            continue
        log_dir = job_dir.resolve() / "logs"
        bad_jobs = get_bad_jobs(log_dir)
        if len(bad_jobs) == 0:
            logging.info(f"No bad jobs found in {log_dir}")
        else:
            logging.info(f"Bad jobs found in {log_dir}: {bad_jobs}")
            if args.rerun:
                output_jdl_path = rerun_jobs(job_dir=job_dir, bad_jobs=bad_jobs)
                logging.info(f"Created new jobs at {output_jdl_path}")
            