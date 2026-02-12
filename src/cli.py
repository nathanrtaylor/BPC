import argparse
from src.runner import run_all_jobs

def main():
    p = argparse.ArgumentParser()
    p.add_argument("config", help="Path to YAML config (e.g., configs/jobs_new_format.yml)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    run_all_jobs(args.config, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
