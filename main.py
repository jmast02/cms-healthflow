"""
CMS HealthFlow — Pipeline CLI entry point.

Run individual steps or the full pipeline from one place.

Usage:
    python main.py download
    python main.py ingest
    python main.py pipeline          # normalize → quality → aggregate → rank
    python main.py api               # start FastAPI dev server
    python main.py --help
"""

import argparse
import logging
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cms-healthflow")


def run(cmd: list[str]) -> None:
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        log.error("Step failed with exit code %d", result.returncode)
        sys.exit(result.returncode)


STEPS = {
    "download":        [sys.executable, "-m", "ingestion.download"],
    "ingest":          [sys.executable, "-m", "ingestion.ingest"],
    "validate":        [sys.executable, "-m", "ingestion.validate"],
    "normalize":       [sys.executable, "-m", "spark.jobs.normalize"],
    "quality":         [sys.executable, "-m", "spark.jobs.quality"],
    "aggregate":       [sys.executable, "-m", "spark.jobs.aggregate"],
    "rank":            [sys.executable, "-m", "spark.jobs.rankings"],
    "hospitals":       [sys.executable, "-m", "spark.jobs.hospitals"],
    "api":             ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"],
}

PIPELINE_STEPS = ["normalize", "quality", "aggregate", "rank"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CMS HealthFlow pipeline runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join([f"  {k}" for k in STEPS]) + "\n  pipeline  (normalize+quality+aggregate+rank)",
    )
    parser.add_argument(
        "step",
        choices=list(STEPS) + ["pipeline"],
        help="Pipeline step to run",
    )
    args = parser.parse_args()

    if args.step == "pipeline":
        log.info("Running full Spark pipeline: %s", " → ".join(PIPELINE_STEPS))
        for step in PIPELINE_STEPS:
            run(STEPS[step])
        log.info("Pipeline complete.")
    else:
        run(STEPS[args.step])


if __name__ == "__main__":
    main()
