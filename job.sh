#!/bin/bash -l
#SBATCH --job-name=process-extreme-events  # Job name
#SBATCH --output=output.log       # Standard output file
#SBATCH --error=error.log         # Standard error file
#SBATCH --mem=300GB                 # Memory per node
#SBATCH --time=120:00:00           # Maximum runtime (hh:mm:ss)
#SBATCH --account=ag-schultz

module load tools/poetry
poetry install
poetry run python src/processing/processor.py





