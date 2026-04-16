"""
run_pipeline.py — Run all notebooks sequentially.

Usage:
    python run_pipeline.py [--skip-data] [--skip-train]

    --skip-data   Skip notebook 01 (data already seeded)
    --skip-train  Skip notebook 03 (model already trained)
"""

import sys
import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent
NOTEBOOKS = [
    ("01_create_dummy_data.py", "Seeding bronze Delta tables"),
    ("02_medallion_pipeline.py", "Running medallion pipeline (Bronze→Silver→Gold)"),
    ("03_train_model.py", "Training safety stock model"),
    ("04_batch_scoring.py", "Batch scoring + writing recommendations"),
]


def run_notebook(script: str, description: str) -> bool:
    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"{'='*60}")
    result = subprocess.run(
        [sys.executable, str(ROOT / "notebooks" / script)],
        cwd=str(ROOT),
    )
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-data", action="store_true", help="Skip notebook 01")
    parser.add_argument("--skip-train", action="store_true", help="Skip notebook 03")
    args = parser.parse_args()

    skip = set()
    if args.skip_data:
        skip.add("01_create_dummy_data.py")
    if args.skip_train:
        skip.add("03_train_model.py")

    print("\n🏭 Generac Safety Stock — Full Pipeline\n")

    for script, desc in NOTEBOOKS:
        if script in skip:
            print(f"\n  [skipped] {desc}")
            continue
        ok = run_notebook(script, desc)
        if not ok:
            print(f"\n  ✗ Pipeline failed at: {script}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print("  ✓ Pipeline complete!")
    print("  Launch app:  streamlit run app/streamlit_app.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
