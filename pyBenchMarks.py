import os
import time
import subprocess
import statistics
import glob

REPEATS = 5
TEST_DATA_DIR = "test_data"
GO_BINARY = "./describe"

def build_go():
    print("🔧 Building Go binary...")
    subprocess.run(["go", "build", "-o", GO_BINARY], check=True)

def benchmark_command(cmd: list, repeats: int = REPEATS) -> list:
    times = []
    for _ in range(repeats):
        start = time.time()
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        times.append(time.time() - start)
    return times

def print_summary(name: str, times: list):
    print(f"{name}:")
    print(f"  Min   : {min(times):.4f} s")
    print(f"  Max   : {max(times):.4f} s")
    print(f"  Mean  : {statistics.mean(times):.4f} s")
    print(f"  Median: {statistics.median(times):.4f} s")
    print()

def benchmark_dataset(csv_file: str):
    print(f"📊 Benchmarking dataset: {csv_file}")

    # Go benchmark
    go_env = os.environ.copy()
    go_env["CSV_FILE"] = csv_file  # assuming your Go code reads from config.FilePath
    go_times = benchmark_command([GO_BINARY], REPEATS)

    # Pandas benchmark
    pandas_script = f"""
import pandas as pd
pd.read_csv("{csv_file}").describe(include="all")
"""
    pandas_times = benchmark_command(["python3", "-c", pandas_script], REPEATS)

    print_summary("Go describe", go_times)
    print_summary("Pandas describe", pandas_times)

def main():
    build_go()
    print()

    csv_files = glob.glob(os.path.join(TEST_DATA_DIR, "*.csv"))
    if not csv_files:
        print("No CSV files found in test_data/")
        return

    for csv_file in csv_files:
        benchmark_dataset(csv_file)

if __name__ == "__main__":
    main()