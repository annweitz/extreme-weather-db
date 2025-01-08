import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import processing as pr

"""
Main function to load and process data while iterating over all files in the raw database directory.
Executed in parallel by 10 workers.
"""

def main():
    for year in range(1990, 2023):
        for month in range(1, 13):
            print(f"Processing data for {year}-{month}")
            threadpool = ThreadPoolExecutor(max_workers=10)
            for variable in ["temperature", "wind", "precipitation"]:
                data = pr.load_data(variable, year, month)
                if data is not None:
                    threadpool.submit(pr.process_data(data))
                    threadpool.shutdown()
            print("All data processed.")

if __name__ == "__main__":
    main()