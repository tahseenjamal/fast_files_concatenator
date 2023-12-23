# Description: This script will read all the in a specified directory and combine them into a single file.
# The script will use multiple threads to speed up the process.
# Usage: python merge_to_df.py <output_file_name> <glob_descriptor> <num_threads>
# Example: python merge_to_df.py speedtest.csv 'speedtest*.csv' 4

import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from glob import glob

def read_csv(file):
    return pd.read_csv(file)

def main(file_name, glob_descriptor, num_threads):

    files = glob(glob_descriptor)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:

        dfs = list(executor.map(read_csv, files))

    df = pd.concat(dfs, ignore_index=True)

    df.to_csv(file_name, index=None)

if __name__ == '__main__':

    file_name = sys.argv[1]
    glob_descriptor = sys.argv[2]
    if len(sys.argv) > 3:
        num_threads = int(sys.argv[3])
    else:
        num_threads = None

    print('Filename: ', file_name)
    print('Glob Descriptor: ', glob_descriptor)
    print('Number of Threads: ', num_threads)

    main(file_name, glob_descriptor, num_threads)

