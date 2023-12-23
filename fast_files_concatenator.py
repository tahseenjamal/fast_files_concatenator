# Description: This script will combine all the files in a directory into one file.
# Usage: python main.py <output_file_name> <glob_descriptor>
# Example: python fast_files_concatenator.py combined.txt "data/*.txt"

import sys
from glob import glob
from concurrent.futures import ThreadPoolExecutor

def process_file(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    return lines

def write_to_file(file_name, lines):
    with open(file_name, 'w') as f:
        f.writelines(lines)
        f.flush()

def main(file_name, glob_descriptor, num_of_threads):

    files = glob(glob_descriptor)
    print("Total files: ", len(files))

    # If num_of_threads value is not specified, then it would use all cores
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        files_lines_list = list(executor.map(process_file, files))

    # Flatten the list of lists
    # files_lines_list has the following structure: [[file1_lines], [file2_lines], ...]
    # combined has the following structure: [file1_lines, file2_lines, ...]
    combined = [line for lines_list in files_lines_list for line in lines_list] 
    write_to_file(file_name, combined)

if __name__ == "__main__":

    file_name = sys.argv[1]
    glob_descriptor = sys.argv[2]
    if len(sys.argv) >= 4:
        num_of_threads = int( sys.argv[3] )
    else:
        num_of_threads = None

    print("File name: ", file_name)
    print("Glob descriptor: ", glob_descriptor)
    print("Threads: ", num_of_threads)

    main(file_name, glob_descriptor, num_of_threads)


