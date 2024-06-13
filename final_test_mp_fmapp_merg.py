'''
this file is in case when server allocated space is too low.
'''

import os
import shutil
from concurrent.futures import ThreadPoolExecutor
import threading
import time

# Function to fetch .txt files from a directory
def fetch_txt_files(directory_path):
    txt_files = []
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.txt'):
            txt_files.append(file_name)
    return txt_files

# Function to extract elements from file names
def extract_elements(file_names):
    extracted_elements = []
    for file_name in file_names:
        parts = file_name.split('_')
        if len(parts) >= 7:  # Ensure there are enough parts
            first_element = parts[0]
            second_element = parts[1].upper()
            fourth_element = parts[3].upper()
            seventh_element = parts[6]
            extra_element = 'CDR'
            extracted_elements.append(
                (first_element, second_element, seventh_element, fourth_element, extra_element, file_name))
        else:
            # Add to exceptions if format is not correct
            extracted_elements.append(('EXCEPTION', 'EXCEPTION', 'EXCEPTION', 'EXCEPTION', 'CDR', file_name))
    return extracted_elements

# Function to read the domain file
def read_domain_file(file_path):
    first_elements = set()
    second_elements = set()
    fourth_elements = set()
    seventh_elements = set()

    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split('/')
            if len(parts) >= 7:
                first_elements.add(parts[0])
                second_elements.add(parts[1].upper())
                fourth_elements.add(parts[3].upper())
                seventh_elements.add(parts[6])

    return first_elements, second_elements, fourth_elements, seventh_elements

# Function to process a file
def process_file(element_tuple, base_path, txt_files_path, domain_elements, exception_dir_path, lock):
    first_elements, second_elements, fourth_elements, seventh_elements = domain_elements
    file_name = element_tuple[5]
    src_file_path = os.path.join(txt_files_path, file_name)

    if 'EXCEPTION' in element_tuple or \
            element_tuple[0] not in first_elements or \
            element_tuple[1] not in second_elements or \
            element_tuple[2] not in seventh_elements or \
            element_tuple[3] not in fourth_elements:
        dest_dir_path = exception_dir_path
    else:
        dest_dir_path = os.path.join(base_path, element_tuple[0], element_tuple[1], element_tuple[2],
                                     element_tuple[3], element_tuple[4])

    # Move the file if it exists in the source directory and destination directory is valid
    if os.path.exists(src_file_path):
        with lock:
            if os.path.exists(dest_dir_path):
                dest_file_path = os.path.join(dest_dir_path, file_name)
                shutil.move(src_file_path, dest_file_path)
                print("Moved file:", src_file_path, "to", dest_file_path)
                
                # Check for existing text files in the destination directory
                txt_files_in_dest = [f for f in os.listdir(dest_dir_path) if f.endswith('.txt')]
                if len(txt_files_in_dest) > 1:
                    txt_files_in_dest.sort()  # Ensure a consistent order
                    existing_file_path = os.path.join(dest_dir_path, txt_files_in_dest[0])
                    new_file_path = os.path.join(dest_dir_path, txt_files_in_dest[1])

                    with open(existing_file_path, 'a') as existing_file, open(new_file_path, 'r') as new_file:
                        existing_file.write('\n---------------------------\n')
                        existing_file.write(new_file.read())

                    os.remove(new_file_path)
                    print(f"Merged content of {new_file_path} into {existing_file_path} and deleted {new_file_path}")

            else:
                print("Destination directory does not exist:", dest_dir_path)
                # File remains in the txtFiles directory
    else:
        print("Source file does not exist:", src_file_path)

# Function to map files to directories
def map_files_to_directories(base_path, txt_files_path, elements, domain_elements):
    exception_dir_path = os.path.join(base_path, '_Exception')
    if not os.path.exists(exception_dir_path):
        os.makedirs(exception_dir_path)

    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for element_tuple in elements:
            futures.append(executor.submit(process_file, element_tuple, base_path, txt_files_path, domain_elements, exception_dir_path, lock))
        for future in futures:
            future.result()  # To ensure any raised exceptions are caught

# Main function
def main():
    txt_files_path = 'txtFiles'  # Path to the directory containing the .txt files
    base_output_path = 'destFolders'  # Base path where the directories are already created
    domain_file_path = 'input/domain_file.txt'  # Path to the domain file

    # Fetch and extract data in parallel
    with ProcessPoolExecutor() as executor:
        txt_file_names_future = executor.submit(fetch_txt_files, txt_files_path)
        domain_elements_future = executor.submit(read_domain_file, domain_file_path)
        extracted_data_future = executor.submit(extract_elements, txt_file_names_future.result())

    txt_file_names = txt_file_names_future.result()
    print("Text files found:", txt_file_names)

    domain_elements = domain_elements_future.result()
    extracted_data = extracted_data_future.result()

    for data in extracted_data:
        print(data)

    map_files_to_directories(base_output_path, txt_files_path, extracted_data, domain_elements)

# Entry point
if __name__ == '__main__':
    start_time = time.time()  # Record the start time
    main()
    end_time = time.time()  # Record the end time
    print("-------------------Time taken: {:.2f} seconds------------".format(end_time - start_time))
