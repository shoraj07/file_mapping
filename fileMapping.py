"""
- executed files names saved in resounce as processed_file_log
- tar and merged files on the basis of element 1, 2, 3 is saved in lab/metadata
- final output is saved in cdrs folder 
- error files will be inside cdrs/date_time_stamped_folder/A or B/_errors
- input is taken from source folder.

Final working file.
sraj1-07-23-24
"""
import os
import shutil
import tarfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
from threading import Lock
from datetime import datetime

lock = Lock()

def fetch_txt_files(directory_path):
    txt_files = [file_name for file_name in os.listdir(directory_path) if file_name.endswith('.txt')]
    return txt_files

def extract_elements(file_names):
    extracted_elements = []
    for file_name in file_names:
        parts = file_name.split('_')
        if len(parts) >= 7:  # Ensure there are enough parts
            first_element = parts[0]
            second_element = parts[1].upper()
            third_element = parts[2]
            fourth_element = parts[3].upper()
            extra_element = 'CDR'
            extracted_elements.append(
                (first_element, second_element, third_element, fourth_element, extra_element, file_name))
        else:
            # Add to exceptions if format is not correct
            extracted_elements.append(('EXCEPTION', 'EXCEPTION', 'EXCEPTION', 'EXCEPTION', 'CDR', file_name))
    return extracted_elements

def read_domain_file(file_path):
    first_elements = set()
    second_elements = set()
    third_elements = set()
    fourth_elements = set()

    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split('/')
            if len(parts) >= 4:  # Ensure there are enough parts
                first_elements.add(parts[0])
                second_elements.add(parts[1].upper())
                third_elements.add(parts[2])
                fourth_elements.add(parts[3].upper())

    return first_elements, second_elements, third_elements, fourth_elements

def create_merged_files_and_tar(base_path, date_time_str, elements):
    file_groups = defaultdict(list)

    for element_tuple in elements:
        if 'EXCEPTION' not in element_tuple:
            key = (element_tuple[0], element_tuple[1], element_tuple[2], element_tuple[3])
            file_groups[key].append(element_tuple[5])

    merged_files = []

    for key, files in file_groups.items():
        # Use key for the merged file name and place it directly in the base_path
        merged_file_name = "%s_%s_%s_%s_%s.txt" % (key[0], key[1], key[2], key[3], date_time_str)
        merged_file_path = os.path.join(base_path, merged_file_name)

        with open(merged_file_path, 'w') as merged_file:
            for file_name in files:
                src_file_path = os.path.join("source", file_name)
                if os.path.exists(src_file_path):
                    with open(src_file_path, 'r') as f:
                        merged_file.write(f.read())
                        merged_file.write('\n---------------------------\n')
                    # print("Merged file:", src_file_path, "into", merged_file_path)
                else:
                    print("Source file does not exist:", src_file_path)

        # Add the merged file to the list
        merged_files.append(merged_file_path)

        # Create tar file
        tar_file_name = "%s_%s_%s_%s.tar" % (key[0], key[1], key[2], key[3])
        tar_file_path = os.path.join(base_path, tar_file_name)
        with tarfile.open(tar_file_path, "w") as tar:
            tar.add(merged_file_path, arcname=os.path.basename(merged_file_path))
            # print("Added merged file to tar:", merged_file_path)

    return merged_files

def process_file(merged_file_path, base_path, domain_elements, date_time_str, time_period):
    """Processes each merged file and moves it to the appropriate directory. Logs file names."""
    first_elements, second_elements, third_elements, fourth_elements = domain_elements
    file_name = os.path.basename(merged_file_path)
    parts = file_name.split('_')
    first_element, second_element, third_element, fourth_element = parts[0], parts[1], parts[2], parts[3]

    dest_dir_path = os.path.join(base_path, date_time_str, time_period, first_element)
    error_dir_path = os.path.join(base_path, date_time_str, time_period, '_Errors')
    log_file_path = os.path.join('resource', 'processed_files_log.txt')

    # Ensure the log file exists
    if not os.path.exists('resource'):
        os.makedirs('resource')
    
    # Determine destination directory
    if first_element not in first_elements or \
            second_element not in second_elements or \
            third_element not in third_elements or \
            fourth_element not in fourth_elements:
        dest_dir_path = error_dir_path
        file_logged = False
    else:
        file_logged = True

    if os.path.exists(merged_file_path):
        try:
            with lock:
                if not os.path.exists(dest_dir_path):
                    os.makedirs(dest_dir_path)
                dest_file_path = os.path.join(dest_dir_path, file_name)
                shutil.move(merged_file_path, dest_file_path)
                # print("Moved file:", merged_file_path, "to", dest_file_path)

                # Log the file name only if it's not an error file
                if file_logged:
                    with open(log_file_path, 'a') as log_file:
                        log_file.write(f"{file_name}\n")
                        # print("Logged file name:", file_name)

        except Exception as e:
            print(f"Error moving file {merged_file_path}: {e}")

def map_files_to_directories(base_path, merged_files, domain_elements, date_time_str, time_period):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for merged_file_path in merged_files:
            futures.append(executor.submit(process_file, merged_file_path, base_path, domain_elements, date_time_str, time_period))
        for future in futures:
            future.result()  # To ensure any raised exceptions are caught

def main():
    txt_files_path = 'source'  # Path to the directory containing the .txt files
    base_output_path = 'cdrs'  # Base path where the directories are already created
    domain_file_path = 'resource/domain_file.txt'  # Path to the domain file
    tar_file_base_path = 'lab/metadata'  # Base path for the .tar file

    current_time = datetime.now()
    time_A = '071500'
    time_B = '151500'
    time_period = 'B' if current_time.hour >= 15 else 'A'
    date_time_str = "%s%s" % (current_time.strftime('%y%m%d'), time_A if time_period == 'A' else time_B)

    # Create the timestamped directory under tar_file_base_path
    timestamped_dir_path = os.path.join(tar_file_base_path, date_time_str)
    if not os.path.exists(timestamped_dir_path):
        os.makedirs(timestamped_dir_path)

    # Fetch and extract data in parallel
    with ProcessPoolExecutor() as executor:
        txt_file_names_future = executor.submit(fetch_txt_files, txt_files_path)
        domain_elements_future = executor.submit(read_domain_file, domain_file_path)
        extracted_data_future = executor.submit(extract_elements, txt_file_names_future.result())

    txt_file_names = txt_file_names_future.result()
    # print("Text files found:", txt_file_names)

    domain_elements = domain_elements_future.result()
    extracted_data = extracted_data_future.result()

    # for data in extracted_data:
    #     print(data)

    # Create merged files and tar files
    merged_files = create_merged_files_and_tar(timestamped_dir_path, date_time_str, extracted_data)

    # Map files to directories
    map_files_to_directories(base_output_path, merged_files, domain_elements, date_time_str, time_period)

if __name__ == '__main__':
    print("-------------------Executing...")
    start_time = time.time()  # Record the start time
    main()
    end_time = time.time()  # Record the end time
    print("-------------------Time taken: {:.2f} seconds------------".format(end_time - start_time))
