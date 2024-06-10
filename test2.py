import os
import shutil


def fetch_txt_files(directory_path):
    txt_files = []
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.txt'):
            txt_files.append(file_name)
    return txt_files


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


def map_files_to_directories(base_path, txt_files_path, elements):
    exception_dir_path = os.path.join(base_path, '_Exception')
    if not os.path.exists(exception_dir_path):
        os.makedirs(exception_dir_path)

    for element_tuple in elements:
        file_name = element_tuple[5]
        src_file_path = os.path.join(txt_files_path, file_name)

        if 'EXCEPTION' in element_tuple:
            dest_dir_path = exception_dir_path
        else:
            dest_dir_path = os.path.join(base_path, element_tuple[0], element_tuple[1], element_tuple[2],
                                         element_tuple[3], element_tuple[4])

        # Ensure the destination directory exists
        if not os.path.exists(dest_dir_path):
            print("Destination directory does not exist:", dest_dir_path)
            dest_file_path = os.path.join(exception_dir_path, file_name)
        else:
            dest_file_path = os.path.join(dest_dir_path, file_name)

        # Move the file if it exists in the source directory
        if os.path.exists(src_file_path):
            shutil.move(src_file_path, dest_file_path)
            print("Moved file:", src_file_path, "to", dest_file_path)
        else:
            print("Source file does not exist:", src_file_path)


if __name__ == '__main__':
    txt_files_path = 'txtFiles'  # Path to the directory containing the .txt files
    base_output_path = 'destFolders'  # Base path where the directories are already created
    txt_file_names = fetch_txt_files(txt_files_path)

    print("Text files found:", txt_file_names)
    extracted_data = extract_elements(txt_file_names)

    for data in extracted_data:
        print(data)

    map_files_to_directories(base_output_path, txt_files_path, extracted_data)
