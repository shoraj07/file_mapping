import os
import pytest
import shutil
from concurrent.futures import ProcessPoolExecutor
from file_mapping_multiProcessing import fetch_txt_files, extract_elements, read_domain_file, map_files_to_directories

# Constants for test paths
TEST_TXT_FILES_PATH = 'test_txtFiles'
TEST_BASE_OUTPUT_PATH = 'test_destFolders'
TEST_DOMAIN_FILE_PATH = 'test_domain_file.txt'


@pytest.fixture(scope="module")
def setup_environment():
    # Create test directories
    os.makedirs(TEST_TXT_FILES_PATH, exist_ok=True)
    os.makedirs(TEST_BASE_OUTPUT_PATH, exist_ok=True)

    # Generate 10,000 test txt files
    for i in range(1, 10001):
        file_name = f"test_{i}_DATA_{i}_INFO_{i}_EXTRA_{i}_DETAIL.txt"
        with open(os.path.join(TEST_TXT_FILES_PATH, file_name), 'w') as f:
            f.write(f"Sample content for file {i}\n")

    # Generate domain file with valid elements
    with open(TEST_DOMAIN_FILE_PATH, 'w') as f:
        for i in range(1, 10001):
            f.write(f"test_{i}/data_{i}/info_{i}/extra_{i}/detail_{i}\n")

    yield

    # Cleanup after tests
    shutil.rmtree(TEST_TXT_FILES_PATH)
    shutil.rmtree(TEST_BASE_OUTPUT_PATH)
    if os.path.exists(TEST_DOMAIN_FILE_PATH):
        os.remove(TEST_DOMAIN_FILE_PATH)


def test_script_with_10000_entries(setup_environment):
    # Use the same steps as in the script's main function

    with ProcessPoolExecutor() as executor:
        txt_file_names_future = executor.submit(fetch_txt_files, TEST_TXT_FILES_PATH)
        domain_elements_future = executor.submit(read_domain_file, TEST_DOMAIN_FILE_PATH)
        extracted_data_future = executor.submit(extract_elements, txt_file_names_future.result())

    txt_file_names = txt_file_names_future.result()
    assert len(txt_file_names) == 10000, "Not all text files were found"

    domain_elements = domain_elements_future.result()
    extracted_data = extracted_data_future.result()
    assert len(extracted_data) == 10000, "Not all text files were processed correctly"

    map_files_to_directories(TEST_BASE_OUTPUT_PATH, TEST_TXT_FILES_PATH, extracted_data, domain_elements)

    # Check if files were moved to correct directories
    for data in extracted_data:
        if 'EXCEPTION' in data:
            dest_dir = os.path.join(TEST_BASE_OUTPUT_PATH, '_Exception')
        else:
            dest_dir = os.path.join(TEST_BASE_OUTPUT_PATH, data[0], data[1], data[2], data[3], data[4])
        dest_file_path = os.path.join(dest_dir, data[5])
        assert os.path.exists(dest_file_path), f"File {data[5]} was not moved correctly"


if __name__ == "__main__":
    pytest.main([__file__])
