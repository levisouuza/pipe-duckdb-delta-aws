import os

from constants.constants import LOCAL_FOLDER_DATASETS
from utils.date_utils import get_yesterday_date


def get_files_in_raw_datasets():
    file_path = []
    files = os.listdir(LOCAL_FOLDER_DATASETS)

    files_csv = [file for file in files if file.endswith(".csv")]

    for file in files_csv:
        filename = os.path.splitext(file)[0]
        path = f"delta-operations/{filename}/{file}"
        file_path.append(
            {"local_filename": LOCAL_FOLDER_DATASETS + file, "s3_filename": path}
        )

    return file_path
