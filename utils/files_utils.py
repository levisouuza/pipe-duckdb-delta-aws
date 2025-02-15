import os
from utils.date_utils import get_yesterday_date
from constants.constants import LOCAL_FOLDER_DATASETS


def get_files_in_raw_datasets():
    file_path = []
    files = os.listdir(LOCAL_FOLDER_DATASETS)

    files_csv = [file for file in files if file.endswith(".csv")]
    yesterday_date = get_yesterday_date()

    for file in files_csv:
        filename = os.path.splitext(file)[0]
        path = f"delta-operations/{yesterday_date}/{filename}/{file}"
        file_path.append(
            {"local_filename": LOCAL_FOLDER_DATASETS + file, "s3_filename": path}
        )

    return file_path
