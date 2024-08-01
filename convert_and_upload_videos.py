#!/usr/bin/env python

"""
Video Upload and SQL Generation Script

This script automates the process of converting MP4 video files to HLS format, retrieving video durations,
and syncing the content to an S3 bucket. It also generates SQL queries based on predefined parameters for
subsequent database updates.

Usage:
1. Set up the required parameters in 'params.input.course_content' and 'params.input.query'.
2. Ensure that the AWS CLI is configured.
3. Run the script using Python.

Dependencies:
- AWS CLI
- ffmpeg

Note:
- This script assumes a specific directory structure for video files.
- The 'course_content' dictionary should contain metadata for each video.
- Update the 's3_key' and 's3_path' variables according to your S3 bucket configuration.

Author: Julie Scherer
"""

import os
import subprocess
import logging
from datetime import datetime
from params.input import course_content, query

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

s3_bucket = "s3_bucket"
s3_key = "noneyabidness"
s3_path = f"s3://{s3_bucket}/{s3_key}"

video_dir = f"{os.getcwd()}/upload_videos/s3/{s3_key}"
logs_out_dir = f"{os.getcwd()}/upload_videos/logs"
sql_out_dir = f"{os.getcwd()}/upload_videos/sql"
sql_sub_dir = os.path.join(sql_out_dir, datetime.now().strftime("%Y-%m-%d"))

video_durations_csv = os.path.join(logs_out_dir, "video_durations.csv")
processing_log = os.path.join(
    logs_out_dir, f"{datetime.now().strftime('%Y-%m-%d')}_processing_log.log"
)
sql_output = os.path.join(
    sql_out_dir, f"{datetime.now().strftime('%Y-%m-%d')}_queries.sql"
)

logging.basicConfig(
    filename=processing_log,
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

logging.info(f"video_dir: {video_dir}")
logging.info(f"logs_out_dir: {logs_out_dir}")
logging.info(f"sql_out_dir: {sql_out_dir}")

logging.info(f"Current working directory: {os.getcwd()}")
logging.info(f"S3 bucket: {s3_path}")


def convert_mp4(
    source_dir,
    base_filename,
    local_mp4_file_path,
    local_m3u8_dir_path,
    local_m3u8_file_path,
    rerun=False,
):
    """
    Converts MP4 files to HLS format (.m3u8) using ffmpeg.

    Args:
        source_dir (str): Source directory containing the MP4 file.
        base_filename (str): Base filename for the video.
        local_mp4_file_path (str): Local path of the MP4 file.
        local_m3u8_dir_path (str): Local directory path for storing HLS files.
        local_m3u8_file_path (str): Local path of the generated M3U8 file.
        rerun (bool, optional): If True, forces the rerun of the conversion process. Default is False.

    Returns:
        bool: True if conversion is successful, False otherwise.
    """

    logging.info(">> Running MP4 to HLS conversion task <<")
    os.makedirs(local_m3u8_dir_path, exist_ok=True)

    if os.path.isfile(local_m3u8_file_path) and rerun:
        os.remove(local_m3u8_file_path)
        logging.info("Deleted existing .m3u8 file")
        ts_files = [
            f
            for f in os.listdir(local_m3u8_dir_path)
            if f.startswith(base_filename) and f.endswith(".ts")
        ]

        for ts_file in ts_files:
            os.remove(os.path.join(local_m3u8_dir_path, ts_file))

        logging.info("Deleted existing .ts files")

    if not os.path.isfile(local_m3u8_file_path) or rerun:
        logging.info(f"Coverting {base_filename}.mp4 file")
        ffmpeg_command = [
            "ffmpeg",
            "-i",
            local_mp4_file_path,
            "-profile:v",
            "baseline",
            "-level",
            "3.0",
            "-s",
            "1920x1080",
            "-start_number",
            "0",
            "-hls_time",
            "10",
            "-hls_list_size",
            "0",
            "-f",
            "hls",
            local_m3u8_file_path,
        ]

        ffmpeg_output = subprocess.run(
            ffmpeg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if ffmpeg_output.stdout and not ffmpeg_output.stderr:
            logging.info(f"HLS conversion successful.")

        for directory in [video_dir, source_dir, local_m3u8_dir_path]:
            del_ds_store_output = subprocess.run(
                ["find", directory, "-name", ".DS_Store", "-type", "f", "-delete"]
            )
            if not del_ds_store_output.stderr:
                logging.info(f"Deleted .DS_Store in {directory}")

    if os.path.isfile(local_m3u8_file_path):
        logging.info(f"Found M3U8 file: {local_m3u8_file_path}")
        return True

    logging.error(f"M3U8 file not found: {local_m3u8_file_path}")
    return False


def retrieve_video_duration(
    folder_path, dest_dir, local_m3u8_file_path, s3_m3u8_file_path
):
    """
    Retrieves the video duration from an M3U8 file.

    Args:
        folder_path (str): Path of the folder being processed.
        dest_dir (str): Destination directory on S3.
        local_m3u8_file_path (str): Local path of the M3U8 file.
        s3_m3u8_file_path (str): S3 path of the M3U8 file.

    Returns:
        bool: True if video duration retrieval is successful, False otherwise.
    """

    logging.info(f"Retrieving video duration for {folder_path}")

    if not os.path.isfile(local_m3u8_file_path):
        s3_mp4_list = (
            subprocess.check_output(["aws", "s3", "ls", dest_dir, "--recursive"])
            .decode("utf-8")
            .split("\n")
        )
        s3_mp4_list = [
            item.split()[-1] for item in s3_mp4_list if item.endswith(".mp4")
        ]

        s3_copy_output = subprocess.run(
            ["aws", "s3", "cp", s3_m3u8_file_path, local_m3u8_file_path]
        )
        if s3_copy_output.stderr:
            logging.error(
                f"Error retrieving M3U8 file from S3: {s3_copy_output.stderr}"
            )
            return False
        elif not os.path.isfile(local_m3u8_file_path):
            logging.warning(
                f"M3U8 file  Unable to retrieve M3U8 file from S3: {s3_copy_output.stderr}"
            )
        else:
            logging.info(
                f"Retrieved M3U8 file from S3. File location: {local_m3u8_file_path}"
            )

    total_duration = 0
    with open(local_m3u8_file_path, "r") as file:
        for line in file:
            if line.startswith("#EXTINF:"):
                total_duration += float(line.split(":")[1].split(",")[0])

    total_duration = round(total_duration)

    if total_duration > 0:
        key = os.path.relpath(local_m3u8_file_path, video_dir)
        with open(video_durations_csv, "a+", newline="") as file:
            file.seek(
                0
            )  # Move the cursor to the beginning of the file to read existing content
            search = [key, str(total_duration)]

            for line in file.readlines():
                if line.strip().split(",") == search:
                    logging.info(
                        f"Found video duration in CSV file. Video duration: {total_duration} seconds. File location: {video_durations_csv}"
                    )
                    return True

            file.seek(0, 2)
            file.write(f"{key},{total_duration}\n")

        logging.info(
            f"Successfully added video duration to CSV. Video: {s3_key}/{folder_path}, Duration (seconds): {total_duration}. File location: {video_durations_csv}"
        )
        return True

    return False


def sync_to_s3(folder_path, base_filename, source_dir, dest_dir):
    """
    Syncs local video files to the specified S3 destination directory, skipping if files are already uploaded.

    Args:
        folder_path (str): Path of the folder being processed.
        source_dir (str): Local source directory.
        dest_dir (str): Destination directory on S3.

    Returns:
        bool: True if synchronization is successful or files are already up-to-date, False otherwise.
    """

    logging.info(f">> Running S3 sync task <<")

    if len(os.listdir(source_dir)) == 1 and os.path.isfile(
        os.path.join(source_dir, ".gitkeep")
    ):
        logging.info(
            f"Skipping S3 sync for {folder_path} as it only contains a .gitkeep file."
        )
        return False

    local_files = os.listdir(os.path.join(source_dir, base_filename))
    if not local_files:
        logging.warning(f"No files found locally.")
        return False

    logging.info(f"Files found locally: {local_files}")

    # Check if files are already uploaded to S3
    s3_ls_command = ["aws", "s3", "ls", f"{os.path.join(dest_dir, base_filename)}/"]
    s3_ls_output = subprocess.run(
        s3_ls_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    if s3_ls_output.stderr:
        logging.warning(
            f"Bucket does not exist or path is incorrect. Command: {' '.join(s3_ls_command)} \n{s3_ls_output.stderr}"
        )

        s3_create_command = [
            "aws",
            "s3api",
            "put-object",
            "--bucket",
            "s3_bucket",
            "--key",
            f"{s3_key}/{folder_path}/",
        ]
        s3_create_output = subprocess.run(
            s3_create_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if s3_create_output.stderr:
            logging.error(
                f"Error creating bucket. Command: {' '.join(s3_create_command)} \n{s3_create_output.stderr}"
            )
            return False

        s3_files = []

    else:
        s3_files = [
            item.split()[-1].strip("/")
            for item in s3_ls_output.stdout.split("\n")
            if item
        ]

    logging.info(f"Files found in S3: {s3_files}")

    if all(file in s3_files for file in local_files):
        logging.info(f"All files are already uploaded to S3. Skipping sync.")

    elif len(s3_files) == len(local_files):
        logging.warning(
            f"Found the same number of files but not the same contents in S3 and local folder: {folder_path}. Proceeding with caution."
        )

    elif len(s3_files) > len(local_files):
        logging.warning(
            f"There are more files in S3 than locally: {folder_path}. Proceeding with caution."
        )

    else:
        logging.info(f"Syncing local files to S3")
        s3_sync_command = ["aws", "s3", "sync", source_dir, dest_dir]
        s3_sync_output = subprocess.run(
            s3_sync_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if s3_sync_output.stderr:
            logging.error(
                f"Error syncing to S3. Command: aws s3 sync {' '.join(s3_sync_command)} \n{s3_sync_output.stderr}"
            )
            return False
        else:
            logging.info(f"Successfully uploaded MP4 files to {dest_dir}")

    return True


def generate_sql_query(folder_path, base_filename, query_text=""):
    """
    Generates SQL queries using the 'course_content' dictionary, which has a nested structure
    matching the local and S3 folder paths. The function retrieves video metadata based on
    the folder structure, updates the query_text string, and constructs SQL queries using the
    predefined query template in params.input.query.

    The 'course_content' dictionary should have keys corresponding to local folder paths and values
    containing metadata, including 'video_url', which is used to match and retrieve video information.

    Args:
        folder_path (str): Path of the folder being processed.
        query_text (str): Accumulated SQL queries.

    Returns:
        str: Updated SQL queries.
    """

    logging.info(">> Running SQL generation task <<")

    folder_keys = folder_path.split("/")
    params_found = course_content

    logging.info(f"Folder keys: {folder_keys}")

    for folder_key in folder_keys:
        params_found = params_found.get(folder_key)

    if not params_found:
        logging.warning(
            f"Query parameters not found in input dictionary. Please check the params were added and the path is correct."  # noqa: F541
        )
        return query_text

    filename = f"{'_'.join(folder_keys[-2:])}_{base_filename}"
    params = params_found.get(base_filename)
    if not params:
        logging.warning(
            f"Key {base_filename} not found in input dictionary. Please check the path is correct."
        )
        return query_text

    logging.info(f"File: {filename}, Value: {params}")

    params["description"] = (
        params["description"].replace("'", "")
        if params.get("description")
        else params["description"]
    )

    query_output = os.path.join(sql_sub_dir, f"{filename}.sql")
    formatted_query = ""
    if params.get("video_url"):
        with open(video_durations_csv, "r") as file:
            for line in file:
                video_url_value, video_duration_value = map(str.strip, line.split(","))
                video_url_value = f"{s3_key}/{video_url_value}"
                if params["video_url"] == video_url_value:
                    params["duration_seconds"] = video_duration_value

                    formatted_query = query.format(**params)
                    query_text += formatted_query
                    logging.info(
                        f"Video duration found in {video_durations_csv}: {video_url_value},{video_duration_value}"
                    )

                    with open(query_output, "w") as sql_file:
                        sql_file.write(formatted_query)
                        logging.info(f"Exported query to {query_output}")

                    return query_text

    return query_text


def clean_up(video_dir):
    for root, subdirs, files in os.walk(video_dir, topdown=False):
        if any(root.endswith(suffix) for suffix in ["/recording", "/lecture", "/lab"]):
            logging.info(f"Deleted {root}")


def main():
    logging.info(f"Running... See logs at {processing_log}")

    # Initialize the application, set up necessary directories and files, and
    # gets the list of folder paths to process
    if not os.path.isdir(video_dir):
        logging.error(f"Invalid folder path: {video_dir}. Exiting.")
        exit(1)

    dirs = [logs_out_dir, sql_out_dir, sql_sub_dir]
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

    files = [video_durations_csv, processing_log, sql_output]
    for file in files:
        if not os.path.isfile(file):
            open(file, "w").close()

    open(processing_log, "w").close()
    open(sql_output, "w").close()

    paths = []
    for root, subdirs, files in os.walk(video_dir, topdown=False):
        if not subdirs and not any(
            root.endswith(suffix) for suffix in [".mp4", ".m3u8", ".tf"]
        ):
            if root.endswith(("/recording", "/lecture", "/lab", "/lecture-lab")):
                root = (
                    root.replace("/recording", "")
                    .replace("/lecture", "")
                    .replace("/lab", "")
                    .replace("/lecture-lab", "")
                )

            paths.append(root.replace(f"{video_dir}/", ""))

    folder_paths = sorted(paths)
    logging.info(f"Folder paths: {folder_paths}")

    # Initialize an empty string to store SQL queries
    global_query_text = ""

    # Iterate through each folder path
    for folder_path in folder_paths:
        source_dir = os.path.join(video_dir, folder_path)
        dest_dir = os.path.join(s3_path, folder_path)

        if not os.path.exists(source_dir):
            logging.error(f"Source directory not found. Check the path: {source_dir}")
            return False

        # List all video files in the source directory
        video_list = [
            video for video in os.listdir(source_dir) if video.endswith(".mp4")
        ]

        if len(video_list) == 0:
            logging.info(f"~ No videos found in {folder_path} ~")
            continue

        # Initialize source and destination directories
        print(f"Processing {folder_path}...")
        logging.info(f"** Processing {folder_path} **")

        # Iterate through each video file in the folder
        for mp4_file in video_list:
            logging.info(f"** Found {mp4_file} file! **")

            # Initialize parameters for HLS conversion
            base_filename = os.path.splitext(os.path.basename(mp4_file))[0]

            local_m3u8_dir_path = os.path.join(source_dir, base_filename)
            os.makedirs(local_m3u8_dir_path, exist_ok=True)

            local_m3u8_file_path = os.path.join(
                source_dir, base_filename, f"{base_filename}.m3u8"
            )
            s3_m3u8_file_path = os.path.join(
                dest_dir, base_filename, f"{base_filename}.m3u8"
            )

            local_mp4_file_path = os.path.join(source_dir, mp4_file)

            # Run HLS conversion task
            convert_mp4(
                source_dir,
                base_filename,
                local_mp4_file_path,
                local_m3u8_dir_path,
                local_m3u8_file_path,
            )

            # Run video duration retrieval task
            retrieve_video_duration(
                folder_path, dest_dir, local_m3u8_file_path, s3_m3u8_file_path
            )

            # Run S3 sync task
            sync_to_s3(folder_path, base_filename, source_dir, dest_dir)

            # Generate SQL query for the current folder
            global_query_text = generate_sql_query(
                folder_path, base_filename, global_query_text
            )

        # Log completion of processing for the current folder
        logging.info(f"** Finished processing {folder_path} **")

    logging.info(" ")

    # Write all SQL queries to a file if there are any
    if global_query_text:
        with open(sql_output, "a") as file:
            file.write(global_query_text)
        logging.info(f"All SQL queries saved at {sql_output}")

    # Clean up temporary files and directories
    clean_up(video_dir)

    print(
        f"Video processing and upload complete. Check logs for details: {processing_log}"
    )
    logging.info("Video conversion and upload complete.")


if __name__ == "__main__":
    main()
