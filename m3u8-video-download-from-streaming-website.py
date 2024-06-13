import os
import requests
import m3u8
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_ts_segment(segment_url, segment_index, save_dir, retries=3, timeout=10):
    for attempt in range(retries):
        try:
            response = requests.get(segment_url, stream=True, timeout=timeout)
            response.raise_for_status()
            segment_path = os.path.join(save_dir, f"segment_{segment_index}.ts")
            with open(segment_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        file.write(chunk)
            logging.info(f"Downloaded segment {segment_index} from {segment_url}")
            return segment_path
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to download segment {segment_url} (attempt {attempt + 1}/{retries}): {e}")
            sleep(2)  # Wait before retrying
    logging.error(f"Failed to download segment {segment_url} after {retries} attempts")
    return None

def download_from_m3u8(m3u8_url, save_dir, output_file, retries=3, timeout=10):
    try:
        response = requests.get(m3u8_url, timeout=timeout)
        response.raise_for_status()
        m3u8_obj = m3u8.loads(response.text)

        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        segment_paths = [None] * len(m3u8_obj.segments)
        with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust number of threads as needed
            futures = [
                executor.submit(download_ts_segment, segment.uri, i, save_dir, retries, timeout)
                for i, segment in enumerate(m3u8_obj.segments)
            ]

            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading segments"):
                segment_path = future.result()
                if segment_path:
                    segment_index = int(segment_path.split('_')[-1].split('.')[0])
                    segment_paths[segment_index] = segment_path

        # Check if all segments were downloaded
        if None in segment_paths:
            missing_segments = [i for i, path in enumerate(segment_paths) if path is None]
            logging.error(f"Missing segments: {missing_segments}")
            return

        # Merge all segments
        with open(os.path.join(save_dir, output_file), 'wb') as merged_file:
            for segment_path in segment_paths:
                if segment_path and os.path.exists(segment_path):
                    with open(segment_path, 'rb') as segment_file:
                        merged_file.write(segment_file.read())
                    os.remove(segment_path)  # Remove segment file after merging

        logging.info(f"All segments downloaded and merged into {output_file}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download m3u8 playlist: {e}")

# Example usage
m3u8_url = "https://ewal.an3418211.site/_v2-lrld/9a701df34ea7e4ae16c25b01dd7fefae201975c45d1cc5e3888a78fe4fcd06741ae3e719f0ef63837c6cf62e8daaecb2817ee6ae0d2652909a222e9c8158016d2ca472c63ac0f2e86495f37f925b90c805d57da8f7e58e57c37949b3a593/h/d4/d;9d705ee448b4e4e553dc06568f6feda3345b239e1c12c6.m3u8"
save_directory = "downloads"
output_filename = "merged_video.ts"

download_from_m3u8(m3u8_url, save_directory, output_filename)
