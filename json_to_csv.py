import json
import polars as pl
import os
import argparse
from concurrent.futures import ThreadPoolExecutor

def process_json_file(filepath):
    """Traite un fichier JSON provenant d'un fit de Coros, et en créé un CSV"""
    with open(filepath, 'r') as f:
        data = json.load(f)
    all_records = []
    for row in data:
        if row.get("frame_type") == "data_message" and row.get("name") == "record":
            fields = row.get("fields")
            record_data = {field.get("name"): field.get("value") for field in fields}
            all_records.append(record_data)
    pl.DataFrame(all_records).write_csv(csv_dir + '/' + os.path.basename(filepath).replace('.json', '.csv'))

def main():
    parser = argparse.ArgumentParser(description="Process JSON files in parallel.")
    parser.add_argument("--json_dir", type=str, default='/home/albert/repos/coros_explo/.json_data',
                       help="Directory containing JSON files (input)")
    parser.add_argument("--csv_dir", type=str, default='/home/albert/repos/coros_explo/.csv_data',
                       help="Directory containing CSV files (output)")
    args = parser.parse_args()

    global csv_dir
    csv_dir = args.csv_dir

    # Create CSV directory if it doesn't exist
    os.makedirs(csv_dir, exist_ok=True)

    # List of JSON files to process
    filepaths = [os.path.join(args.json_dir, f) for f in os.listdir(args.json_dir) if f.endswith('.json')]
    print(f"Found {len(filepaths)} JSON files to process")

    # Execution with multithreading
    with ThreadPoolExecutor(max_workers=8) as executor:
        dfs = list(executor.map(process_json_file, filepaths))

if __name__ == "__main__":
    main()