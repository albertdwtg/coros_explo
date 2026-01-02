import json
import polars as pl
import os
import argparse
from concurrent.futures import ThreadPoolExecutor

def process_json_file(filepath):
    """Traite un fichier JSON provenant d'un fit de Coros, et en créé un CSV"""
    with open(filepath, 'r') as f:
        data = json.load(f)
    all_data = {
        "records": [],
        "events": [],
        "laps": [],
        "sessions": []
    }
    for row in data:
        fields = row.get("fields")
        if fields:
            row_data = {field.get("name"): field.get("value") for field in fields}
            row_data["activity_id"] = os.path.basename(filepath).split('.')[0]
            if row.get("frame_type") == "data_message" and row.get("name") == "record":
                all_data["records"].append(row_data)
            if row.get("frame_type") == "data_message" and row.get("name") == "event":
                all_data["events"].append(row_data)
            if row.get("frame_type") == "data_message" and row.get("name") == "lap":
                all_data["laps"].append(row_data)
            if row.get("frame_type") == "data_message" and row.get("name") == "session":
                all_data["sessions"].append(row_data)
    for key, value in all_data.items():
        try:
            os.makedirs(csv_dir + '/' + key + '/', exist_ok=True)
            pl.DataFrame(value).write_csv(csv_dir + '/' + key + '/' + os.path.basename(filepath).replace('.json', '.csv'))
        except pl.exceptions.ComputeError:
            print(f"Warning: Impossible to load {key} for file {os.path.basename(filepath)}")

def main():
    parser = argparse.ArgumentParser(description="Process JSON files in parallel.")
    parser.add_argument("--json_dir", type=str,
                       help="Directory containing JSON files (input)")
    parser.add_argument("--csv_dir", type=str,
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