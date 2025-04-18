import pandas as pd
import numpy as np
import os

def event_counth(path):
    if not os.path.isdir(path):
        return "Error : {path} is invalid path"
    count_by_file = {}
    files = os.listdir(path)
    
    for file in files:
        print(f'File: {file}')
        try:
            # Read the CSV file
            data = pd.read_csv(os.path.join(folder, file), on_bad_lines='skip')  # Skip lines with issues
            data.drop('EVENT_NARRATIVE', axis=1, inplace=True)  # Remove the 'EVENT_NARRATIVE' column (if exists)
            print(f'Rows: {data.shape[0]}')
            print(f'Columns: {data.columns}')
            print(data.head())
            # Count occurrences of each event
            event_counts = data["EVENT_TYPE"].value_counts()

            # Add the counts to the dictionary, using the filename as the key
            counts_by_file[file] = event_counts

            # Display event counts for the file
            print(f'Event count in file {file}:')
            print(event_counts)

        except Exception as e:
            print(f"Error reading file {file}: {e}")

        # Display event counts for all files
        print("\nSummary of event counts across all files:")
        for file, event_counts in counts_by_file.items():
            print(f"\nFile: {file}")
            print(event_counts)



# Path to the folder
folder = './data/raw/base_disaster/'

# List all files in the folder
files = os.listdir(folder)

# Dictionary to store counts by file
counts_by_file = {}

for file in files:
    print(f'File: {file}')
    try:
        # Read the CSV file
        data = pd.read_csv(os.path.join(folder, file), on_bad_lines='skip')  # Skip lines with issues
        data.drop('EVENT_NARRATIVE', axis=1, inplace=True)  # Remove the 'EVENT_NARRATIVE' column (if exists)
        print(f'Rows: {data.shape[0]}')
        print(f'Columns: {data.columns}')
        print(data.head())
        # Count occurrences of each event
        event_counts = data["EVENT_TYPE"].value_counts()

        # Add the counts to the dictionary, using the filename as the key
        counts_by_file[file] = event_counts

        # Display event counts for the file
        print(f'Event count in file {file}:')
        print(event_counts)

    except Exception as e:
        print(f"Error reading file {file}: {e}")

# Display event counts for all files
print("\nSummary of event counts across all files:")
for file, event_counts in counts_by_file.items():
    print(f"\nFile: {file}")
    print(event_counts)
