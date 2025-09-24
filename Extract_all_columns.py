#!/usr/bin/env python3 
import pandas as pd
import os
import re

# Function to normalize names
def normalize_name(name):
    """Normalize names by removing white space and replacing special characters with underscores."""
    name = name.strip()  # Remove leading and trailing whitespace
    name = re.sub(r"[^\w\s]", "_", name)  # Replace special characters with underscores
    name = re.sub(r"\s+", "_", name)  # Replace spaces with underscores
    return name

# Function to process files, normalize entries, and save the output as CSV
def normalize_files_and_save_with_log(folder_path, output_folder, log_csv):
    excluded_names = [
        "Processed by :-",
        "Name",
        "MRN",
        "Lab No",
        "Referred By",
        "Test Name",
        "Report Printed On:",
        "JANE MUMBI",
        "VARIBIO LAB",
        "VB",
        "VB LAB",
        "VB DOCTOR"
    ]  # List of names to exclude

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # List to store log data
    log_data = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".xls") or file_name.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file_name)
            print(f"Processing file: {file_name}")

            try:
                # Read the file into a DataFrame
                if file_name.endswith(".xls"):
                    df = pd.read_excel(file_path, header=None, engine='xlrd')
                else:
                    df = pd.read_excel(file_path, header=None)

                # Normalize entries in the first three columns and collect log data
                # Normalize entries in the first four columns and collect log data
                for col in range(4):  # First four columns
                    for index, value in df.iloc[:, col].dropna().items():
                        if isinstance(value, str):
                            value_stripped = value.strip()
            
                            # Skip if the name is in excluded_names, contains numbers, asterisks, or is entirely numeric
                            if (
                                value_stripped in excluded_names or
                                any(char.isdigit() for char in value_stripped) or  # Contains any digit
                                "*" in value_stripped or                          # Contains asterisks
                                value_stripped.isnumeric()                        # Is entirely numeric
                            ):
                                continue  # Skip this entry

                            # Normalize and log valid names
                            normalized = normalize_name(value_stripped)
                            log_data.append({
                                "File Name": file_name,
                                "Old Name": value_stripped,
                                "New Name": normalized
                            })
                            df.iat[index, col] = normalized  # Replace the value in the DataFrame

                # Save the modified DataFrame to a CSV file in the new folder
                output_csv_name = os.path.splitext(file_name)[0] + ".csv"  # Change extension to .csv
                output_file_path = os.path.join(output_folder, output_csv_name)
                df.to_csv(output_file_path, index=False, header=False)
                print(f"File saved to: {output_file_path}")

            except Exception as e:
                print(f"Error processing file {file_name}: {e}")

    # Save the log data to a CSV file
    log_df = pd.DataFrame(log_data)
    log_df.to_csv(log_csv, index=False)
    print(f"Log successfully saved to {log_csv}")

# Specify folder paths
input_folder = "./xls"  # Replace with the path to your original folder
output_folder = "./normalized_files"  # Replace with the path to your output folder
log_csv = "normalization_log.csv"  # Output CSV for old and new names

# Run the normalization function
normalize_files_and_save_with_log(input_folder, output_folder, log_csv)

