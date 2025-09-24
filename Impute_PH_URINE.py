#!/usr/bin/env python3
import pandas as pd
import os
from difflib import SequenceMatcher

# Function to check for a 75% match
def is_similar(a, b, threshold=0.85):
    return SequenceMatcher(None, a, b).ratio() >= threshold

# Function to process files and update the META.csv
def update_meta_with_ph_and_gamma(meta_file, normalized_folder, output_file):
    # Load the META.csv
    meta_df = pd.read_csv(meta_file)

    # Ensure the required columns exist in the META.csv
    if "ph___urine__urine_" not in meta_df.columns:
        meta_df["ph___urine__urine_"] = None
    if "GAMMA_GT__GGT_" not in meta_df.columns:
        meta_df["GAMMA_GT__GGT_"] = None

    # Iterate through each file name in the "file_name" column of META.csv
    for _, row in meta_df.iterrows():
        file_name = row["file_name"]
        file_path = os.path.join(normalized_folder, file_name)

        if os.path.exists(file_path):
            try:
                # Load the corresponding normalized file
                df = pd.read_csv(file_path, header=None)

                # Logic for "PH___URINE__Urine_"
                found_ph = False
                for row_index in range(len(df)):
                    for col_index in range(4):  # First four columns
                        cell_value = str(df.iat[row_index, col_index]).strip() if pd.notna(df.iat[row_index, col_index]) else ""

                        if is_similar(cell_value, "PH___URINE__Urine_"):
                            # Once found, search the entire row for any non-empty entry other than the target
                            for col_check in range(len(df.columns)):
                                value = str(df.iat[row_index, col_check]).strip() if pd.notna(df.iat[row_index, col_check]) else ""
                                if value and value != cell_value:  # Ensure it's not the target term
                                    meta_df.at[_, "ph___urine__urine_"] = value
                                    found_ph = True
                                    break

                            # If no value found in the same row, check the next row
                            if not found_ph and row_index + 1 < len(df):
                                for col_check in range(len(df.columns)):
                                    value = str(df.iat[row_index + 1, col_check]).strip() if pd.notna(df.iat[row_index + 1, col_check]) else ""
                                    if value:  # Any non-empty value
                                        meta_df.at[_, "ph___urine__urine_"] = value
                                        found_ph = True
                                        break

                            if found_ph:
                                break
                    if found_ph:
                        break

                # If no entry is found, leave as NA
                if not found_ph:
                    meta_df.at[_, "ph___urine__urine_"] = "NA"

                # Logic for "GAMMA_GT__GGT_"
                found_gamma = False
                for row_index in range(len(df)):
                    for col_index in range(3):  # Search in the first three columns
                        cell_value = str(df.iat[row_index, col_index]).strip() if pd.notna(df.iat[row_index, col_index]) else ""

                        if cell_value == "GAMMA_GT__GGT_":
                            result_col = col_index + 4  # Assuming result is 3 columns over
                            if result_col < df.shape[1]:  # Ensure the result column exists
                                result = str(df.iat[row_index, result_col]).strip() if pd.notna(df.iat[row_index, result_col]) else "NA"
                                meta_df.at[_, "GAMMA_GT__GGT_"] = result
                                found_gamma = True
                            break
                    if found_gamma:
                        break

                # If no entry is found, leave as NA
                if not found_gamma:
                    meta_df.at[_, "GAMMA_GT__GGT_"] = "NA"

            except Exception as e:
                print(f"Error processing file {file_name}: {e}")
        else:
            print(f"File {file_name} not found in {normalized_folder}.")
            meta_df.at[_, "ph___urine__urine_"] = "NA"
            meta_df.at[_, "GAMMA_GT__GGT_"] = "NA"

    # Save the updated META.csv with a new name
    meta_df.to_csv(output_file, index=False)
    print(f"Updated META.csv saved to {output_file}")

# Paths and file names
meta_file = "COLOMBIA_WITH_META.csv"  # Input META.csv file
normalized_folder = "normalized_files"  # Folder containing the .csv files to search
output_file = "META_updated_FINAL_COLOMBIA.csv"  # Output file name for the updated META.csv

# Run the function
update_meta_with_ph_and_gamma(meta_file, normalized_folder, output_file)
