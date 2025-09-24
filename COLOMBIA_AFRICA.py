#!/usr/bin/env python3
import pandas as pd
import os

def process_files_with_normalization(normalization_log_path, normalized_files_folder, output_csv):
    # Read normalization log file
    normalization_log = pd.read_csv(normalization_log_path)
    unique_files = normalization_log['File Name'].unique()

    # Initialize a DataFrame to store results
    all_results = []
    error_logs = []

    # Define column mappings for merging duplicates
    duplicate_column_pairs = {
        "CHLORIDE__RANDOM_URINE__Urine_": "CHLORIDE__RANDOM_URINE",
        "CREATININE__RANDOM_URINE__Urine_": "CREATININE__RANDOM_URINE",
        "OSMOLALITY__RANDOM_URINE__Urine_": "OSMOLALITY__RANDOM_URINE",
        "POTASSIUM__RANDOM_URINE__Urine_": "POTASSIUM__RANDOM_URINE",
        "SODIUM__RANDOM_URINE__Urine_": "SODIUM__RANDOM_URINE",
        "URINE__PROTEIN__Urine_": "URINE__PROTEIN",
        "URIC_ACID__URINE__Urine_": "URIC_ACID__URINE",
        "URINE_RANDOM_CALCIUM__Urine_": "URINE_RANDOM_CALCIUM",
        "URINE_PHOSPHATE__Urine_": "URINE_PHOSPHATE",
        "URINE_MICROALBUMIN__Urine_": "URINE_MICROALBUMIN",
        "URINE_UREA__Urine_": "URINE_UREA",
        "APOLIPOPROTEIN_B": "APOLIPOPROTEIN_B__Serum_",
        "APOLIPOPROTEINS_A1": "APOLIPOPROTEINS_A1__Serum_",
        "OSMOLALITY__SERUM": "OSMOLALITY__SERUM__Serum_",
        "CALCIUM__SERUM": "CALCIUM__SERUM__Serum_"
    }

    # Define calcium variants to consolidate
    calcium_variants = [
        "CALCIUM__SERUM",
        "CALCIUM__SERUM__Serum_",
        "CALCIUM__SERUM_Note__Corrected_for_serum_albumin__2_08",
        "CALCIUM__SERUM__Serum__Note__Corrected_serum_calcium_for_low_albumin___2_11_mmol_L",
        "CALCIUM__SERUM__Serum__Note__Corrected_calcium_for_albumin_is_2_15_mmol_L"
    ]

    for file_name in unique_files:
        file_path = os.path.join(normalized_files_folder, file_name)
        print(f"Processing file: {file_name}")

        try:
            # Load the file
            if file_name.endswith(".csv"):
                df = pd.read_csv(file_path, header=None)
            else:
                raise ValueError(f"Unsupported file format: {file_name}")

            # Extract metadata (consider offset for structure variations)
            offset = 0 if file_name not in normalization_log['File Name'].tolist() else -2
            metadata = {"File Name": file_name}

            # Extract metadata and handle missing values
            metadata_fields = {
                "Name": (2, 5 + offset),
                "MRN": (4, 5 + offset),
                "Lab No": (6, 5 + offset),
                "Referred By": (8, 5 + offset),
                "Age": (2, 12 + offset),
                "Gender": (2, 16 + offset),
                "Collected On": (6, 12 + offset),
                "Received On": (6, 16 + offset),
                "Reported On": (8, 12 + offset),
            }

            for key, (row, col) in metadata_fields.items():
                try:
                    metadata[key] = df.iat[row, col]
                except IndexError:
                    metadata[key] = "Missing"
                    error_logs.append(f"Missing '{key}' in file: {file_name}")

            # Extract test results
            test_names = normalization_log[normalization_log['File Name'] == file_name]['New Name'].tolist()
            for row in range(14, df.shape[0]):  # Start at row 14
                try:
                    for col in range(3):  # Search in the first three columns
                        test_name = df.iat[row, col]
                        if isinstance(test_name, str) and test_name.strip() in test_names:
                            result_col = col + 4  # Assuming result is 3 columns over
                            if result_col < df.shape[1]:  # Ensure the column exists
                                result = df.iat[row, result_col]
                                metadata[test_name.strip()] = result
                except Exception as e:
                    error_logs.append(f"Error processing row {row} in file {file_name}: {e}")

            # Merge duplicate columns
            for old_col, new_col in duplicate_column_pairs.items():
                if old_col in metadata and new_col in metadata:
                    metadata[new_col] = metadata[old_col] or metadata[new_col]  # Prioritize non-empty
                    metadata.pop(old_col, None)  # Remove old column

            # Consolidate calcium variants
            metadata["Calcium Corrected Serum"] = None
            for variant in calcium_variants:
                if variant in metadata and metadata[variant] != "Missing":
                    metadata["Calcium Corrected Serum"] = metadata[variant]
                    metadata.pop(variant, None)

            # Append metadata to the results list
            all_results.append(metadata)

        except Exception as e:
            print(f"Error processing file {file_name}: {e}")
            error_logs.append(f"File-level error for {file_name}: {e}")

    # Convert results to DataFrame and save
    results_df = pd.DataFrame(all_results)
    results_df.to_csv(output_csv, index=False)
    print(f"Combined results successfully saved to {output_csv}")

    # Log errors to a separate file
    with open("error_log.txt", "w") as error_file:
        for error in error_logs:
            error_file.write(f"{error}\n")
    print("Error log saved to error_log.txt")


# Paths and Parameters
normalization_log_path = "normalization_log_SECOND.csv"  # Path to the normalization log
normalized_files_folder = "./normalized_files"  # Folder with normalized files
output_csv = "combined_output.csv"  # Output file for combined results

# Run the function
process_files_with_normalization(normalization_log_path, normalized_files_folder, output_csv)

