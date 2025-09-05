import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob

CSV_DIR = os.path.expanduser("~/Desktop/compare_2_files") # Define the directory where CSV files are located. This is my local directory
KEY_COLUMNS = ['employee_id'] # Define the key columns according to dataset

def get_record_identifier(row, key_columns=None):
    """Get The columns used in the duplication analysis:
    use key columns; if do not exist, use the fallback (first) column; 
    if not - then take the first non-null column (if nulls exist in the previous columns)"""
    # Resolve default key columns
    if key_columns is None:
        key_columns = KEY_COLUMNS
    try:
        # First try key columns
        key_values = [f"{col}={row[col]}" for col in key_columns if col in row]
        if key_values:
            return ", ".join(key_values)
        
        # Fallback to the first data column (excluding helper columns)
        helper_cols = {'_merge_key', 'record_identifier', 'source_file', 'error_type', 'num_errors'}
        for col in row.index:
            if col not in helper_cols:
                if pd.notna(row[col]):
                    return f"{col}={row[col]}"
                else:
                    # If first column is null, still use it by name for traceability
                    return f"{col}={row[col]}"
        
        # Lastly, first non-null value in any column
        for col in row.index:
            if pd.notna(row[col]):
                return f"{col}={row[col]}"
        
        return "Unknown record" # If no column can be used as a key column
    except Exception as e:
        return "Error identifying record"
        
def find_duplicates_and_missing(df1, df2, key_columns=None):
    """
    Analyzes the dataframes for duplicates and missing records; 
    Sorts by error_type and source_file;
    Returns a DataFrame with error types marked
    """
    if key_columns is None:
        key_columns = KEY_COLUMNS
    try:
        # Initialize error tracking DataFrames
        df1 = df1.copy()
        df2 = df2.copy()
        
        # Ensure key columns exist in both dataframes
        for col in key_columns:
            if col not in df1.columns or col not in df2.columns:
                raise ValueError(f"Key column '{col}' not found in both dataframes")
        
        # Detect full-row duplicates (all columns identical) in each file
        full_dup_mask1 = df1.duplicated(keep=False)
        full_dup_mask2 = df2.duplicated(keep=False)
        
        if full_dup_mask1.any():
            full_dup_counts1 = df1.groupby(list(df1.columns)).size().reset_index(name='num_errors')
            full_duplicates1 = df1[full_dup_mask1].drop_duplicates().copy()
            full_duplicates1 = full_duplicates1.merge(full_dup_counts1, on=list(df1.columns))
        else:
            full_duplicates1 = df1[full_dup_mask1].copy()
            full_duplicates1['num_errors'] = 0
        
        if full_dup_mask2.any():
            full_dup_counts2 = df2.groupby(list(df2.columns)).size().reset_index(name='num_errors')
            full_duplicates2 = df2[full_dup_mask2].drop_duplicates().copy()
            full_duplicates2 = full_duplicates2.merge(full_dup_counts2, on=list(df2.columns))
        else:
            full_duplicates2 = df2[full_dup_mask2].copy()
            full_duplicates2['num_errors'] = 0
        
        # Add source identifier
        df1['source_file'] = 'file1'
        df2['source_file'] = 'file2'
        full_duplicates1['source_file'] = 'file1'
        full_duplicates2['source_file'] = 'file2'
        
        # Mark full duplicates
        full_duplicates1['error_type'] = 'f'
        full_duplicates2['error_type'] = 'f'
        
        # Find key-based duplicates in each file based on key columns (excluding rows that are full-duplicates)
        key_dup_mask1 = df1.duplicated(subset=key_columns, keep=False)
        key_dup_mask2 = df2.duplicated(subset=key_columns, keep=False)
        duplicates1 = df1[key_dup_mask1 & ~full_dup_mask1].copy()
        duplicates2 = df2[key_dup_mask2 & ~full_dup_mask2].copy()
        
        # Count key-based duplicates and keep unique records by key
        if not duplicates1.empty:
            dup_counts1 = duplicates1.groupby(key_columns).size().reset_index(name='num_errors')
            duplicates1 = duplicates1.drop_duplicates(subset=key_columns, keep='first')
            duplicates1 = duplicates1.merge(dup_counts1, on=key_columns)
        else:
            duplicates1['num_errors'] = 0

        if not duplicates2.empty:
            dup_counts2 = duplicates2.groupby(key_columns).size().reset_index(name='num_errors')
            duplicates2 = duplicates2.drop_duplicates(subset=key_columns, keep='first')
            duplicates2 = duplicates2.merge(dup_counts2, on=key_columns)
        else:
            duplicates2['num_errors'] = 0
        
        # Mark key-based duplicates
        duplicates1['error_type'] = 'k'
        duplicates2['error_type'] = 'k'
        
        # Create comparison keys
        df1_keys = df1[key_columns].apply(lambda x: tuple(x.values.astype(str)), axis=1)
        df2_keys = df2[key_columns].apply(lambda x: tuple(x.values.astype(str)), axis=1)
        
        # Find missing records using sets
        keys1_set = set(df1_keys)
        keys2_set = set(df2_keys)
        
        # Get indices of missing and extra records
        missing_in_2_mask = df1_keys.apply(lambda x: x not in keys2_set) # Marks rows in file 1 whose key does not exist in file 2
        extra_in_2_mask = df2_keys.apply(lambda x: x not in keys1_set) # Marks rows in file 2 whose key does not exist in file 1 
        
        # Get missing and extra records
        missing_in_2 = df1[missing_in_2_mask].copy()
        extra_in_2 = df2[extra_in_2_mask].copy()
        
        # Mark missing and extra records with count of 1
        missing_in_2['error_type'] = 'm'  # missing from target
        extra_in_2['error_type'] = 'e'    # extra in target
        missing_in_2['num_errors'] = 1
        extra_in_2['num_errors'] = 1
        
        # Combine all error records (full duplicates, key duplicates, missing records, extra records)
        error_records = pd.concat([
            full_duplicates1, full_duplicates2, 
            duplicates1, duplicates2, 
            missing_in_2, extra_in_2
        ], ignore_index=True)
        
        # Sort by error_type and source_file
        error_records = error_records.sort_values(['error_type', 'source_file'])
        
        return error_records
        
    except Exception as e:
        print(f"Error in find_duplicates_and_missing: {str(e)}")
        raise
 
def string_compare(s1, s2):
    """Compare two string series, and counts how many positions differ between them"""
    try:
        # Convert to string (to avoid dtype issues) and reset index
        s1 = s1.astype(str).reset_index(drop=True)
        s2 = s2.astype(str).reset_index(drop=True)
        return (s1 != s2).sum()
    except Exception as e:
        print(f"Error in string_compare: {str(e)}")
        return 0

def compare_column_order(df1, df2):
    """Compare column order between two dataframes and returns a list of column order differences"""
    common_cols = list(set(df1.columns) & set(df2.columns))
    order_diff = []
    
    for col in common_cols:
        pos1 = df1.columns.get_loc(col) + 1  # Make it 1-based for readability
        pos2 = df2.columns.get_loc(col) + 1
        if pos1 != pos2:
            order_diff.append(f"Column '{col}': position {pos1} in file1, position {pos2} in file2")
    
    return order_diff 

def get_output_filenames(file1_path, file2_path):
    """Generate output filenames (based on input file names)"""
    # Get base names without extension
    file1_name = os.path.splitext(os.path.basename(file1_path))[0]
    file2_name = os.path.splitext(os.path.basename(file2_path))[0]
    
    # Create suffix from both filenames with double underscore
    suffix = f"__{file1_name}_vs_{file2_name}"
    
    # Generate output filenames
    results_file = f"comparison_results{suffix}.txt"
    errors_file = f"error_records{suffix}.csv"
    
    return results_file, errors_file

def get_timestamp_header(file1_path, file2_path):
    """Generate header with timestamp and file information"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    header = [
        "=== COMPARISON DETAILS ===",
        f"Comparison performed on: {current_time}",
        f"File 1: {os.path.basename(file1_path)}",
        f"File 2: {os.path.basename(file2_path)}",
        "\n"  # Add blank line after header
    ]
    return "\n".join(header)

def compare_values_with_identification(df1, df2, common_cols, key_columns=None):
    """Compare values between dataframes and return differences with record identification"""
    differences = []
    
    try:
        # Ensure key_columns is set
        if key_columns is None:
            key_columns = KEY_COLUMNS
        # Ensure key_columns is a list
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        
        # Validate key columns exist
        for col in key_columns:
            if col not in df1.columns or col not in df2.columns:
                raise ValueError(f"Key column '{col}' not found in both dataframes")
        
        # Create a merged dataframe for comparison
        df1_temp = df1.copy()
        df2_temp = df2.copy()
        
        # Create merge key
        if len(key_columns) == 1:
            df1_temp['_merge_key'] = df1_temp[key_columns[0]].astype(str)
            df2_temp['_merge_key'] = df2_temp[key_columns[0]].astype(str)
        else:
            df1_temp['_merge_key'] = df1_temp[key_columns].astype(str).agg('_'.join, axis=1)
            df2_temp['_merge_key'] = df2_temp[key_columns].astype(str).agg('_'.join, axis=1)
        
        # Add record identifiers
        df1_temp['record_identifier'] = df1_temp.apply(lambda row: get_record_identifier(row, key_columns=key_columns), axis=1)
        df2_temp['record_identifier'] = df2_temp.apply(lambda row: get_record_identifier(row, key_columns=key_columns), axis=1)
        
        # Merge dataframes
        merged = pd.merge(df1_temp, df2_temp, on='_merge_key', suffixes=('_1', '_2'))
        
        # Remove duplicate rows based on merge key
        merged = merged.drop_duplicates(subset=['_merge_key'])
        
        # Compare values for each common column
        for col in common_cols:
            if col in key_columns:
                continue  # Skip key columns as they were used for merging
                
            col1 = f"{col}_1"
            col2 = f"{col}_2"
            
            if col1 in merged.columns and col2 in merged.columns:
                # Convert to string for comparison to handle different types
                s1 = merged[col1].astype(str)
                s2 = merged[col2].astype(str)
                
                # Find mismatches
                mismatches = s1 != s2
                mismatch_records = merged[mismatches]
                
                if not mismatch_records.empty:
                    differences.append(f"\nValue mismatches in column '{col}':")
                    for _, row in mismatch_records.iterrows():
                        differences.append(f"  - {row['record_identifier_1']}:")
                        # Format values based on type
                        val1 = row[col1]
                        val2 = row[col2]
                        
                        # Check if the column is string/object type in original dataframes
                        is_text = (df1[col].dtype == 'object' or 
                                 df2[col].dtype == 'object' or 
                                 'datetime' in str(df1[col].dtype) or 
                                 'datetime' in str(df2[col].dtype))
                        
                        if is_text:
                            differences.append(f"    Source: '{val1}'")
                            differences.append(f"    Target: '{val2}'")
                        else:
                            differences.append(f"    Source: {val1}")
                            differences.append(f"    Target: {val2}")
        
        return differences
        
    except Exception as e:
        print(f"Detailed error in value comparison: {str(e)}")
        print(f"Debug info - df1 columns: {df1.columns.tolist()}")
        print(f"Debug info - df2 columns: {df2.columns.tolist()}")
        print(f"Debug info - key_columns: {key_columns}")
        return [f"\nError comparing values: {str(e)}"]

def enhanced_csv_comparison(file1_path, file2_path):
    """
    Enhanced comparison of two CSV files with data quality checks:
    """
    try:
        # Read CSVs without assuming column order
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)
    except Exception as e:
        return f"Error reading files: {str(e)}", None
    
    results = []
    
    # Add timestamp and file information header
    results.append(get_timestamp_header(file1_path, file2_path))
    
    # Basic Record Count Check
    results.append("=== BASIC RECORD COUNT ===")
    records_match = len(df1) == len(df2)
    results.append(f"Record Count Check: {'PASS' if records_match else 'FAIL'}")
    results.append(f"File 1 records: {len(df1)}")
    results.append(f"File 2 records: {len(df2)}")
    
    # Column Comparison
    results.append("\n=== COLUMN ANALYSIS ===")
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    common_cols = list(cols1.intersection(cols2))
    
    # Find missing or extra columns
    missing_cols = cols1 - cols2
    extra_cols = cols2 - cols1
    
    if missing_cols:
        results.append(f"Missing columns in file 2: {missing_cols}")
    if extra_cols:
        results.append(f"Extra columns in file 2: {extra_cols}")
    
    # Check column order
    order_differences = compare_column_order(df1, df2)
    if order_differences:
        results.append("\nColumn Order Differences:")
        for diff in order_differences:
            results.append(f"  {diff}")
    
    # Data Type Consistency Check
    results.append("\n=== DATA TYPE CONSISTENCY ===")
    for col in common_cols:
        dtype1 = df1[col].dtype
        dtype2 = df2[col].dtype
        if dtype1 != dtype2:
            results.append(f"Data type mismatch in column '{col}': File1={dtype1}, File2={dtype2}")
    
    # Null Value Analysis
    results.append("\n=== NULL VALUE ANALYSIS ===")
    for col in common_cols:
        nulls1 = df1[col].isna().sum()
        nulls2 = df2[col].isna().sum()
        empty1 = (df1[col] == '').sum() if df1[col].dtype == 'object' else 0
        empty2 = (df2[col] == '').sum() if df2[col].dtype == 'object' else 0
        
        if nulls1 != nulls2 or empty1 != empty2:
            results.append(f"Null/Empty value mismatch in '{col}':")
            results.append(f"  File1: {nulls1} nulls, {empty1} empty strings")
            results.append(f"  File2: {nulls2} nulls, {empty2} empty strings")
    
    # Format Consistency Check
    results.append("\n=== FORMAT CONSISTENCY ===")
    for col in common_cols:
        try:
            if df1[col].dtype == 'object' and df2[col].dtype == 'object':
                # Convert to string and strip for comparison
                s1 = df1[col].astype(str).reset_index(drop=True)
                s2 = df2[col].astype(str).reset_index(drop=True)
                
                # Check for leading/trailing spaces
                spaces1 = (s1.str.len() != s1.str.strip().str.len()).sum()
                spaces2 = (s2.str.len() != s2.str.strip().str.len()).sum()
                if spaces1 != spaces2:
                    results.append(f"Leading/trailing space differences in '{col}':")
                    results.append(f"  File1: {spaces1} values with extra spaces")
                    results.append(f"  File2: {spaces2} values with extra spaces")
                
                # Case sensitivity check - compare values directly
                case_diff = string_compare(s1.str.lower(), s2.str.lower())
                if case_diff > 0:
                    results.append(f"Case sensitivity differences in '{col}': {case_diff} mismatches")
        except Exception as e:
            print(f"Error in format consistency check for column {col}: {str(e)}")
            continue
    
    # Value Comparison with Record Identification
    results.append("\n=== VALUE COMPARISON ===")
    try:
        value_differences = compare_values_with_identification(df1, df2, common_cols, key_columns=KEY_COLUMNS)
        results.extend(value_differences)
    except Exception as e:
        results.append(f"Error comparing values: {str(e)}")
    
    # Statistical Comparison for Numeric Columns
    results.append("\n=== STATISTICAL COMPARISON ===")
    for col in common_cols:
        try:
            if pd.api.types.is_numeric_dtype(df1[col]) and pd.api.types.is_numeric_dtype(df2[col]):
                stats1 = df1[col].describe()
                stats2 = df2[col].describe()
                
                if not np.allclose(stats1, stats2, rtol=1e-05, equal_nan=True):
                    results.append(f"\nStatistical differences in column '{col}':")
                    results.append(f"  File1: mean={stats1['mean']:.2f}, median={stats1['50%']:.2f}")
                    results.append(f"  File 2: mean={stats2['mean']:.2f}, median={stats2['50%']:.2f}")
        except Exception as e:
            print(f"Error in statistical comparison for column {col}: {str(e)}")
            continue
    
    try:
        # Find duplicates and missing records
        error_records = find_duplicates_and_missing(df1, df2, key_columns=KEY_COLUMNS)
        
        # Add error records summary to results
        if not error_records.empty:
            results.append("\n=== ERROR RECORDS SUMMARY ===")
            results.append(f"Total error records found: {len(error_records)}")
            
            # Count each type of error
            full_duplicates = error_records[error_records['error_type'] == 'f']
            duplicates = error_records[error_records['error_type'] == 'k']
            missing = error_records[error_records['error_type'] == 'm']
            extra = error_records[error_records['error_type'] == 'e']
            
            if not full_duplicates.empty:
                results.append(f"\nFull-row duplicates ({len(full_duplicates)}):")
                for _, row in full_duplicates.iterrows():
                    record_id = get_record_identifier(row)
                    results.append(f"  - {record_id} in {row['source_file']} (appears {row['num_errors']} times)")
            
            if not duplicates.empty:
                results.append(f"\nKey-based duplicates ({len(duplicates)}):")
                for _, row in duplicates.iterrows():
                    record_id = get_record_identifier(row)
                    results.append(f"  - {record_id} in {row['source_file']} (appears {row['num_errors']} times)")
            
            if not missing.empty:
                results.append(f"\nMissing records in target ({len(missing)}):")
                for _, row in missing.iterrows():
                    record_id = get_record_identifier(row)
                    results.append(f"  - {record_id}")
            
            if not extra.empty:
                results.append(f"\nExtra records in target ({len(extra)}):")
                for _, row in extra.iterrows():
                    record_id = get_record_identifier(row)
                    results.append(f"  - {record_id}")
    
    except Exception as e:
        print(f"Detailed error in duplicate/missing record detection: {str(e)}")
        results.append("\n=== ERROR FINDING DUPLICATES/MISSING RECORDS ===")
        results.append(f"Error: {str(e)}")
        error_records = None
    
    # Value Distribution Analysis (moved to end)
    results.append("\n=== VALUE DISTRIBUTION ===")
    for col in common_cols:
        try:
            unique1 = df1[col].nunique()
            unique2 = df2[col].nunique()
            if unique1 != unique2:
                results.append(f"Different number of unique values in '{col}':")
                results.append(f"  File1: {unique1} unique values")
                results.append(f"  File2: {unique2} unique values")
        except Exception as e:
            print(f"Error in value distribution analysis for column {col}: {str(e)}")
            continue
    
    return "\n".join(results), error_records

def find_csv_files():
    """Find CSV files in the specified directory; If can't find 2 files, raise an error"""
    if not os.path.exists(CSV_DIR):
        raise FileNotFoundError(f"Directory not found: {CSV_DIR}")
    
    # Get all CSV files, excluding directories
    csv_files = [f for f in glob.glob(os.path.join(CSV_DIR, "*.csv")) 
                 if os.path.isfile(f)]
    
    # Filter out any non-CSV files that might have been included
    csv_files = [f for f in csv_files if f.lower().endswith('.csv')]
    
    if len(csv_files) != 2:
        raise ValueError(
            f"Expected exactly 2 CSV files in {CSV_DIR}, but found {len(csv_files)}.\n"
            f"CSV files found: {[os.path.basename(f) for f in csv_files]}\n"
            "Please ensure exactly two CSV files are present in the directory."
        )
    
    # Sort to ensure consistent ordering and print found files
    csv_files = sorted(csv_files)
    print(f"\nFound CSV files:")
    for file in csv_files:
        print(f"- {os.path.basename(file)}")
    print()
    
    return csv_files

def main():
    try:
        # Find CSV files
        print(f"Looking for CSV files in: {CSV_DIR}")
        file1, file2 = find_csv_files()
        
        # Get output filenames
        results_filename, errors_filename = get_output_filenames(file1, file2)
        
        # Compare files
        print("\nStarting comparison...")
        result, error_records = enhanced_csv_comparison(file1, file2)
        
        # Save comparison results to text file
        output_text_file = os.path.join(CSV_DIR, results_filename)
        with open(output_text_file, "w") as f:
            f.write(result)
        
        # Save error records to CSV if any were found
        if error_records is not None and not error_records.empty:
            output_csv_file = os.path.join(CSV_DIR, errors_filename)
            error_records.to_csv(output_csv_file, index=False)
            print(f"\nError records have been saved to: {output_csv_file}")
        
        print("\nComparison Results:")
        print(result)
        print(f"\nDetailed results have been saved to: {output_text_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 
