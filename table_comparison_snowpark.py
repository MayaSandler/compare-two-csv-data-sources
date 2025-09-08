import snowflake.snowpark as sp
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, isnan, isnull, count, sum as sum_func, min as min_func, max as max_func, avg, median, stddev, var_samp
from snowflake.snowpark.types import StringType, IntegerType, FloatType, DoubleType, BooleanType, DateType, TimestampType
import pandas as pd
import numpy as np
from datetime import datetime
import os
import json

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    "account": "your_account.region",  # e.g., "abc123.us-east-1"
    "user": "your_username",
    "password": "your_password",  # Consider using environment variables
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema",
    "role": "your_role"  # Optional
}

# Table configuration - specify the two tables to compare
TABLE1_CONFIG = {
    "database": "your_database",
    "schema": "your_schema", 
    "table": "table1_name"
}

TABLE2_CONFIG = {
    "database": "your_database",
    "schema": "your_schema",
    "table": "table2_name"
}

# Key columns for comparison (adjust based on your tables)
KEY_COLUMNS = ['employee_id']  # Define the key columns according to your dataset

def create_snowpark_session(config):
    """Create a Snowpark session with the provided configuration"""
    try:
        session = Session.builder.configs(config).create()
        return session
    except Exception as e:
        print(f"Error creating Snowpark session: {str(e)}")
        raise

def get_record_identifier_snowpark(row_dict, key_columns=None):
    """Get record identifier for Snowpark row data"""
    if key_columns is None:
        key_columns = KEY_COLUMNS
    
    try:
        # First try key columns
        key_values = [f"{col}={row_dict.get(col, 'NULL')}" for col in key_columns if col in row_dict]
        if key_values:
            return ", ".join(key_values)
        
        # Fallback to the first non-null column
        helper_cols = {'_merge_key', 'record_identifier', 'source_table', 'error_type', 'num_errors'}
        for col_name, value in row_dict.items():
            if col_name not in helper_cols and value is not None:
                return f"{col_name}={value}"
        
        return "Unknown record"
    except Exception as e:
        return "Error identifying record"

def read_snowflake_table(session, table_config):
    """Read a Snowflake table and return as Snowpark DataFrame"""
    try:
        database = table_config["database"]
        schema = table_config["schema"]
        table = table_config["table"]
        
        # Create fully qualified table name
        table_name = f'"{database}"."{schema}"."{table}"'
        
        # Read the table
        df = session.table(table_name)
        return df
    except Exception as e:
        print(f"Error reading table {table_config}: {str(e)}")
        raise

def find_duplicates_and_missing_snowpark(df1, df2, key_columns=None, session=None):
    """
    Analyzes Snowpark DataFrames for duplicates and missing records
    Returns a pandas DataFrame with error types marked
    """
    if key_columns is None:
        key_columns = KEY_COLUMNS
    
    try:
        # Add source table identifier
        df1_with_source = df1.with_column("source_table", col("'file1'"))
        df2_with_source = df2.with_column("source_table", col("'file2'"))
        
        # Detect full-row duplicates in each table
        # For Snowpark, we'll use group by all columns and count
        df1_cols = df1.columns
        df2_cols = df2.columns
        
        # Full duplicates detection
        full_dup1 = df1_with_source.group_by(*df1_cols).agg(count("*").alias("num_errors")).filter(col("num_errors") > 1)
        full_dup2 = df2_with_source.group_by(*df2_cols).agg(count("*").alias("num_errors")).filter(col("num_errors") > 1)
        
        # Add error type
        full_dup1 = full_dup1.with_column("error_type", col("'f'"))
        full_dup2 = full_dup2.with_column("error_type", col("'f'"))
        
        # Key-based duplicates detection
        key_dup1 = df1_with_source.group_by(*key_columns).agg(count("*").alias("num_errors")).filter(col("num_errors") > 1)
        key_dup2 = df2_with_source.group_by(*key_columns).agg(count("*").alias("num_errors")).filter(col("num_errors") > 1)
        
        # Add error type for key duplicates
        key_dup1 = key_dup1.with_column("error_type", col("'k'"))
        key_dup2 = key_dup2.with_column("error_type", col("'k'"))
        
        # Find missing records using anti-join
        # Records in df1 not in df2
        missing_in_2 = df1_with_source.join(df2_with_source.select(*key_columns), key_columns, "left_anti")
        missing_in_2 = missing_in_2.with_column("error_type", col("'m'"))
        missing_in_2 = missing_in_2.with_column("num_errors", col("1"))
        
        # Records in df2 not in df1
        extra_in_2 = df2_with_source.join(df1_with_source.select(*key_columns), key_columns, "left_anti")
        extra_in_2 = extra_in_2.with_column("error_type", col("'e'"))
        extra_in_2 = extra_in_2.with_column("num_errors", col("1"))
        
        # Union all error records
        error_records = full_dup1.union_all(full_dup2).union_all(key_dup1).union_all(key_dup2).union_all(missing_in_2).union_all(extra_in_2)
        
        # Convert to pandas for easier processing
        error_records_pd = error_records.to_pandas()
        
        # Sort by error_type and source_table
        error_records_pd = error_records_pd.sort_values(['error_type', 'source_table'])
        
        return error_records_pd
        
    except Exception as e:
        print(f"Error in find_duplicates_and_missing_snowpark: {str(e)}")
        raise

def compare_column_order_snowpark(df1, df2):
    """Compare column order between two Snowpark DataFrames"""
    cols1 = df1.columns
    cols2 = df2.columns
    common_cols = list(set(cols1) & set(cols2))
    order_diff = []
    
    for col_name in common_cols:
        pos1 = cols1.index(col_name) + 1  # Make it 1-based for readability
        pos2 = cols2.index(col_name) + 1
        if pos1 != pos2:
            order_diff.append(f"Column '{col_name}': position {pos1} in table1, position {pos2} in table2")
    
    return order_diff

def get_output_filenames(table1_config, table2_config):
    """Generate output filenames based on table names"""
    table1_name = table1_config["table"]
    table2_name = table2_config["table"]
    
    # Create suffix from both table names
    suffix = f"__{table1_name}_vs_{table2_name}"
    
    # Generate output filenames
    results_file = f"comparison_results{suffix}.txt"
    errors_file = f"error_records{suffix}.csv"
    
    return results_file, errors_file

def get_timestamp_header(table1_config, table2_config):
    """Generate header with timestamp and table information"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    header = [
        "=== COMPARISON DETAILS ===",
        f"Comparison performed on: {current_time}",
        f"Table 1: {table1_config['database']}.{table1_config['schema']}.{table1_config['table']}",
        f"Table 2: {table2_config['database']}.{table2_config['schema']}.{table2_config['table']}",
        "\n"  # Add blank line after header
    ]
    return "\n".join(header)

def compare_values_with_identification_snowpark(df1, df2, common_cols, key_columns=None, session=None):
    """Compare values between Snowpark DataFrames and return differences with record identification"""
    differences = []
    
    try:
        if key_columns is None:
            key_columns = KEY_COLUMNS
        
        # Ensure key_columns is a list
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        
        # Create merge keys
        if len(key_columns) == 1:
            df1_with_key = df1.with_column("_merge_key", col(key_columns[0]).cast(StringType()))
            df2_with_key = df2.with_column("_merge_key", col(key_columns[0]).cast(StringType()))
        else:
            # Concatenate multiple key columns
            key_expr = col(key_columns[0]).cast(StringType())
            for col_name in key_columns[1:]:
                key_expr = key_expr + "_" + col(col_name).cast(StringType())
            df1_with_key = df1.with_column("_merge_key", key_expr)
            df2_with_key = df2.with_column("_merge_key", key_expr)
        
        # Add record identifiers
        # For Snowpark, we'll create a simple identifier using key columns
        record_id_expr = col(key_columns[0]).cast(StringType())
        for col_name in key_columns[1:]:
            record_id_expr = record_id_expr + ", " + col(col_name).cast(StringType())
        
        df1_with_id = df1_with_key.with_column("record_identifier", record_id_expr)
        df2_with_id = df2_with_key.with_column("record_identifier", record_id_expr)
        
        # Merge dataframes
        merged = df1_with_id.join(df2_with_id, "_merge_key", "inner")
        
        # Compare values for each common column
        for col_name in common_cols:
            if col_name in key_columns:
                continue  # Skip key columns as they were used for merging
                
            col1 = f"{col_name}_1"
            col2 = f"{col_name}_2"
            
            # Find mismatches
            mismatches = merged.filter(col(col1).cast(StringType()) != col(col2).cast(StringType()))
            
            # Convert to pandas for detailed analysis
            mismatch_pd = mismatches.to_pandas()
            
            if not mismatch_pd.empty:
                differences.append(f"\nValue mismatches in column '{col_name}':")
                for _, row in mismatch_pd.iterrows():
                    differences.append(f"  - {row['record_identifier_1']}:")
                    differences.append(f"    Source: '{row[col1]}'")
                    differences.append(f"    Target: '{row[col2]}'")
        
        return differences
        
    except Exception as e:
        print(f"Detailed error in value comparison: {str(e)}")
        return [f"\nError comparing values: {str(e)}"]

def enhanced_snowpark_comparison(table1_config, table2_config, session):
    """
    Enhanced comparison of two Snowflake tables with data quality checks
    """
    try:
        # Read tables
        df1 = read_snowflake_table(session, table1_config)
        df2 = read_snowflake_table(session, table2_config)
    except Exception as e:
        return f"Error reading tables: {str(e)}", None
    
    results = []
    
    # Add timestamp and table information header
    results.append(get_timestamp_header(table1_config, table2_config))
    
    # Basic Record Count Check
    results.append("=== BASIC RECORD COUNT ===")
    count1 = df1.count()
    count2 = df2.count()
    records_match = count1 == count2
    results.append(f"Record Count Check: {'PASS' if records_match else 'FAIL'}")
    results.append(f"Table 1 records: {count1}")
    results.append(f"Table 2 records: {count2}")
    
    # Column Comparison
    results.append("\n=== COLUMN ANALYSIS ===")
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    common_cols = list(cols1.intersection(cols2))
    
    # Find missing or extra columns
    missing_cols = cols1 - cols2
    extra_cols = cols2 - cols1
    
    if missing_cols:
        results.append(f"Missing columns in table 2: {missing_cols}")
    if extra_cols:
        results.append(f"Extra columns in table 2: {extra_cols}")
    
    # Check column order
    order_differences = compare_column_order_snowpark(df1, df2)
    if order_differences:
        results.append("\nColumn Order Differences:")
        for diff in order_differences:
            results.append(f"  {diff}")
    
    # Data Type Consistency Check
    results.append("\n=== DATA TYPE CONSISTENCY ===")
    df1_schema = df1.schema
    df2_schema = df2.schema
    
    for col_name in common_cols:
        try:
            dtype1 = str(df1_schema[col_name].datatype)
            dtype2 = str(df2_schema[col_name].datatype)
            if dtype1 != dtype2:
                results.append(f"Data type mismatch in column '{col_name}': Table1={dtype1}, Table2={dtype2}")
        except KeyError:
            results.append(f"Column '{col_name}' not found in schema")
    
    # Null Value Analysis
    results.append("\n=== NULL VALUE ANALYSIS ===")
    for col_name in common_cols:
        try:
            nulls1 = df1.select(sum_func(when(isnull(col(col_name)), 1).otherwise(0)).alias("null_count")).collect()[0]["NULL_COUNT"]
            nulls2 = df2.select(sum_func(when(isnull(col(col_name)), 1).otherwise(0)).alias("null_count")).collect()[0]["NULL_COUNT"]
            
            if nulls1 != nulls2:
                results.append(f"Null value mismatch in '{col_name}':")
                results.append(f"  Table1: {nulls1} nulls")
                results.append(f"  Table2: {nulls2} nulls")
        except Exception as e:
            results.append(f"Error checking nulls in column '{col_name}': {str(e)}")
    
    # Value Comparison with Record Identification
    results.append("\n=== VALUE COMPARISON ===")
    try:
        value_differences = compare_values_with_identification_snowpark(df1, df2, common_cols, key_columns=KEY_COLUMNS, session=session)
        results.extend(value_differences)
    except Exception as e:
        results.append(f"Error comparing values: {str(e)}")
    
    # Statistical Comparison for Numeric Columns
    results.append("\n=== STATISTICAL COMPARISON ===")
    for col_name in common_cols:
        try:
            # Check if column is numeric by trying to get stats
            stats1 = df1.select(
                avg(col(col_name)).alias("mean"),
                median(col(col_name)).alias("median"),
                min_func(col(col_name)).alias("min"),
                max_func(col(col_name)).alias("max")
            ).collect()[0]
            
            stats2 = df2.select(
                avg(col(col_name)).alias("mean"),
                median(col(col_name)).alias("median"),
                min_func(col(col_name)).alias("min"),
                max_func(col(col_name)).alias("max")
            ).collect()[0]
            
            # Compare statistics
            if (abs(stats1["MEAN"] - stats2["MEAN"]) > 1e-5 or 
                abs(stats1["MEDIAN"] - stats2["MEDIAN"]) > 1e-5):
                results.append(f"\nStatistical differences in column '{col_name}':")
                results.append(f"  Table1: mean={stats1['MEAN']:.2f}, median={stats1['MEDIAN']:.2f}")
                results.append(f"  Table2: mean={stats2['MEAN']:.2f}, median={stats2['MEDIAN']:.2f}")
                
        except Exception as e:
            # Column might not be numeric, skip
            continue
    
    # Find duplicates and missing records
    try:
        error_records = find_duplicates_and_missing_snowpark(df1, df2, key_columns=KEY_COLUMNS, session=session)
        
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
                    record_id = get_record_identifier_snowpark(row.to_dict())
                    results.append(f"  - {record_id} in {row['source_table']} (appears {row['num_errors']} times)")
            
            if not duplicates.empty:
                results.append(f"\nKey-based duplicates ({len(duplicates)}):")
                for _, row in duplicates.iterrows():
                    record_id = get_record_identifier_snowpark(row.to_dict())
                    results.append(f"  - {record_id} in {row['source_table']} (appears {row['num_errors']} times)")
            
            if not missing.empty:
                results.append(f"\nMissing records in target ({len(missing)}):")
                for _, row in missing.iterrows():
                    record_id = get_record_identifier_snowpark(row.to_dict())
                    results.append(f"  - {record_id}")
            
            if not extra.empty:
                results.append(f"\nExtra records in target ({len(extra)}):")
                for _, row in extra.iterrows():
                    record_id = get_record_identifier_snowpark(row.to_dict())
                    results.append(f"  - {record_id}")
    
    except Exception as e:
        print(f"Detailed error in duplicate/missing record detection: {str(e)}")
        results.append("\n=== ERROR FINDING DUPLICATES/MISSING RECORDS ===")
        results.append(f"Error: {str(e)}")
        error_records = None
    
    # Value Distribution Analysis
    results.append("\n=== VALUE DISTRIBUTION ===")
    for col_name in common_cols:
        try:
            unique1 = df1.select(col_name).distinct().count()
            unique2 = df2.select(col_name).distinct().count()
            if unique1 != unique2:
                results.append(f"Different number of unique values in '{col_name}':")
                results.append(f"  Table1: {unique1} unique values")
                results.append(f"  Table2: {unique2} unique values")
        except Exception as e:
            results.append(f"Error in value distribution analysis for column '{col_name}': {str(e)}")
    
    return "\n".join(results), error_records

def main():
    """Main function to run the Snowpark comparison"""
    session = None
    try:
        # Create Snowpark session
        print("Connecting to Snowflake...")
        session = create_snowpark_session(SNOWFLAKE_CONFIG)
        print("Connected successfully!")
        
        # Get output filenames
        results_filename, errors_filename = get_output_filenames(TABLE1_CONFIG, TABLE2_CONFIG)
        
        # Compare tables
        print("\nStarting comparison...")
        result, error_records = enhanced_snowpark_comparison(TABLE1_CONFIG, TABLE2_CONFIG, session)
        
        # Save comparison results to text file
        output_text_file = results_filename
        with open(output_text_file, "w") as f:
            f.write(result)
        
        # Save error records to CSV if any were found
        if error_records is not None and not error_records.empty:
            error_records.to_csv(errors_filename, index=False)
            print(f"\nError records have been saved to: {errors_filename}")
        
        print("\nComparison Results:")
        print(result)
        print(f"\nDetailed results have been saved to: {output_text_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if session:
            session.close()
            print("\nSnowflake session closed.")

if __name__ == "__main__":
    main()
