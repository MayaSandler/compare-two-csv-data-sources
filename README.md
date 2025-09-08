# Data Comparison Tools

This repository contains *two* powerful data comparison tools for different use cases: 
1. CSV File Comparison Tool
2. Snowflake Table Comparison Tool

---

# 📄 CSV File Comparison Tool
This tool provides comprehensive comparison and analysis of two CSV files, checking for various data quality issues and differences.

**File:** `csv_comparison.py`  
**Purpose:** Compare two CSV files for data quality issues and differences

## Features
- Automatic detection of CSV files from the specified directory
- Record count comparison
- Column structure analysis
- Data type consistency checks
- Null value analysis
- Statistical comparison for numeric columns
- Full-row and key-based duplicate analysis with occurrence counts
- Format consistency checks (whitespace, case sensitivity)
- Value comparison with clear formatting
- Value distribution analysis
- Column order independence (works with different column orders)
- Smart record identification with first-column/first-non-null fallback
- Single-place configuration for key columns via `KEY_COLUMNS`


## This tool performs the following Analysis checks

1. **Basic Checks**
   - Record count comparison
   - Column name matching
   - Column order analysis
   - Data type consistency

2. **Data Quality Checks**
   - Null value analysis
   - Empty string detection
   - Whitespace issues
   - Case sensitivity differences

3. **Value Analysis**
   - Detailed value comparisons with clear formatting
   - Special handling for text vs numeric values
   - Consolidated duplicate differences

4. **Statistical Analysis**
   - Mean and median comparison for numeric columns
   - Unique value counts
   - Distribution analysis

5. **Error Tracking**
   - Identifies full-row duplicates ('f') and key-based duplicates ('k') with occurrence counts
   - Finds records missing from target file ('m')
   - Detects extra records in target file ('e')
   - Maintains full record context in error reporting


## Multiple Types Of Error Detection

1. **Full-row Duplicates** (marked as 'f')
   - Rows that are identical across all columns within a single file
   - Reported with the source file and occurrence count
   - Consolidated in output (one entry with count)

2. **Key-based Duplicates** (marked as 'k')
   - Multiple rows sharing the same `KEY_COLUMNS` within a single file
   - These are distinct from full-row duplicates (values may differ in other columns)
   - Reported with the source file and occurrence count

3. **Missing Records** (marked as 'm')
   - Records present in source file but missing in target (by `KEY_COLUMNS`)

4. **Extra Records** (marked as 'e')
   - Records present in target file but not in source (by `KEY_COLUMNS`)

5. **Value comparison**
  - Quoted text values for clear whitespace visibility
  - Raw numeric values without quotes
  - Record identification for each mismatch
  - Consolidated duplicate differences (shown only once)
  - Special handling for different data types

    
## Output Files

1. **Console/Terminal Output**
   - Real-time comparison progress and summary, as well as visibility of any issues found

2. **Comparison Results Text File** (`comparison_results__file1_vs_file2.txt`)
   - Detailed analysis in logical section order, saved in the same directory as input files:
     1. Comparison Details (with timestamp)
     2. Basic Record Count
     3. Column Analysis
     4. Data Type Consistency
     5. Null Value Analysis
     6. Format Consistency
     7. Value Comparison
     8. Statistical Comparison
     9. Error Records Summary
     10. Value Distribution
  
3. **Error Records CSV File** (`error_records__file1_vs_file2.csv`)
   - Only created if errors are found, and include only the problematic records
   - Consolidates duplicate records (one entry with count) for easy reading
   - Includes additional columns to indicate the issues:
     - `source_file`: Indicates which file the record came from ('file1' or 'file2')
     - `error_type`: Type of error found:
       - 'f' = Full-row duplicate (all columns identical)
       - 'k' = Key-based duplicate (same `KEY_COLUMNS`)
       - 'm' = Missing record (not in target)
       - 'e' = Extra record (only in target)
     - `num_errors`: Count of occurrences for duplicate records

## Example analysis and result files
- Files for analysis:
   - 'example__employees_source.csv' - example file of employee data as a source/file 1 for the comparison
   - 'example_file2__employees_target.csv' - example file of employee data as a target/file 2 for the comparison
- Analysis result files:
   - 'comparison_results__employees_source_vs_employees_target.txt' - a categorized analysis of the errors found in the file (see Output Files > 2)
   - 'error_records__employees_source_vs_employees_target.csv' - the records that showed issued in the comparison analysis, their count of errors, and their error_type (i.e. key column duplication, missing, etc.)  (see Output Files > 3)

## Requirements
- Python 3.6+
- pandas
- numpy


## Installation
1. Clone this repository or download the files
2. Install the required packages:
```bash
pip install -r requirements.txt
```

## How to use this tool
1. Place the two CSV files that you want to compare a directory.
2. Update the `csv_comparison.py` with the directory that contains the two CSVs in this line in the script:
  - `CSV_DIR = os.path.expanduser("~/Desktop/compare_2_files")`
3. Define your unique key columns in one place at the top of `csv_comparison.py` in this line in the script:
  - `KEY_COLUMNS = ['employee_id']` (change as needed, e.g. `['emp_id', 'dob']`)
4. Record identifiers in outputs use `KEY_COLUMNS` first; if unavailable, they fall back to the first data column, then to the first non-null column.
5. Run the script in the terminal:
```bash
python csv_comparison.py
```

## Notes
- The script requires exactly two CSV files in the specified directory
- Works with different column orders between files
- Automatically handles different types of null values and empty strings
- For numeric comparisons, the tool uses numpy's isclose function to handle floating-point precision
- Special characters and whitespace are clearly shown in the output
- Results are automatically saved to both text and CSV files for easy analysis
- Error records maintain all original columns plus error tracking columns
- File names are included in output file names for easy tracking
- Comparison timestamp is included in the results
- Duplicate records are consolidated in the error output
- Text values are quoted in the output for clear whitespace visibility


# ❄️ Snowflake Table Comparison Tool
**File:** `table_comparison_snowpark.py`  
**Purpose:** Compare two Snowflake tables directly in a Snowflake worksheet using Snowpark. The features and the output of this script is similar to the csv comparison process. To execute this script, please follow section 'How to Run in Snowflake Worksheets'

## Output
- **Console output**: Real-time comparison results
- **Text file**: Detailed analysis report (`comparison_results__table1_vs_table2.txt`)
- **CSV file**: Error records with details (`error_records__table1_vs_table2.csv`)

## Features
- Record count comparison
- Column structure analysis  
- Duplicate detection (full-row and key-based)
- Missing/extra record identification
- Data type consistency checks
- Statistical comparison for numeric columns
- Value distribution analysis

## How to Run in Snowflake Worksheets

### Step 1: Configure table schemas and key columns in the Python script `table_comparison_snowpark.py` 

```python
# Update these values in table_comparison_snowpark.py
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

KEY_COLUMNS = ['employee_id']  # The column that uniquely identifies each record
```
### Step 2: In Snowflake
1. Go to **Worksheets** in your Snowflake account
2. Create a **new worksheet**

### Step 3: Copy This Code
Copy and paste this code into your worksheet:

```sql
EXECUTE IMMEDIATE $$
import snowflake.snowpark as sp
from snowflake.snowpark.functions import col, when, isnull, count

# Get active session
session = sp.get_active_session()

# Import configuration from the Python file
# This reads the TABLE1_CONFIG, TABLE2_CONFIG, and KEY_COLUMNS from table_comparison_snowpark.py
exec(open('table_comparison_snowpark.py').read().split('def create_snowpark_session')[0])

# Build table names from configuration
TABLE1 = f'"{TABLE1_CONFIG["database"]}"."{TABLE1_CONFIG["schema"]}"."{TABLE1_CONFIG["table"]}"'
TABLE2 = f'"{TABLE2_CONFIG["database"]}"."{TABLE2_CONFIG["schema"]}"."{TABLE2_CONFIG["table"]}"'

# Read tables
df1 = session.table(TABLE1)
df2 = session.table(TABLE2)

print("=== BASIC COMPARISON ===")
count1 = df1.count()
count2 = df2.count()
print(f"Table 1 records: {count1}")
print(f"Table 2 records: {count2}")
print(f"Records match: {count1 == count2}")

# Column analysis
cols1 = set(df1.columns)
cols2 = set(df2.columns)
print(f"\nCommon columns: {len(cols1.intersection(cols2))}")
print(f"Missing in Table 2: {cols1 - cols2}")
print(f"Extra in Table 2: {cols2 - cols1}")

# Duplicate analysis
if KEY_COLUMNS:
    dup1 = df1.group_by(*KEY_COLUMNS).agg(count("*").alias("count")).filter(col("count") > 1)
    dup2 = df2.group_by(*KEY_COLUMNS).agg(count("*").alias("count")).filter(col("count") > 1)
    print(f"\nDuplicates - Table 1: {dup1.count()}, Table 2: {dup2.count()}")

# Missing records
missing = df1.join(df2.select(*KEY_COLUMNS), KEY_COLUMNS, "left_anti")
extra = df2.join(df1.select(*KEY_COLUMNS), KEY_COLUMNS, "left_anti")
print(f"\nMissing records: {missing.count()}")
print(f"Extra records: {extra.count()}")

print("\n=== COMPARISON COMPLETE ===")
$$;
```


### Step 4: Run
Click **Run** in your Snowflake worksheet to see the comparison results.
**Note:** The worksheet script automatically reads your configuration from `table_comparison_snowpark.py`, so you only need to update the configuration in one place!

## Alternative: Run Python Script Locally

If you prefer to run the full comparison script locally:

### Prerequisites
```bash
pip install -r requirements_snowpark.txt
```

### Configuration
Update these variables in `table_comparison_snowpark.py`:
```python
SNOWFLAKE_CONFIG = {
    "account": "your_account.region",
    "user": "your_username", 
    "password": "your_password",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
}

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

KEY_COLUMNS = ['employee_id']
```

### Run
```bash
python table_comparison_snowpark.py
```
