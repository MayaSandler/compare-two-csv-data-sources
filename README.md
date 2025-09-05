# CSV File Comparison Tool
This tool provides comprehensive comparison and analysis of two CSV files, checking for various data quality issues and differences.


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
