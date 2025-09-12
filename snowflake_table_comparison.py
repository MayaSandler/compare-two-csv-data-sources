# ================================================================
# SNOWFLAKE TABLE COMPARISON TOOL
# ================================================================ 
# Do do:
# 1. Copy this entire file into a Snowflake Python worksheet
# 2. Update the configuration section below  
# 3. Click Run

import snowflake.snowpark as sp
from snowflake.snowpark.functions import col, count, max

def main(session: sp.Session):
    # ===== CONFIGURATION - UPDATE THESE VALUES =====
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

    KEY_COLUMNS = ['employee_id']  # The column(s) that uniquely identify each record. If multiple columns, use a comma separated list.
    # ===== END OF CONFIGURATION =====================
    
    # Table names from configuration values
    TABLE1 = f'"{TABLE1_CONFIG["database"]}"."{TABLE1_CONFIG["schema"]}"."{TABLE1_CONFIG["table"]}"'
    TABLE2 = f'"{TABLE2_CONFIG["database"]}"."{TABLE2_CONFIG["schema"]}"."{TABLE2_CONFIG["table"]}"'

    try:
        # Read tables
        df1 = session.table(TABLE1)
        df2 = session.table(TABLE2)

        # Initialize variables
        missing_count = 0
        extra_count = 0
        different_values_count = 0
        missing_in_2 = None
        extra_in_2 = None
        different_records_table1 = None
        different_records_table2 = None

        # Record count
        count1 = df1.count()
        count2 = df2.count()

        # Column-names analysis
        cols1 = set(df1.columns) 
        cols2 = set(df2.columns)
        common_cols = cols1.intersection(cols2)
        missing_cols = cols1 - cols2
        extra_cols = cols2 - cols1

        # Duplication analysis (aggragate by key columns and count the number of records)
        if KEY_COLUMNS:
            try:
                dup1 = df1.group_by(*KEY_COLUMNS).agg(count("*").alias("count")).filter(col("count") > 1)
                dup2 = df2.group_by(*KEY_COLUMNS).agg(count("*").alias("count")).filter(col("count") > 1)
                dup1_count = dup1.count()
                dup2_count = dup2.count()
            except Exception as e: # If there is an error, set the duplicate counts to -1 (for debugging purposes)
                dup1_count = -1
                dup2_count = -1
        else:  # If no key columns are specified, set the duplicate counts to -2 (for debugging purposes)
            dup1_count = -2
            dup2_count = -2 

        # Missing and extrarecords analysis (based on key columns match)        
        try:
            # Distinct records
            df1_keys = df1.select(*KEY_COLUMNS).distinct()
            df2_keys = df2.select(*KEY_COLUMNS).distinct()
            
            # Count distinct records
            df1_keys_count = df1_keys.count()
            df2_keys_count = df2_keys.count()
            
            missing_keys = df1_keys.join(df2_keys, KEY_COLUMNS, "left_anti")  # Records in table1 not in table2
            extra_keys = df2_keys.join(df1_keys, KEY_COLUMNS, "left_anti") # Records in table2 not in table1
           
            # Missing and extra records and count them
            if missing_keys.count() > 0: # Records in table1 not in table2
                missing_in_2 = df1.join(missing_keys, KEY_COLUMNS, "inner")
                missing_count = missing_in_2.count()
            else:
                missing_count = 0
                missing_in_2 = None
                
            if extra_keys.count() > 0: # Records in table2 not in table1
                extra_in_2 = df2.join(extra_keys, KEY_COLUMNS, "inner")
                extra_count = extra_in_2.count()
            else: 
                extra_count = 0
                extra_in_2 = None
            
            # Records with identical keys but different values in other (common) columns
            if common_cols and len(common_cols) > len(KEY_COLUMNS):
                # Select only common columns for comparison
                common_cols_list = list(common_cols)
                df1_common = df1.select(*common_cols_list)
                df2_common = df2.select(*common_cols_list)
                
                # Find identical records (same values in ALL common columns)
                identical_records = df1_common.intersect(df2_common)
                identical_count = identical_records.count()                
                total_records_t1 = df1_common.count()
                total_records_t2 = df2_common.count()
                
                # Calculate different records
                different_t1_count = total_records_t1 - identical_count
                different_t2_count = total_records_t2 - identical_count                
                different_records_table1 = df1_common.subtract(identical_records)
                different_records_table2 = df2_common.subtract(identical_records)
                different_values_count = max(different_t1_count, different_t2_count)
            else:
                different_records_table1 = None
                different_records_table2 = None
                different_values_count = 0
        
        except Exception as e:
            pass

        # total_issues count the difference in record number + missing and extra column 
        total_issues = 0
        if count1 != count2:
            total_issues += abs(count1 - count2)
        if missing_cols or extra_cols:
            total_issues += len(missing_cols) + len(extra_cols)
        
        # Create comparison views 
        try:
            # Create DQ_COMPARISON_SUMMARY_VIEW
            detailed_summary_data = [
                ("COMPARISON_TIMESTAMP", str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0])),
                ("TABLE1_NAME", TABLE1),
                ("TABLE2_NAME", TABLE2),
                ("TABLE1_RECORDS", str(count1)),
                ("TABLE2_RECORDS", str(count2)),
                ("RECORD_COUNT_MATCH", "YES" if count1 == count2 else "NO"),
                ("COMMON_COLUMNS", str(len(common_cols))),
                ("MISSING_COLUMNS_IN_T2", str(len(missing_cols))),
                ("EXTRA_COLUMNS_IN_T2", str(len(extra_cols))),
                ("COLUMN_STRUCTURE_MATCH", "YES" if not missing_cols and not extra_cols else "NO"),
                ("DUPLICATE_GROUPS_T1", str(dup1_count) if 'dup1_count' in locals() else "0"),
                ("DUPLICATE_GROUPS_T2", str(dup2_count) if 'dup2_count' in locals() else "0"),
                ("MISSING_RECORDS_COUNT", str(missing_count) if 'missing_count' in locals() else "0"),
                ("EXTRA_RECORDS_COUNT", str(extra_count) if 'extra_count' in locals() else "0"),
                ("DIFFERENT_VALUES_COUNT", str(different_values_count) if 'different_values_count' in locals() else "0"),
                ("TOTAL_ISSUES_FOUND", str(total_issues)),
                ("OVERALL_STATUS", "PERFECT_MATCH" if total_issues == 0 else "DIFFERENCES_FOUND")
            ]            
            detailed_df = session.create_dataframe(detailed_summary_data, schema=["METRIC", "VALUE"])
            detailed_df.create_or_replace_view("DEV_SILVER.DQ.DQ_COMPARISON_SUMMARY_VIEW")
            
            # Create DQ_MISSING_RECORDS_VIEW - records in table1 missing in table2
            if 'missing_in_2' in locals() and missing_in_2 is not None:
                try:
                    missing_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW")
                except Exception as e:
                    pass
            else:  #Create empty view
                try:
                    empty_df = session.create_dataframe([], schema=df1.schema)
                    empty_df.create_or_replace_view("DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW")
                except Exception as e:
                    pass
            
            # Create DQ_EXTRA_RECORDS_VIEW - records in table2 missing in table1
            if 'extra_in_2' in locals() and extra_in_2 is not None:
                try:
                    extra_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW")
                except Exception as e:
                    pass
            else:
                try: #Create empty view
                    empty_df = session.create_dataframe([], schema=df2.schema)
                    empty_df.create_or_replace_view("DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW")
                except Exception as e:
                    pass
                
            # Create DQ_DIFFERENT_VALUES_T1_VIEW - records with the same keys but that have different values in other columns
            if 'different_records_table1' in locals() and different_records_table1 is not None:
                try:
                    different_records_table1.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW")
                except Exception as e:
                    pass
            else: #Create empty view
                try:
                    empty_diff_df = session.create_dataframe([], schema=df1.schema)
                    empty_diff_df.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW")
                except Exception as e:
                    pass

            # Create DQ_DIFFERENT_VALUES_T2_VIEW - records with the same keys but that have different values in other columns  
            if 'different_records_table2' in locals() and different_records_table2 is not None:
                try:
                    different_records_table2.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T2_VIEW")
                except Exception as e:
                    pass
            else: # Create empty view
                try:
                    empty_diff_df = session.create_dataframe([], schema=df2.schema)
                    empty_diff_df.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T2_VIEW")
                except Exception as e:
                    pass

            # Create DQ_TABLE1_DUPLICATES_VIEW - duplicate records in Table 1 
            if 'dup1' in locals() and 'dup1_count' in locals() and dup1_count > 0: 
                try: 
                    dup1.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW")
                except Exception as e:
                    pass
            else: # Create empty view
                try:
                    empty_dup_df = dup1.limit(0) # Just header
                    empty_dup_df.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW")
                except Exception as e:
                    pass
                
            # Create DQ_TABLE2_DUPLICATES_VIEW - duplicate records in Table 2
            if 'dup2' in locals() and 'dup2_count' in locals() and dup2_count > 0:
                try:
                    dup2.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE2_DUPLICATES_VIEW") 
                except Exception as e:
                    pass
            else: # Create empty view
                try:
                    empty_dup_df = dup2.limit(0) # Just header
                    empty_dup_df.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE2_DUPLICATES_VIEW")
                except Exception as e:
                    pass                
        except Exception as view_error:
            pass
        
        # Print as query results for a snapshot of the test
        summary_data = [
            ("TABLE1_RECORDS", str(count1)),
            ("TABLE2_RECORDS", str(count2)),
            ("RECORDS_MATCH", "YES" if count1 == count2 else "NO"),
            ("COMMON_COLUMNS", str(len(common_cols))),
            ("MISSING_COLUMNS_T2", str(len(missing_cols))),
            ("EXTRA_COLUMNS_T2", str(len(extra_cols))),
            ("MISSING_RECORDS", str(missing_count) if 'missing_count' in locals() else "0"),
            ("EXTRA_RECORDS", str(extra_count) if 'extra_count' in locals() else "0"),
            ("DIFFERENT_VALUES", str(different_values_count) if 'different_values_count' in locals() else "0"),
            ("TOTAL_ISSUES", str(total_issues)),
            ("STATUS", "PERFECT_MATCH" if total_issues == 0 else "DIFFERENCES_FOUND")
        ]
        
        return session.create_dataframe(summary_data, schema=["METRIC", "VALUE"])
    except Exception as e:
        error_data = [("ERROR", str(e))]
        return session.create_dataframe(error_data, schema=["STATUS", "MESSAGE"])