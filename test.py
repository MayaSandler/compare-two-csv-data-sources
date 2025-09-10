# ================================================================
# SNOWFLAKE TABLE COMPARISON TOOL
# ================================================================
# Complete script for comparing two Snowflake tables.
# 
# USAGE:
# 1. Copy this entire file into a Snowflake Python worksheet
# 2. Update the configuration section below  
# 3. Click Run

import snowflake.snowpark as sp
from snowflake.snowpark.functions import col, count, avg, median, min as min_func, max as max_func

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

    KEY_COLUMNS = ['employee_id']  # The column(s) that uniquely identify each record
    # ===============================================
    
    print("SNOWFLAKE TABLE COMPARISON TOOL")
    print("=" * 50)
    
    # Build table names from configuration
    TABLE1 = f'"{TABLE1_CONFIG["database"]}"."{TABLE1_CONFIG["schema"]}"."{TABLE1_CONFIG["table"]}"'
    TABLE2 = f'"{TABLE2_CONFIG["database"]}"."{TABLE2_CONFIG["schema"]}"."{TABLE2_CONFIG["table"]}"'
    
    print(f"Comparing tables:")
    print(f"Table 1: {TABLE1}")
    print(f"Table 2: {TABLE2}")
    print(f"Key columns: {KEY_COLUMNS}")
    print("-" * 50)



    try:
        # Read tables
        print("Reading tables...")
        df1 = session.table(TABLE1)
        df2 = session.table(TABLE2)
        print("Tables loaded successfully")

        # Basic record count comparison
        print("\n=== BASIC COMPARISON ===")
        count1 = df1.count()
        count2 = df2.count()
        print(f"Table 1 records: {count1:,}")
        print(f"Table 2 records: {count2:,}")
        print(f"Records match: {'YES' if count1 == count2 else 'NO'}")

        # Column analysis
        print("\n=== COLUMN ANALYSIS ===")
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)
        common_cols = cols1.intersection(cols2)
        missing_cols = cols1 - cols2
        extra_cols = cols2 - cols1
        
        print(f"Common columns: {len(common_cols)}")
        if missing_cols:
            print(f"Missing in Table 2: {missing_cols}")
        if extra_cols:
            print(f"Extra in Table 2: {extra_cols}")
        if not missing_cols and not extra_cols:
            print("All columns match perfectly!")

        # Duplicate analysis
        print("\n=== DUPLICATE ANALYSIS ===")
        if KEY_COLUMNS:
            try:
                dup1 = df1.group_by(*KEY_COLUMNS).agg(count("*").alias("count")).filter(col("count") > 1)
                dup2 = df2.group_by(*KEY_COLUMNS).agg(count("*").alias("count")).filter(col("count") > 1)
                dup1_count = dup1.count()
                dup2_count = dup2.count()
                
                print(f"Duplicate groups in Table 1: {dup1_count}")
                print(f"Duplicate groups in Table 2: {dup2_count}")
                
                if dup1_count > 0:
                    print("Sample duplicates in Table 1:")
                    dup1.limit(5).show()
                else:
                    print("No duplicates found in Table 1")
                
                if dup2_count > 0:
                    print("Sample duplicates in Table 2:")
                    dup2.limit(5).show()
                else:
                    print("No duplicates found in Table 2")
        
    except Exception as e:
                print(f"Error in duplicate analysis: {e}")
        else:
            print("No key columns specified for duplicate analysis")

        # Missing records analysis - ONLY based on key columns
        print("\n=== MISSING RECORDS ANALYSIS ===")
        print(f"Analyzing based ONLY on key columns: {KEY_COLUMNS}")
        
        # Initialize variables
        missing_count = 0
        extra_count = 0
        different_values_count = 0
        missing_in_2 = None
        extra_in_2 = None
        different_records_table1 = None
        different_records_table2 = None
        
        try:
            # Records with keys in table1 not in table2 (truly missing records)
            df1_keys = df1.select(*KEY_COLUMNS).distinct()
            df2_keys = df2.select(*KEY_COLUMNS).distinct()
            
            # Debug: Show total distinct keys in each table
            df1_keys_count = df1_keys.count()
            df2_keys_count = df2_keys.count()
            print(f"DEBUG: Distinct key combinations in Table 1: {df1_keys_count:,}")
            print(f"DEBUG: Distinct key combinations in Table 2: {df2_keys_count:,}")
            
            # Use left_anti join instead of subtract for more reliable results
            missing_keys = df1_keys.join(df2_keys, KEY_COLUMNS, "left_anti")
            extra_keys = df2_keys.join(df1_keys, KEY_COLUMNS, "left_anti")
            
            missing_count = missing_keys.count()
            extra_count = extra_keys.count()
            
            print(f"Key combinations in Table 1 missing from Table 2: {missing_count:,}")
            print(f"Key combinations in Table 2 missing from Table 1: {extra_count:,}")
            
            # Debug: Show some sample keys from each table
            if df1_keys_count > 0:
                print("DEBUG: Sample keys from Table 1:")
                df1_keys.limit(3).show()
            if df2_keys_count > 0:
                print("DEBUG: Sample keys from Table 2:")
                df2_keys.limit(3).show()
                
            # If we're still getting false positives, let's debug the missing keys
            if missing_count > 0:
                print("DEBUG: Sample missing keys:")
                missing_keys.limit(3).show()
            if extra_count > 0:
                print("DEBUG: Sample extra keys:")
                extra_keys.limit(3).show()
            
            # Get full records for missing keys
            if missing_count > 0:
                missing_in_2 = df1.join(missing_keys, KEY_COLUMNS, "inner")
                print("Sample missing records (Table 1 -> Table 2):")
                missing_in_2.limit(5).show()
            else:
                print("No missing key combinations from Table 1")
                missing_in_2 = None
                
            if extra_count > 0:
                extra_in_2 = df2.join(extra_keys, KEY_COLUMNS, "inner")
                print("Sample extra records (Table 2 -> Table 1):")
                extra_in_2.limit(5).show()
        else:
                print("No extra key combinations in Table 2")
                extra_in_2 = None
            
            # Records with different values in ANY common columns (including keys)
            print(f"Analyzing differences in ALL common columns: {list(common_cols)}")
            
            if common_cols:
                # Select only common columns for comparison (including keys)
                common_cols_list = list(common_cols)
                df1_common = df1.select(*common_cols_list)
                df2_common = df2.select(*common_cols_list)
                
                # Find identical records (same values in ALL common columns)
                identical_records = df1_common.intersect(df2_common)
                identical_count = identical_records.count()
                
                # Total records that could potentially match (use the smaller table count)
                total_records_t1 = df1_common.count()
                total_records_t2 = df2_common.count()
                
                # Calculate different records
                different_t1_count = total_records_t1 - identical_count
                different_t2_count = total_records_t2 - identical_count
                
                print(f"Records identical in ALL common columns: {identical_count:,}")
                print(f"Records in Table 1 with different values: {different_t1_count:,}")
                print(f"Records in Table 2 with different values: {different_t2_count:,}")
                
                # Always create the different values analysis, even if counts are 0
                # Find records that are different (subtract identical ones)
                different_records_table1 = df1_common.subtract(identical_records)
                different_records_table2 = df2_common.subtract(identical_records)
                
                different_values_count = max(different_t1_count, different_t2_count)
                
                if different_values_count > 0:
                    print("Sample records with different values (Table 1 version):")
                    different_records_table1.limit(5).show()
                    print("Sample records with different values (Table 2 version):")
                    different_records_table2.limit(5).show()
                else:
                    print("All records are identical in common columns")
            else:
                print("No common columns found for comparison")
                different_records_table1 = None
                different_records_table2 = None
                different_values_count = 0
        
    except Exception as e:
            print(f"Error in missing records analysis: {e}")


        # Final summary
        print("\n" + "=" * 50)
        print("COMPARISON COMPLETE!")
        print("=" * 50)
        
        # Summary statistics
        total_issues = 0
        if count1 != count2:
            total_issues += abs(count1 - count2)
        if missing_cols or extra_cols:
            total_issues += len(missing_cols) + len(extra_cols)
            
        if total_issues == 0:
            print("PERFECT MATCH! Tables are identical.")
        else:
            print(f"Found {total_issues} differences between tables.")
        
        print(f"Final Summary: {count1:,} vs {count2:,} records")
        
        # ================================================================
        # CREATE VIEWS WITH COMPARISON RESULTS
        # ================================================================
        try:
            print("\nCreating result views...")
            
            # Create view with detailed comparison summary
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
            
            # Create DataFrame and save as view
            detailed_df = session.create_dataframe(detailed_summary_data, schema=["METRIC", "VALUE"])
            detailed_df.create_or_replace_view("DEV_SILVER.DQ.DQ_COMPARISON_SUMMARY_VIEW")
            print("Created view: DQ_COMPARISON_SUMMARY_VIEW")
            
            # Create view with missing records (always create, even if empty)
            if 'missing_in_2' in locals() and missing_in_2 is not None:
                try:
                    missing_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW")
                    print(f"Created view: DQ_MISSING_RECORDS_VIEW ({missing_count:,} records)")
                except Exception as e:
                    print(f"Could not create DQ_MISSING_RECORDS_VIEW: {e}")
            else:
                # Create empty view if no missing records analysis was done
                try:
                    empty_df = session.create_dataframe([], schema=df1.schema)
                    empty_df.create_or_replace_view("DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW")
                    print("Created view: DQ_MISSING_RECORDS_VIEW (0 records)")
                except Exception as e:
                    print(f"Could not create empty DQ_MISSING_RECORDS_VIEW: {e}")
            
            # Create view with extra records (always create, even if empty)  
            if 'extra_in_2' in locals() and extra_in_2 is not None:
                try:
                    extra_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW")
                    print(f"Created view: DQ_EXTRA_RECORDS_VIEW ({extra_count:,} records)")
                except Exception as e:
                    print(f"Could not create DQ_EXTRA_RECORDS_VIEW: {e}")
            else:
                # Create empty view if no extra records analysis was done
                try:
                    empty_df = session.create_dataframe([], schema=df2.schema)
                    empty_df.create_or_replace_view("DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW")
                    print("Created view: DQ_EXTRA_RECORDS_VIEW (0 records)")
                except Exception as e:
                    print(f"Could not create empty DQ_EXTRA_RECORDS_VIEW: {e}")
                
            # Create view with records that have different values (always create, even if empty)
            if 'different_records_table1' in locals() and different_records_table1 is not None:
                try:
                    different_records_table1.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW")
                    record_count = different_records_table1.count()
                    print(f"Created view: DQ_DIFFERENT_VALUES_T1_VIEW ({record_count:,} records)")
        except Exception as e:
                    print(f"Could not create DQ_DIFFERENT_VALUES_T1_VIEW: {e}")
    
            if 'different_records_table2' in locals() and different_records_table2 is not None:
    try:
                    different_records_table2.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T2_VIEW")
                    record_count = different_records_table2.count()
                    print(f"Created view: DQ_DIFFERENT_VALUES_T2_VIEW ({record_count:,} records)")
    except Exception as e:
                    print(f"Could not create DQ_DIFFERENT_VALUES_T2_VIEW: {e}")
                
            # Create view with duplicate records from Table 1 (always create, even if empty)
            if 'dup1' in locals() and 'dup1_count' in locals():
                try:
                    dup1.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW")
                    print(f"Created view: DQ_TABLE1_DUPLICATES_VIEW ({dup1_count:,} records)")
        except Exception as e:
                    print(f"Could not create DQ_TABLE1_DUPLICATES_VIEW: {e}")
            else:
                # Create empty duplicate view
                try:
                    empty_dup_df = session.create_dataframe([], schema=["KEY_COLUMN", "COUNT"])
                    empty_dup_df.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW")
                    print("Created view: DQ_TABLE1_DUPLICATES_VIEW (0 records)")
    except Exception as e:
                    print(f"Could not create empty DQ_TABLE1_DUPLICATES_VIEW: {e}")
                
            # Create view with duplicate records from Table 2 (always create, even if empty)
            if 'dup2' in locals() and 'dup2_count' in locals():
                try:
                    dup2.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE2_DUPLICATES_VIEW") 
                    print(f"Created view: DQ_TABLE2_DUPLICATES_VIEW ({dup2_count:,} records)")
        except Exception as e:
                    print(f"Could not create DQ_TABLE2_DUPLICATES_VIEW: {e}")
            else:
                # Create empty duplicate view
                try:
                    empty_dup_df = session.create_dataframe([], schema=["KEY_COLUMN", "COUNT"])
                    empty_dup_df.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE2_DUPLICATES_VIEW")
                    print("Created view: DQ_TABLE2_DUPLICATES_VIEW (0 records)")
                except Exception as e:
                    print(f"Could not create empty DQ_TABLE2_DUPLICATES_VIEW: {e}")
                
            print(f"\nAll views created successfully! You can now query:")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_COMPARISON_SUMMARY_VIEW;")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW;")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW;")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW;")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T2_VIEW;")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW;")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_TABLE2_DUPLICATES_VIEW;")
                
        except Exception as view_error:
            print(f"Warning: Could not create views: {view_error}")
        
        # Return a simple DataFrame with summary results for Snowflake worksheet
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
        print(f"FATAL ERROR: {str(e)}")
        # Return error as DataFrame
        error_data = [("ERROR", str(e))]
        return session.create_dataframe(error_data, schema=["STATUS", "MESSAGE"])
