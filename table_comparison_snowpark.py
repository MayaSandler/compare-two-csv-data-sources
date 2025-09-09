# Instructions: 
## 1. Copy this entire file into a Snowflake Python worksheet
## 2. Update the configuration section below  
## 3. Click Run

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
            
            # Records with same keys but different values in common columns
            different_values_count = 0
            different_records_table1 = None
            different_records_table2 = None
            
            if common_cols and len(common_cols) > len(KEY_COLUMNS):
                # Find keys that exist in both tables
                matching_keys = df1_keys.intersect(df2_keys)
                matching_keys_count = matching_keys.count()
                
                if matching_keys_count > 0:
                    print(f"Key combinations that exist in both tables: {matching_keys_count:,}")
                    
                    # Select only common columns for comparison (including keys)
                    common_cols_list = list(common_cols)
                    df1_common = df1.select(*common_cols_list)
                    df2_common = df2.select(*common_cols_list)
                    
                    # Find identical records (same keys AND same values in all common columns)
                    identical_records = df1_common.intersect(df2_common)
                    identical_count = identical_records.count()
                    
                    different_values_count = matching_keys_count - identical_count
                    
                    print(f"Records with same keys but different values in common columns: {different_values_count:,}")
                    
                    if different_values_count > 0:
                        # Get records with matching keys from both tables
                        records_with_matching_keys_t1 = df1.join(matching_keys, KEY_COLUMNS, "inner")
                        records_with_matching_keys_t2 = df2.join(matching_keys, KEY_COLUMNS, "inner")
                        
                        # Find records that are different (subtract identical ones)
                        different_records_table1 = records_with_matching_keys_t1.select(*common_cols_list).subtract(
                            records_with_matching_keys_t2.select(*common_cols_list)
                        )
                        
                        different_records_table2 = records_with_matching_keys_t2.select(*common_cols_list).subtract(
                            records_with_matching_keys_t1.select(*common_cols_list)
                        )
                        
                        print("Sample records with different values (Table 1 version):")
                        different_records_table1.limit(5).show()
                else:
                    print("No matching key combinations found between tables")
            else:
                print("No additional columns to compare beyond key columns")
                
        except Exception as e:
            print(f"Error in missing records analysis: {e}")

        # Statistical comparison for numeric columns
        print("\n=== STATISTICAL COMPARISON ===")
        stats_found = False
        for col_name in common_cols:
            try:
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
                
                # Check if there are significant differences
                if (abs(stats1["MEAN"] - stats2["MEAN"]) > 1e-5 or 
                    abs(stats1["MEDIAN"] - stats2["MEDIAN"]) > 1e-5):
                    print(f"Statistical differences in '{col_name}':")
                    print(f"   Table1: mean={stats1['MEAN']:.2f}, median={stats1['MEDIAN']:.2f}")
                    print(f"   Table2: mean={stats2['MEAN']:.2f}, median={stats2['MEDIAN']:.2f}")
                    stats_found = True
                    
            except Exception:
                # Column is not numeric, skip
                continue
        
        if not stats_found:
            print("No significant statistical differences found in numeric columns")

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
            
            # Create view with missing records (if any)
            if 'missing_in_2' in locals() and missing_in_2 is not None and missing_count > 0:
                missing_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW")
                print("Created view: DQ_MISSING_RECORDS_VIEW")
            
            # Create view with extra records (if any)  
            if 'extra_in_2' in locals() and extra_in_2 is not None and extra_count > 0:
                extra_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW")
                print("Created view: DQ_EXTRA_RECORDS_VIEW")
                
            # Create view with records that have same keys but different values
            if 'different_records_table1' in locals() and different_records_table1 is not None:
                different_records_table1.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW")
                print("Created view: DQ_DIFFERENT_VALUES_T1_VIEW")
                
            if 'different_records_table2' in locals() and different_records_table2 is not None:
                different_records_table2.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T2_VIEW")
                print("Created view: DQ_DIFFERENT_VALUES_T2_VIEW")
                
            # Create view with duplicate records from Table 1 (if any)
            if 'dup1' in locals() and dup1_count > 0:
                dup1.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW")
                print("Created view: DQ_TABLE1_DUPLICATES_VIEW")
                
            # Create view with duplicate records from Table 2 (if any)
            if 'dup2' in locals() and dup2_count > 0:
                dup2.create_or_replace_view("DEV_SILVER.DQ.DQ_TABLE2_DUPLICATES_VIEW") 
                print("Created view: DQ_TABLE2_DUPLICATES_VIEW")
                
            print(f"\nViews created successfully! You can now query:")
            print("- SELECT * FROM DEV_SILVER.DQ.DQ_COMPARISON_SUMMARY_VIEW;")
            if missing_count > 0:
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW;")
            if extra_count > 0:
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW;")
            if 'different_records_table1' in locals() and different_records_table1 is not None:
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW;")
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T2_VIEW;")
            if 'dup1_count' in locals() and dup1_count > 0:
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW;")
            if 'dup2_count' in locals() and dup2_count > 0:
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
