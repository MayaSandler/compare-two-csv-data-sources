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
    
    print("SNOWFLAKE TABLE COMPARISON TOOL")
    print("=" * 50)
    
    # Build table names from configuration
    TABLE1 = f'"{TABLE1_CONFIG["database"]}"."{TABLE1_CONFIG["schema"]}"."{TABLE1_CONFIG["table"]}"'
    TABLE2 = f'"{TABLE2_CONFIG["database"]}"."{TABLE2_CONFIG["schema"]}"."{TABLE2_CONFIG["table"]}"'
    
    print(f"Comparing tables:")
    print(f"   Table 1: {TABLE1}")
    print(f"   Table 2: {TABLE2}")
    print(f"   Key columns: {KEY_COLUMNS}")
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

        # Missing records analysis
        print("\n=== MISSING RECORDS ANALYSIS ===")
        try:
            # Records with keys in table1 not in table2 (truly missing records)
            missing_in_2 = df1.join(df2.select(*KEY_COLUMNS), KEY_COLUMNS, "left_anti")
            missing_count = missing_in_2.count()
            
            # Records with keys in table2 not in table1 (truly extra records)
            extra_in_2 = df2.join(df1.select(*KEY_COLUMNS), KEY_COLUMNS, "left_anti")
            extra_count = extra_in_2.count()
            
            print(f"Records with keys in Table 1 missing from Table 2: {missing_count:,}")
            print(f"Records with keys in Table 2 missing from Table 1: {extra_count:,}")
            
            # Records with same keys but different values in common columns
            different_values_count = 0
            if common_cols and len(common_cols) > len(KEY_COLUMNS):
                # Select only common columns for comparison
                common_cols_list = list(common_cols)
                df1_common = df1.select(*common_cols_list)
                df2_common = df2.select(*common_cols_list)
                
                # Find records that exist in both tables by key but have different values
                matching_keys = df1.join(df2.select(*KEY_COLUMNS), KEY_COLUMNS, "inner")
                matching_keys_count = matching_keys.count()
                
                # Compare full records with common columns only
                identical_records = df1_common.intersect(df2_common)
                identical_count = identical_records.count()
                
                different_values_count = matching_keys_count - identical_count
                
                print(f"Records with same keys but different values: {different_values_count:,}")
                
                if different_values_count > 0:
                    # Create a view showing records with same keys but different values
                    # Get records from table1 that have matching keys but different values
                    df1_keys = df1.select(*KEY_COLUMNS)
                    df2_keys = df2.select(*KEY_COLUMNS)
                    same_keys = df1_keys.intersect(df2_keys)
                    
                    # Records from table1 with same keys
                    records_with_same_keys_t1 = df1.join(same_keys, KEY_COLUMNS, "inner")
                    # Records from table2 with same keys  
                    records_with_same_keys_t2 = df2.join(same_keys, KEY_COLUMNS, "inner")
                    
                    # Find records that are different (subtract identical ones)
                    different_records_t1 = records_with_same_keys_t1.select(*common_cols_list).subtract(
                        records_with_same_keys_t2.select(*common_cols_list)
                    )
                    
                    different_records_t2 = records_with_same_keys_t2.select(*common_cols_list).subtract(
                        records_with_same_keys_t1.select(*common_cols_list)
                    )
                    
                    # Store these for view creation
                    different_records_table1 = different_records_t1
                    different_records_table2 = different_records_t2
                    
                    print("Sample records with different values (Table 1 version):")
                    different_records_t1.limit(5).show()
            
            if missing_count > 0:
                print("Sample missing records (Table 1 -> Table 2):")
                missing_in_2.limit(5).show()
            else:
                print("No missing records from Table 1")
                
            if extra_count > 0:
                print("Sample extra records (Table 2 -> Table 1):")
                extra_in_2.limit(5).show()
            else:
                print("No extra records in Table 2")
                
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
            if 'missing_in_2' in locals() and missing_count > 0:
                missing_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_MISSING_RECORDS_VIEW")
                print("Created view: DQ_MISSING_RECORDS_VIEW")
            
            # Create view with extra records (if any)  
            if 'extra_in_2' in locals() and extra_count > 0:
                extra_in_2.create_or_replace_view("DEV_SILVER.DQ.DQ_EXTRA_RECORDS_VIEW")
                print("Created view: DQ_EXTRA_RECORDS_VIEW")
                
            # Create view with records that have same keys but different values
            if 'different_records_table1' in locals():
                different_records_table1.create_or_replace_view("DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW")
                print("Created view: DQ_DIFFERENT_VALUES_T1_VIEW")
                
            if 'different_records_table2' in locals():
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
            if 'different_records_table1' in locals():
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T1_VIEW;")
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_DIFFERENT_VALUES_T2_VIEW;")
            if dup1_count > 0:
                print("- SELECT * FROM DEV_SILVER.DQ.DQ_TABLE1_DUPLICATES_VIEW;")
            if dup2_count > 0:
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
