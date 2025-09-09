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
            # Records in table1 not in table2
            missing_in_2 = df1.join(df2.select(*KEY_COLUMNS), KEY_COLUMNS, "left_anti")
            missing_count = missing_in_2.count()
            
            # Records in table2 not in table1
            extra_in_2 = df2.join(df1.select(*KEY_COLUMNS), KEY_COLUMNS, "left_anti")
            extra_count = extra_in_2.count()
            
            print(f"Records in Table 1 missing from Table 2: {missing_count:,}")
            print(f"Records in Table 2 missing from Table 1: {extra_count:,}")
            
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
        
        # return f"Comparison completed: {count1:,} vs {count2:,} records"
        
        # Return a simple DataFrame with summary results for Snowflake worksheet
        summary_data = [
            ("TABLE1_RECORDS", str(count1)),
            ("TABLE2_RECORDS", str(count2)),
            ("RECORDS_MATCH", "YES" if count1 == count2 else "NO"),
            ("COMMON_COLUMNS", str(len(common_cols))),
            ("MISSING_COLUMNS_T2", str(len(missing_cols))),
            ("EXTRA_COLUMNS_T2", str(len(extra_cols))),
            ("TOTAL_ISSUES", str(total_issues)),
            ("STATUS", "PERFECT_MATCH" if total_issues == 0 else "DIFFERENCES_FOUND")
        ]
        return session.create_dataframe(summary_data, schema=["METRIC", "VALUE"])

    except Exception as e:
        print(f"FATAL ERROR: {str(e)}")
        # return f"Error: {str(e)}"
        
        # Return error as DataFrame
        error_data = [("ERROR", str(e))]
        return session.create_dataframe(error_data, schema=["STATUS", "MESSAGE"])
