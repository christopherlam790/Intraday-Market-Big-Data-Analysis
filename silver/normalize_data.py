


import pandas as pd
import os
from pathlib import Path
import pyarrow as pq
import json
import polars as pl
from datetime import datetime


import sys


root_dir = Path(__file__).resolve().parent.parent
helper_path = str(root_dir / "helpers")

if helper_path not in sys.path:
    sys.path.append(helper_path)


import add_metadata



PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "normalize"
SILVER_META = PROJECT_ROOT / "data" / "silver" / "normalize_metadata"


VALID_COLS = ['ID', 'TimeStamp', '/ES', '/NQ', '/RTY', 'SPY', 'QQQ', 'IWM']

# ----------------------------
# Helpers
# ----------------------------

"""
Ensure directory of root is estavlished correctly

@param: Path path - Path of directory
@returns: None
"""
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)



"""
Get list of files, ordered alphabeticaly for cleaning

@param:Path folder_path - path to folder
@param:str substring - optional substring that file path must abide by 
@returns: list[Path] - List of files, alphabetically
"""
def list_files_alphabetically(folder_path: Path, substring:str = "") -> list[Path]:
        
    paths = []
    
    # Access all parquet files
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        if substring in file_path:
        
            paths.append(file_path)
    
    return sorted(paths)


def main(folder_path:str, substring: str = ""):
        
    sorted_parquet_paths = list_files_alphabetically(folder_path=folder_path, substring=substring)

    ensure_dir(SILVER_ROOT)

    for parquet in sorted_parquet_paths:
        
        df = pd.read_parquet(parquet)
        
        df = df[VALID_COLS]
        
        
        # --------------
        # Write parquets to folder w/ only necassary cols
        # --------------
        dataset_name = Path(parquet).stem   
        
        dataset_name = dataset_name.replace("_cleaning", "_normalized")
        
        if not os.path.isfile(f"{SILVER_ROOT}/{dataset_name}.parquet"):
            print(f"Adding parquet {parquet}")
        
            df.to_parquet(f"{SILVER_ROOT}/{dataset_name}.parquet", engine="pyarrow")
        
        
    # ---------------------
    # Switch to polars for efficiency, now that cols are normalized
    # ----------------------
    
    lazy_df = (
        pl.scan_parquet(SILVER_ROOT)
        .select(VALID_COLS)
    )

    df = lazy_df.collect()



    # -------------
    # Validate types
    # -------------

    for i in VALID_COLS:
        try:
            if i == "ID":
                assert(str(df[i].dtype) == "Int64")
            elif i == "TimeStamp":
                assert(str(df[i].dtype) == "String")
            else:
                assert(str(df[i].dtype) == "Float64")
        except:
            raise Exception("Error in data types in silver normalization")
       
       

    # -----------
    # Standardize timestamps
    # -----------

    # Define the regex patterns for the two 'messy' formats
    pattern_short = r"^\d{1,2}/\d{1,2}/\d{2} \d{2}:\d{2}$"            # 8/9/20 17:59
    pattern_medium = r"^\d{1,2}/\d{1,2}/\d{4} \d{2}:\d{2}:\d{2}$"    # 11/22/2022 18:45:39

    df_standardized = df.with_columns(
        pl.when(pl.col("TimeStamp").str.contains(pattern_short))
        .then(
            # Convert 8/9/20 17:59 -> 2020-08-09 17:59:00.000
            pl.col("TimeStamp").str.to_datetime("%m/%d/%y %H:%M", strict=False)
        )
        .when(pl.col("TimeStamp").str.contains(pattern_medium))
        .then(
            # Convert 11/22/2022 18:45:39 -> 2022-11-22 18:45:39.000
            pl.col("TimeStamp").str.to_datetime("%m/%d/%Y %H:%M:%S", strict=False)
        )
        .otherwise(
            # Keep the already 'correct' ones or parse them directly
            pl.col("TimeStamp").str.to_datetime("%Y-%m-%d %H:%M:%S%.3f", strict=False)
        )
        .dt.strftime("%Y-%m-%d %H:%M:%S.000") # Force back to your regex-conformant string
        .alias("TimeStamp")
    )
            

    # The 'Gold Standard' pattern: YYYY-MM-DD HH:MM:SS.mmm
    target_regex = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$"

    # Filter for anything that STILL doesn't match
    remaining_issues = df_standardized.filter(
        ~pl.col("TimeStamp").str.contains(target_regex)
    )

    print(f"Remaining non-conforming rows: {remaining_issues.height}")

    if remaining_issues.height > 0:
        print("Sample of stubborn rows:")
        print(remaining_issues.select("TimeStamp").unique().head(5))
        
        raise Exception("TimeStamp conformity issue")

        
    
        
        
    


       

    
if __name__ == "__main__":
    main(folder_path="data/silver/cleaning")