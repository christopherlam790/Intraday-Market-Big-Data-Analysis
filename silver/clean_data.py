import pandas as pd
import os
from pathlib import Path
import pyarrow as pq


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
@returns: list[Path] - List of files, alphabetically
"""
def list_files_alphabetically(folder_path: Path) -> list[Path]:
        
    paths = []
    
    # Access all parquet files
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        paths.append(file_path)
    
    return sorted(paths)






def clean_all_parquet_files(folder_path:str):    
    
    sorted_parquet_paths = list_files_alphabetically(folder_path=folder_path)

    print(sorted_parquet_paths)


if __name__ == "__main__":
    
    clean_all_parquet_files(folder_path="data/bronze/intraday_prices/")
    
    
    pass