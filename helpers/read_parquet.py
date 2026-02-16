"""
Helper for reading parquete files
"""


import pandas as pd
from pathlib import Path

import os


def read_one_parquet(parquete_path: str) -> None:
    
    try:
        df = pd.read_parquet(parquete_path)
        
        print(df)
    except FileNotFoundError:
        print(f"File DNE on path: {parquete_path}")



def read_all_parquets_in_folder(folder_path: str) -> None:
    
    try:
    
        files = []
    
        for filename in os.listdir(folder_path):
            full_path = os.path.join(folder_path, filename) # Get the full path
            if os.path.isfile(full_path) and filename not in files: # Check if it is a file
                #print(filename)
                files.append(filename)
            else:
                print("ERR; File duplicate")
    
    except:
        raise Exception("Err in reading fodler")



if __name__ == "__main__":

    read_all_parquets_in_folder("data/silver/normalize")