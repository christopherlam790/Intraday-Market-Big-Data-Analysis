import pandas as pd
import os
from pathlib import Path
import pyarrow as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "intraday_prices"


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


"""
Attempt access of valid cols; Fails if col does not exist

@param:pd.DataFrame df - df to verify col exists in
@returns: None
"""
def verify_cols(df:pd.DataFrame) -> None:
    
    valid_cols = ['ID', 'TimeStamp', '/ES', '/NQ', '/RTY', 'SPY', 'QQQ', 'IWM']
    
    #Check cols exist
    for col in valid_cols:
        exists = df[col]
        
    return None




def clean_all_parquet_files(folder_path:str):    
    
    sorted_parquet_paths = list_files_alphabetically(folder_path=folder_path)

    ensure_dir(SILVER_ROOT)
    
    
    
    for parquet in sorted_parquet_paths:

        df = pd.read_parquet(parquet)
        
        
        # Verify Cols exist; Handle otherwise
        try:
            verify_cols(df=df)
        except:
            
            #Handles schema issue 1 (See README.md)
            if len(df.columns) > 1:
                print(parquet)
                                            
                # Step 1: Save current headers as first row
                header_row = pd.DataFrame([df.columns], columns=df.columns)

                # Step 2: Concatenate
                df = pd.concat([header_row, df], ignore_index=True)

                # Step 3: Let pandas auto-name columns (0, 1, 2, 3...)
                df.columns = range(len(df.columns))

                # Now you have numbered columns and old headers as first row
            
                df.rename(columns={0: 'ID',
                                1: 'TimeStamp',
                                2: '/ES',
                                3: '/NQ', 
                                4: '/RTY', 
                                5: 'SPY', 
                                6: 'QQQ', 
                                7: 'IWM'}, inplace=True)
                                
                
            else:
        
                print("EXCEPTION")
                
                


            
            

if __name__ == "__main__":
    
    clean_all_parquet_files(folder_path="data/bronze/intraday_prices/")
    
    
    pass