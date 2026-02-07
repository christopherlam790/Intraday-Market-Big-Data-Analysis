import kagglehub
from dotenv import load_dotenv
import os


load_dotenv() 


# Download latest version
path = kagglehub.dataset_download(os.getenv("KAGGLE_PATH"))

print("Path to dataset files:", path)