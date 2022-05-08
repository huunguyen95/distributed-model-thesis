import os
import pandas as pd
import shutil


data = pd.read_csv("data/embeddings_ae_before.csv")
os.mkdir("../server_data/test_data")
for row in data.values:
    if os.path.exists(f"../server_data/benign/{row[0]}.adjlist"):
        shutil.copyfile(f"../server_data/benign/{row[0]}.adjlist", f"../server_data/test_data/{row[0]}.adjlist")
    if os.path.exists(f"../server_data/malware/{row[0]}.adjlist"):
        shutil.copyfile(f"../server_data/malware/{row[0]}.adjlist", f"../server_data/test_data/{row[0]}.adjlist")
    if os.path.exists(f"../server_data/new_malware/{row[0]}.adjlist"):
        shutil.copyfile(f"../server_data/new_malware/{row[0]}.adjlist", f"../server_data/test_data/{row[0]}.adjlist")