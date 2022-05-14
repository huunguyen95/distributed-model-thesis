import os
import pandas as pd
import shutil


data = pd.read_csv("data/embeddings.csv")
#os.mkdir("../server_data/test_data")
for row in data.values:
    if os.path.exists(f"../../agent/client_data/scg/data/benign/{row[0]}.adjlist"):
        shutil.copyfile(f"../../agent/client_data/scg/data/benign/{row[0]}.adjlist",
                        f"../server_data/test_dataset/train_dataset/{row[0]}.adjlist")
    if os.path.exists(f"../../agent/client_data/scg/data/malware/{row[0]}.adjlist"):
        shutil.copyfile(f"../../agent/client_data/scg/data/malware/{row[0]}.adjlist",
                        f"../server_data/test_dataset/train_dataset/{row[0]}.adjlist")
    if os.path.exists(f"../../agent/client_data/scg/data/new_malware/{row[0]}.adjlist"):
        shutil.copyfile(f"../../agent/client_data/scg/data/new_malware/{row[0]}.adjlist",
                        f"../server_data/test_dataset/train_dataset/{row[0]}.adjlist")
