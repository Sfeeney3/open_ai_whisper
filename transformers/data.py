import pandas as pd




data = pd.read_csv('output_list.txt', sep=" ", header=None)
data.columns = ["a", "b", "c", "etc."]