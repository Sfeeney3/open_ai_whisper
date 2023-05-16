import pandas as pd


num=pd.read_csv(("./finance/data/2022q4/num.txt"), sep=' ', header=0, error_bad_lines=False)
pre=pd.read_csv(("./finance/data/2022q4/pre.txt"), sep=' ', header=0, error_bad_lines=False)
sub=pd.read_csv(("./finance/data/2022q4/sub.txt"), sep=' ', header=0, error_bad_lines=False)
tag=pd.read_csv(("./finance/data/2022q4/tag.txt"), sep=' ', header=0, error_bad_lines=False)



num.to_csv("./csv/num.csv")
pre.to_csv("./csv/pre.csv")
sub.to_csv("./csv/sub.csv")
tag.to_csv("./csv/tag.csv")