
import pandas as pd

df_main = pd.read_csv("usable_summary_final.csv")
df_main = df_main.drop(df_main.columns[[0, 1, 2, 3]], axis=1)
df_main = df_main.dropna(subset=['alt', 'uwnd', 'vwnd'])
df_main = df_main.reset_index()
