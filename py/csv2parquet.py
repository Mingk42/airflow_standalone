import pandas as pd
import sys

df = pd.read_csv(f'/home/root2/data/csv/{sys.argv[1]}/count.csv',
                 on_bad_lines='skip',
                 encoding_errors='ignore',
                 names=['dt', 'cmd', 'cnt']
                )

df['dt']=df['dt'].str.replace('^','')
df['cmd']=df['cmd'].str.replace('^','')

df['cnt']=df['cnt'].str.replace('^','')

df['cnt'] = pd.to_numeric(df['cnt'], errors='coerce')  # coerce === NaN
df['cnt'] = df['cnt'].fillna(-1).astype(int)

# df.to_parquet(f'/home/root2/data/parquet/{sys.argv[1]}/count.parquet')
df.to_parquet('/home/root2/data/parquet', partition_cols=['dt'])
