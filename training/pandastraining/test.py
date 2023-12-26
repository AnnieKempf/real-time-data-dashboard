import pandas as pd

df = pd.read_csv('pandastraining/pokemon_data.csv')
for df in pd.read_csv('pandas/pokemon_data.csv', chunksize=5):
    print("CHUNK DF")
    print(df)

