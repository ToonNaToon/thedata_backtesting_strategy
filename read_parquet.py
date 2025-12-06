import pandas as pd
import ast

df = pd.read_parquet("/Users/sikalp/Desktop/QQQ_20211201.parquet")
# print(df.columns.tolist())


df["response"] = df["response"].apply(ast.literal_eval)
df_exploded = df.explode("data").reset_index(drop=True)
data_expanded = pd.json_normalize(df_exploded["data"])

# final_df = pd.concat(
#     [df_exploded.drop(columns=["data"]), data_expanded],
#     axis=1
# )

# # print(final_df.head())
# final_df["timestamp"] = pd.to_datetime(final_df["timestamp"])
# final_df["underlying_timestamp"] = pd.to_datetime(final_df["underlying_timestamp"])
# # final_df.info()
# final_df.to_csv("QQQ_20211201_FULL.csv", index=False)
# print("Saved clean file ✅")
