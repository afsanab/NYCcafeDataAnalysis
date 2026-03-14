import pandas as pd
df = pd.read_csv("nyc_cafes_enriched.csv", on_bad_lines="skip")
df_clean = df.dropna(subset=["review_count"])
df_clean.to_csv("nyc_cafes_clean.csv", index=False)
print(f"Full dataset: {len(df)}")
print(f"Clean dataset: {len(df_clean)}")