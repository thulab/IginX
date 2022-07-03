import pandas as pd

class LastTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = []
        for col in df.columns:
            i = df[col].last_valid_index()
            ret.append(df[col][i])
        return ret.values.tolist()
