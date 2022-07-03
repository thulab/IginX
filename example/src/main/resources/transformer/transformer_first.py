import pandas as pd

class FirstTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = []
        for col in df.columns:
            i = df[col].first_valid_index()
            ret.append(df[col][i])
        return ret.values.tolist()
