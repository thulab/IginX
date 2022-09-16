"""
Access a group of rows and columns by label(s) or a boolean array.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.loc[1]  # return df.loc[row_ranking, column_ranking]
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
