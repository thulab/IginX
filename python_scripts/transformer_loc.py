"""
Access a group of rows and columns by label(s) or a boolean array.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        return df.loc[1].values.tolist()  # return df.loc[row_ranking, column_ranking]
