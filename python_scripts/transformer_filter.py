"""
The filter() function is used to subset rows or columns of dataframe according to labels in the specified index.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.filter(items=['High', 'Low'])  # (items=None, like=None, regex=None, axis=None)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
