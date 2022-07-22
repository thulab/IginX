"""
The filter() function is used to subset rows or columns of dataframe according to labels in the specified index.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        return df.filter(items=['High', 'Low']).values.tolist()  # (items=None, like=None, regex=None, axis=None)
