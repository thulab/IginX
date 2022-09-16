"""
Return the first n rows.
This function returns the first n rows for the object based on position.
It is useful for quickly testing if your object has the right type of data in it.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.head(n=5)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
