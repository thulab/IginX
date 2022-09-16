"""
mean() returns the average of non-null values in a specified column from each input table.
Mean function returns the average by dividing the sum of the values in the set by their number.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        # Average of each column using DataFrame.mean()
        df = pd.DataFrame(rows)
        df = df.mean(axis=0)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
