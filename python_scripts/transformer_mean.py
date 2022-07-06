"""
mean() returns the average of non-null values in a specified column from each input table.
Mean function returns the average by dividing the sum of the values in the set by their number.
"""

from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        # Average of each column using DataFrame.mean()
        return df.mean(axis=0)
