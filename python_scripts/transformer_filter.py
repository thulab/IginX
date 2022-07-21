"""
The filter() function is used to subset rows or columns of dataframe according to labels in the specified index.
"""
from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        return df.filter(items=['High', 'Low'])  # (items=None, like=None, regex=None, axis=None)
