"""
Access a group of rows and columns by label(s) or a boolean array.
"""
from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        return df.loc[1]  # return df.loc[row_ranking, column_ranking]
