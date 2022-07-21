"""
Sorts index in descending order:

DataFrame.sort_index(axis=0, level=None, ascending=True, inplace=False, kind='quicksort', na_position='last',
sort_remaining=True, ignore_index=False, key=None
"""
from transformer import BaseTransformer

import pandas as pd

class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        # By default, it sorts in ascending order, to sort in descending order, use ascending=False
        return df.sort_index(axis=0, level=None, ascending=False, inplace=False, kind='quicksort', na_position='last', sort_remaining=True, ignore_index=False, key=None)
