"""
Sorts index in descending order:

DataFrame.sort_index(axis=0, level=None, ascending=True, inplace=False, kind='quicksort', na_position='last',
sort_remaining=True, ignore_index=False, key=None
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        # By default, it sorts in ascending order, to sort in descending order, use ascending=False
        df = df.sort_index(axis=0, level=None, ascending=False, inplace=False, kind='quicksort', na_position='last', sort_remaining=True, ignore_index=False, key=None)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
