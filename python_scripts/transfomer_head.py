"""
Return the first n rows.
This function returns the first n rows for the object based on position.
It is useful for quickly testing if your object has the right type of data in it.
"""
from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        return df.head(n=5)
