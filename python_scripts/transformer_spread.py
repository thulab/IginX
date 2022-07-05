from transformer import BaseTransformer

import pandas as pd

class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        return df.idxmax()-df.idxmin()