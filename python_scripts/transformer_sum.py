from transformer import BaseTransformer
import pandas as pd


class SumTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        ret = pd.DataFrame(data=df.sum(axis=0)).transpose()
        return ret
        pass
