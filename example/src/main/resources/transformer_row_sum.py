from transformer import BaseTransformer
import pandas as pd
import numpy as np


class RowSumTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df):
        ret = np.zeros((df.shape[0], 2), dtype=np.integer)
        for index, row in df.iterrows():
            row_sum = 0
            for num in row[1:]:
                row_sum += num
            ret[index][0] = row[0]
            ret[index][1] = row_sum
        return pd.DataFrame(ret, columns=['time', 'sum'])
