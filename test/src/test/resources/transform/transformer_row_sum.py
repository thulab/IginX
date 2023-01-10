import pandas as pd
import numpy as np


class RowSumTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows[1:], columns=rows[0])
        ret = np.zeros((df.shape[0], 2), dtype=np.integer)
        for index, row in df.iterrows():
            row_sum = 0
            for num in row[1:]:
                row_sum += num
            ret[index][0] = row[0]
            ret[index][1] = row_sum

        df = pd.DataFrame(ret, columns=['key', 'sum'])
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
