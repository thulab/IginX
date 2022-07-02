import pandas as pd
import math

class Log2Transformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: math.log2(x)))
        return ret.values.tolist()
