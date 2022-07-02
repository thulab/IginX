import pandas as pd
import math

class LogTransformer:
    def __init__(self):
        pass

    def transform(self, rows, n):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: math.log(x, n)))
        return ret.values.tolist()
