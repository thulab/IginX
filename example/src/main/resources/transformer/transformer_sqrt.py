import pandas as pd
import math

class SqrtTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: math.pow(x, 0.5)))
        return ret.values.tolist()
