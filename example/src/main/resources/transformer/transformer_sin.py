import pandas as pd
import math

class SinTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: math.sin(x)))
        return ret.values.tolist()
