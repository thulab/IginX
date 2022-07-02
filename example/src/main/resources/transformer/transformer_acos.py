import pandas as pd
import math

class AcosTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        ret = pd.DataFrame(data=df.applymap(lambda x: math.acos(x)))
        return ret.values.tolist()