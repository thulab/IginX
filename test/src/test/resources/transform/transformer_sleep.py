import pandas as pd
import time


class AddOneTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        time.sleep(5)
        return rows
