import math


class UDFCos:
    def __init__(self):
        pass

    def transform(self, row):
        res = []
        for num in row:
            res.append(math.cos(num))
        return res
