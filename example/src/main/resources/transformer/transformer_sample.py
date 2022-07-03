import random

class SampleTransformer:
    def __init__(self):
        pass

    def transform(self, row, n):
        l = len(row) - 1
        currNum = n
        res = []
        for i in range(n):
            res.append(row[i])
        for j in range(n, l):
            r = random.randint(0, currNum)
            if r < n:
                res[r] = row[j]
            currNum += 1
        return res
