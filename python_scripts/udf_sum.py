class UDFSum:
    def __init__(self):
        pass

    def transform(self, rows):
        res = []
        for row in zip(*rows):
            sum = 0
            for num in row:
                if num is not None:
                    sum += num
            res.append(sum)
        return res
