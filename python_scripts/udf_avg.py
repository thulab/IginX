class UDFAvg:
    def __init__(self):
        pass

    def transform(self, rows):
        res = []
        for row in zip(*rows):
            sum, count = 0, 0
            for num in row:
                if num is not None:
                    sum += num
                    count += 1
            res.append(sum / count)
        return res
