"""
The bottom() function sorts a table by columns and keeps only the bottom n records. bottom() is a selector function.
bottom() 函数按列对表进行排序并仅保留底部的 n 条记录。 bottom() 是一个选择器函数。

Returns the smallest N field values associated with the field key.
返回与字段键关联的最小 N 字段值。
"""

from transformer import BaseTransformer

import pandas as pd


class MyTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, df, n=1):
        # Return the last n rows from df
        return df.tail(n)
