"""
Writing a distinct() function for IginX:
Count the distinct field values associated with a field key.
1. Return DataFrame with duplicate rows removed.
2. To remove duplicates on specific column(s), use subset.
"""

from transformer import BaseTransformer
import pandas as pd


class MyTransformer(BaseTransformer):  # a class is an object constructor, or a "blueprint" for creating objects
    # All classes have a function called __init__(), it is called automatically every time the class is being used to
    # create a new object.
    def __init__(self):  # the self parameter is a reference to the current instance of the class, used to access
        # variables that belong to the class distinctTransformer()
        pass

    def transform(self, df):
        # dropping all duplicate values
        return df.drop_duplicates(subset=df.columns.values[1:])
