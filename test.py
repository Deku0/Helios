from unicodedata import name
import multiprocessing
import pandas as pd
def fun():
    print("process")


if __name__ == '__main__':
    # x = lambda a : a + 10
    # print(x(5))
    columns = ['jobname', 'vc']
    s = pd.Series([1,2])
    df = pd.DataFrame(columns=columns)
    df.append(s, ignore_index=True)
    print("ssss")
    print(df)
    print("done")