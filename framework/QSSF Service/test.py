import numpy as np
import pandas as pd
import time
import random
import os
import multiprocessing

class Job(dict):
    def __init__(self, series):
        super(Job, self).__init__()


if __name__ == '__main__':
    a = np.load("SpawnPoolWorker-9_results.npy", allow_pickle=True)
    a = pd.DataFrame(a)
    print(a)
    print(type(a))
