from ast import Num
from operator import index
# import numpy as np
import pandas as pd
import argparse
import random
import os
import multiprocessing

class Job(dict):
    def __init__(self, series):
        super(Job, self).__init__()

def get_average_queue(dict, df):
    answer = {}
    for k, v in dict.items():
        values = []
        for index, row in df.iterrows():
            if v(row):
                values.append(row['queue'])
        answer[k] = round(sum(values)/len(values), 4)

    return answer

# def get_args():
#     parser = argparse.ArgumentParser(description='Simulator')
#     parser.add_argument('a', type=int, default=96, help=('Number of CPU cores per node'))
#     args = parser.parse_args()
#     return args

if __name__ == '__main__':
    # df = pd.read_csv("./log/Philly/logfile/qssf/SpawnPoolWorker-6_results.csv")
    # dict = {'queue_x_>_0': lambda x : x['gpu_num'] > 0,
    #         'queue_x>1': lambda x : x['gpu_num'] > 1,
    #         'queue_x>2': lambda x : x['gpu_num'] > 2,
    #         'queue_x>3': lambda x : x['gpu_num'] > 3,
    #         'queue_x>4': lambda x : x['gpu_num'] > 4}
    # answer = get_average_queue(dict, df)
    # parser = argparse.ArgumentParser(description='Simulator')
    # parser.add_argument('-a', type=int, default=96, help=('Number of CPU cores per node'))
    # args = parser.parse_args()
    # print(args.a)
    # print(type(args))
    dict = {"k":'s'}
    if 's' in dict["k"]:
        print("true")

    
