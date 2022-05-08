from ast import Num
from operator import index
# import numpy as np
import pandas as pd
import argparse
import random
import os
from tqdm import tqdm
import multiprocessing

def get_average_queue(rules, df):
    answer = {}
    for k, v in rules.items():
        values = []
        for index, row in df.iterrows():
            if v(row):
                values.append(row['queue'])
        answer['queue_'+k] = round(sum(values)/len(values), 4)

    return answer

def get_average_jct(rules, df):
    answer = {}
    for k, v in rules.items():
        values = []
        for index, row in df.iterrows():
            if v(row):
                values.append(row['end_time']-row['start_time'])
        answer['jct_'+k] = round(sum(values)/len(values), 4)

    return answer

if __name__ == '__main__':
    
    # df_list = []
    # files_path = "./log/Philly/logfile/qssf/results"
    # files = os.listdir(files_path)
    # for file in tqdm(files):
    #     file_path = os.path.join(files_path,file)
    #     df = pd.read_csv(file_path, encoding='gbk')
    #     print(df.shape)
    #     df_list.append(df)
    # df = pd.concat(df_list)
    # print(";;;")
    # print(df.shape)

    df = pd.read_csv("./log/Philly/logfile/qssf/results/SpawnPoolWorker-6_results.csv")
    rules = {'x>0': lambda x : x['gpu_num'] > 0,
            'x>1': lambda x : x['gpu_num'] > 1,
            'x>2': lambda x : x['gpu_num'] > 2,
            'x>3': lambda x : x['gpu_num'] > 3,
            'x>4': lambda x : x['gpu_num'] > 4}
    answer1 = get_average_queue(rules, df)
    answer2 = get_average_jct(rules, df)
    answer3 = dict(answer2, **answer1)
    print(answer1)
    print(answer2)
    print(answer3)
    # parser = argparse.ArgumentParser(description='Simulator')
    # parser.add_argument('-a', type=int, default=96, help=('Number of CPU cores per node'))
    # args = parser.parse_args()
    # print(args.a)
    # print(type(args))
    # dict = {"k":'s'}
    # if 's' in dict["k"]:
    #     print("true")