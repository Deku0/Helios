from unittest import result
import pandas as pd
import time
import os
import argparse
import multiprocessing

from sqlalchemy import false, true

import utils
import cluster
from estimators import NaiveEstimator, LGBEstimator, CombinedEstimator, PhillyEstimator
os.environ['NUMEXPR_MAX_THREADS'] = str(os.cpu_count())

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

def simulate_run(parameters, rules):
    code_start = time.perf_counter()

    '''Logger Setting'''
    log_dir = parameters['log_dir']
    experiment_name = parameters['experiment_name']
    log_dir = f'{log_dir}/{experiment_name}'

    '''Infrastructure & Trace Initialization'''
    if 'Philly' in parameters['experiment_name']:
        trace_range = ('2017-10-01 00:00:00', '2017-11-30 23:59:00')
        trace_df, start_ts = utils.trace_philly_process(
            parameters['trace_dir'], trace_range)
    else:
        if 'Sept' in parameters['experiment_name']:
            trace_range = ('2020-09-01 00:00:00', '2020-09-26 23:59:00')
            trace_df, start_ts = utils.trace_process(
                parameters['trace_dir'], trace_range)
        elif 'July' in parameters['experiment_name']:
            trace_range = ('2020-07-01 00:00:00', '2020-07-31 23:59:00')
            trace_df, start_ts = utils.trace_process(
                parameters['trace_dir'], trace_range)
        else:
            raise ValueError

    trace = utils.trace_parser(trace_df)



    vc_dict = pd.read_pickle(parameters['trace_dir']+'/vc_dict_homo.pkl')
    CLUSTER = cluster.Cluster(
        vc_dict, parameters['num_gpus_per_node'], parameters['num_cpus_per_node'])

    if 'Philly' in parameters['experiment_name']:
        estimator = PhillyEstimator(parameters)
    else:
        # estimator = LGBEstimator(args)
        # estimator = NaiveEstimator(args)
        estimator = CombinedEstimator(parameters)

    '''
    Sweep ON: Run All Scheduler Policies in One Experiment
    Sweep OFF: Run Dedicated Scheduler Policy (Default)
    '''
    if parameters['sweep']:
        process_num = os.cpu_count()
        all_args_list = []
        for policy in utils.get_available_schedulers():
            if policy == 'qssf':
                for i in range(len(vc_dict)):
                    all_args_list.append((trace, CLUSTER.vc_list[i], parameters['placer'], log_dir, "SpawnPoolWorker-"+str(i+1), parameters, policy, start_ts, estimator))
            else:
                for i in range(len(vc_dict)):
                    all_args_list.append((trace, CLUSTER.vc_list[i], parameters['placer'],log_dir, "SpawnPoolWorker-"+str(i+1), parameters, policy, start_ts))
    else:
        if parameters['processes'] is None:
            process_num = min(len(CLUSTER.vc_list), os.cpu_count())
        else:
            process_num = parameters['processes']

        all_args_list = []
        for i in range(len(vc_dict)):
            if parameters['scheduler'] == 'qssf':
                all_args_list.append((trace, CLUSTER.vc_list[i], parameters['placer'], log_dir,  "SpawnPoolWorker-"+str(i+1), parameters, parameters['scheduler'], start_ts, estimator))
            else:
                all_args_list.append((trace, CLUSTER.vc_list[i], parameters['placer'], log_dir, "SpawnPoolWorker-"+str(i+1), parameters, parameters['scheduler'], start_ts))
    
    '''多线程运行'''
    process_num = len(all_args_list)
    results = []
    pool = multiprocessing.Pool(process_num)
    for i in range(len(all_args_list)):
        result = pool.apply_async(utils.simulate_vc, all_args_list[i])
        results.append(result)
    print('----start-----') #调用join之前，先调用close函数，否则会出错。
    pool.close()#关闭进程池，关闭后就不再接受新的请求，即开始执行任务。
    pool.join()## join函数等待所有子进程结束，才会执行主进程之后的代码
    print('-----end------')

    if parameters['sweep']:
        for policy in utils.get_available_schedulers():
            utils.cluster_concatenate(
                policy, parameters['placer'], log_dir, parameters['trace_dir'])
    else:
        utils.cluster_concatenate(
            parameters['scheduler'], parameters['placer'], log_dir, parameters['trace_dir'])
    utils.cluster_analysis(parameters['placer'], log_dir, parameters['trace_dir'])
    
    #main logger
    logger = utils.create_logger("main_process",log_dir, parameters)
    logger.info(f'Execution Time: {round(time.perf_counter() - code_start, 2)}s')

    '''处理数据'''
    df_list = []
    files_path = "./log/Philly/logfile/qssf/results"
    files = os.listdir(files_path)
    for file in files:
        file_path = os.path.join(files_path,file)
        df = pd.read_csv(file_path, encoding='gbk')
        df_list.append(df)
    df = pd.concat(df_list)
    # 计算平均等待时间和jct
    queue = get_average_queue(rules, df)
    jct = get_average_jct(rules, df)
    results = dict(queue, **jct)
    
    return results

if __name__ == '__main__':
    #输入:判断条件：某类任务的jct
    #返回jct
    parameters = {
        'experiment_name':'Philly',
        'trace_dir':'./data/Philly',
        'log_dir':'./log',
        'scheduler':'qssf',
        'placer':'consolidate',
        'sweep':False,
        'processes':None,
        'timeout':1209600,
        'num_gpus_per_node':8,
        'num_cpus_per_node':96
    }

    rules = {'x>0': lambda x : x['gpu_num'] > 0,
            'x>1': lambda x : x['gpu_num'] > 1,
            'x>2': lambda x : x['gpu_num'] > 2,
            'x>3': lambda x : x['gpu_num'] > 3,
            'x>4': lambda x : x['gpu_num'] > 4}

    results = simulate_run(parameters, rules)

    print(results)