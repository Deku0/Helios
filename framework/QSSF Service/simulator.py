from unittest import result
import pandas as pd
import time
import os
import argparse
import multiprocessing

from sqlalchemy import true

import utils
import cluster
from estimators import NaiveEstimator, LGBEstimator, CombinedEstimator, PhillyEstimator
os.environ['NUMEXPR_MAX_THREADS'] = str(os.cpu_count())


def main(args):
    code_start = time.perf_counter()

    '''Logger Setting'''
    print("1. Logger Setting")
    log_dir = f'{args.log_dir}/{args.experiment_name}'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir + '/logfile')
    logger = utils.logger_init(
        file=f'{log_dir}/logfile/{args.scheduler}_{args.placer}')
    print("Done")

    '''Infrastructure & Trace Initialization'''
    print("2. Infrastructure & Trace Initialization")


    vc_dict = pd.read_pickle(args.trace_dir+'/vc_dict_homo.pkl')


    if 'Philly' in args.experiment_name:
        trace_range = ('2017-10-01 00:00:00', '2017-11-30 23:59:00')
        trace_df, start_ts = utils.trace_philly_process(
            args.trace_dir, trace_range)
    else:
        if 'Sept' in args.experiment_name:
            trace_range = ('2020-09-01 00:00:00', '2020-09-26 23:59:00')
            trace_df, start_ts = utils.trace_process(
                args.trace_dir, trace_range)
        elif 'July' in args.experiment_name:
            trace_range = ('2020-07-01 00:00:00', '2020-07-31 23:59:00')
            trace_df, start_ts = utils.trace_process(
                args.trace_dir, trace_range)
        else:
            raise ValueError

    trace = utils.trace_parser(trace_df)


    CLUSTER = cluster.Cluster(
        vc_dict, args.num_gpus_per_node, args.num_cpus_per_node)

    if 'Philly' in args.experiment_name:
        estimator = PhillyEstimator(args)
    else:
        # estimator = LGBEstimator(args)
        # estimator = NaiveEstimator(args)
        estimator = CombinedEstimator(args)
    print("Done")
    '''
    Sweep ON: Run All Scheduler Policies in One Experiment
    Sweep OFF: Run Dedicated Scheduler Policy (Default)
    '''
    print("args.sweep")
    if args.sweep:
        process_num = os.cpu_count()
        all_args_list = []
        for policy in utils.get_available_schedulers():
            if policy == 'qssf':
                for i in range(len(vc_dict)):
                    all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
                                          log_dir, policy, logger, start_ts, estimator))
            else:
                for i in range(len(vc_dict)):
                    all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
                                          log_dir, policy, logger, start_ts))
    else:
        if args.processes is None:
            print("len(CLUSTER.vc_list):", len(CLUSTER.vc_list))
            print(" os.cpu_count():",  os.cpu_count())
            process_num = min(len(CLUSTER.vc_list), os.cpu_count())
        else:
            process_num = args.processes

        all_args_list = []
        for i in range(len(vc_dict)):
            if args.scheduler == 'qssf':
                all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
                                      log_dir, args.scheduler, logger, start_ts, estimator))
            else:
                all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
                                      log_dir, args.scheduler, logger, start_ts))

    # all_args_list
    # 每种11行
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa8495b7e20>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa818ad9190>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa83a03daf0>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa828225160>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa828225490>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa828228100>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa828228d30>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa82822d0a0>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa82822d6d0>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa82822da00>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)
    # (<job.Trace object at 0x7fa8495b7cd0>, <cluster.VC object at 0x7fa82822dbb0>, 'consolidate', './log/Philly', 'fifo', <RootLogger root (INFO)>, 0.0)

    # print("args: ")
    # for args_list in all_args_list:
    #     print(args_list)
    # print("Done")
    # print("with multiprocessing policy")
    
    #def simulate_vc(trace, vc, placement, log_dir, policy, logger, start_ts, *args):

    number = 1
    results = []
    for arg_list in all_args_list:
        print("***********************")
        print('the %d scheduler : %s'%(number, arg_list[4]))
        print("***********************")
        number += 1
        if arg_list[4] == 'qssf':
            continue
            result = utils.simulate_vc(arg_list[0], arg_list[1], arg_list[2], arg_list[3], arg_list[4], arg_list[5], arg_list[6], arg_list[7])
        else:
            continue
            result = utils.simulate_vc(arg_list[0], arg_list[1], arg_list[2], arg_list[3], arg_list[4], arg_list[5], arg_list[6])

        results.append(result)
        print(results)

    # process_num = 1
   
    # with multiprocessing.Pool(processes=process_num) as p:
    #     results = [p.apply_async(utils.simulate_vc, args_list)
    #                for args_list in all_args_list]
    #     results = [result.get() for result in results]

    if args.sweep:
        for policy in utils.get_available_schedulers():
            utils.cluster_concatenate(
                policy, args.placer, log_dir, args.trace_dir)
    else:
        utils.cluster_concatenate(
            args.scheduler, args.placer, log_dir, args.trace_dir)
    
    print("cluster_analysis")
    utils.cluster_analysis(args.placer, log_dir, args.trace_dir)
    print("Done")

    print("logger.info")
    logger.info(
        f'Execution Time: {round(time.perf_counter() - code_start, 2)}s')
    print("Done")
    


if __name__ == '__main__':
    print("parser args")
    parser = argparse.ArgumentParser(description='Simulator')
    parser.add_argument('-e', '--experiment-name', default='Philly',
                        type=str, help='Experiment Name')
    parser.add_argument('-t', '--trace-dir', default='./data/Philly',
                        type=str, help='Trace File Directory')
    parser.add_argument('-l', '--log-dir', default='./log',
                        type=str, help='Log Directory')

    parser.add_argument('-s', '--scheduler', default='fifo',
                        choices=utils.get_available_schedulers(), type=str, help='Scheduler Algorithm')
    parser.add_argument('-p', '--placer', default='consolidate',
                        choices=utils.get_available_placers(), type=str, help='Placer Algorithm')

    parser.add_argument('--sweep', action='store_true', default=True,
                        help='Run All Scheduler Policies in One Time')
    parser.add_argument('-j', '--processes', type=int, default=None,
                        help=('Number of processes to use in multiprocessing.Pool'
                              '(use as many as available if not specified)'))
    parser.add_argument('--timeout', default=1209600, type=int,
                        help='Timeout (in seconds), default 14 days')
    parser.add_argument('--num_gpus_per_node', type=int, default=8,
                        help=('Number of GPUs per node'))
    parser.add_argument('--num_cpus_per_node', type=int, default=96,
                        help=('Number of CPU cores per node'))

    args = parser.parse_args()
    print(type(args))
    print("done")
    main(args)
