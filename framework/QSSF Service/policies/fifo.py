from multiprocessing.dummy import JoinableQueue
from .policy import Policy


class FirstInFirstOut(Policy):
    def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
        super(FirstInFirstOut, self).__init__(
            trace, vc, placement, log_dir, logger, start_ts)
        self._name = 'fifo'

    def simulate(self):
        prev_index = -1
        # print("self.total_job_num:", self.total_job_num)
        iteration = 0
        while self.end_job_num != self.total_job_num:
            # test log
            # print("iteration: ", iteration)
            iteration = iteration + 1
            
            '''1. Check & Release End Jobs'''
            # test log
            # print("%d jobs in the run list, %d free gpus"%(len(self.run_list),self._vc.vc_free_gpus()))                

            self.run_list.sort(key=lambda x: x.__getitem__('end_time'))
            run_ls = self.run_list.copy()  # Avoid list.remove() issue
            job_finish_num = 0
            a = 0
            for job in run_ls:
                if self.time >= job['end_time']:
                    job['remain'] = 0
                    job['status'] = 'end'
                    self.end_job_num += 1
                    # assert self._vc.release_resource(job['nodes']) == True
                    assert self._vc.release_resource(job) == True
                    self.run_list.remove(job)

                    job_finish_num = job_finish_num+1

            # print("%d jobs are finished, now there are %d jobs in the run list"%(job_finish_num, len(self.run_list)))
            
            # print("----------------------------------------------")

            '''2. Allocate New / Pending Jobs'''
            # New Job
            for idx in range(prev_index + 1, self.total_job_num):
                job = self.trace.job_list[idx]
                if job['submit_time'] <= self.time:
                    job['status'] = 'pend'
                    self.que_list.append(job)
                    prev_index = idx
                # elif job['submit_time'] > self.time:
                #     break

            # Pend Job
            # NOTE: Sort by submit time -- FIFO
            self.que_list.sort(key=lambda x: x.__getitem__('submit_time'))
            que_ls = self.que_list.copy()  # Avoid list.remove() issue
            job_num = 0
            cost_gpu_num= 0
            for job in que_ls:
                if self.job_placer(job):
                    job['start_time'] = self.time
                    job['end_time'] = job['start_time'] + job['duration']
                    job['queue'] = self.time - job['submit_time']
                    job['status'] = 'run'
                    self.que_list.remove(job)
                    self.run_list.append(job)
                else:
                    break

            #test log
            # print("already add %d job in run list, cost %d gpu"%(job_num, cost_gpu_num))
            # print()
            

            '''3. Log & Result Recorder'''
            if self.time % 10000 == 0:
                self.runtime_log()

            # Sample Cluster State Every Minute
            if self.time % 60 == 0:
                self.seq_recorder()

            self.time += 60

        self.log_recorder(self._name)
        print("one simulate done")
