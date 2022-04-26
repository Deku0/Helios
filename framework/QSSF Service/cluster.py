class Cluster:
    def __init__(self, vc_dict, num_gpus_per_node, num_cpus_per_node):
        self._vc_dict = vc_dict
        self._num_gpus_per_node = num_gpus_per_node
        self._num_cpus_per_node = num_cpus_per_node
        self.vc_num = len(vc_dict)
        self.node_num = sum(vc_dict.values())
        self.vc_list = []
        self.init_cluster_vc()
        self.total_gpus = sum(vc.total_gpus for vc in self.vc_list)
        self.total_cpus = sum(vc.total_cpus for vc in self.vc_list)

    def init_cluster_vc(self):
        for k, v in self._vc_dict.items():
            vc = VC(k, v, self._num_gpus_per_node, self._num_cpus_per_node)
            self.vc_list.append(vc)

    def cluster_free_gpus(self):
        return sum(vc.vc_free_gpus() for vc in self.vc_list)

    def cluster_free_cpus(self):
        return sum(vc.vc_free_cpus() for vc in self.vc_list)


class VC:
    def __init__(self, vc_name, node_num, num_gpus_per_node, num_cpus_per_node):
        self.vc_name = vc_name
        self.node_num = node_num
        self._num_gpus_per_node = num_gpus_per_node
        self._num_cpus_per_node = num_cpus_per_node
        self.node_list = []
        self.init_vc_node()
        self.total_gpus = num_gpus_per_node * node_num
        self.total_cpus = num_cpus_per_node * node_num

    def init_vc_node(self):
        for i in range(self.node_num):
            node = Node(i, self._num_gpus_per_node, self._num_gpus_per_node)
            self.node_list.append(node)

    def vc_free_gpus(self):
        return sum(node.free_gpus for node in self.node_list)

    def vc_free_cpus(self):
        return sum(node.free_cpus for node in self.node_list)

    def avail_node_list(self):
        avail_node_list = []
        for node in self.node_list:
            if node.free_gpus > 0:
                avail_node_list.append(node)
        return avail_node_list

    
    def release_resource(self, job):
        # import pdb; pdb.set_trace()
        nodes_list = job['nodes']
        for dict in nodes_list:
            for i, gpu_num in dict.items():
                node = self.node_list[i]
                # test log
                # print("node %d has total %d gpus, now there are %d jobs and %d free gpus, 1 job wants to release %d gpus."
                #         %(i, node.get_num_gpus(),node.get_node_job_num(), node.get_free_gpus(), gpu_num))
                assert node.node_name == i
                node.release_gpu(gpu_num)
                
        return True

    def consolidate_node_num(self):
        list = []
        for node in self.node_list:
            if node.job_num == 1:
                list.append(node)
        return len(list)

    def shared_node_num(self):
        list = []
        for node in self.node_list:
            if node.job_num > 1:
                list.append(node)
        return len(list)


class Node:
    def __init__(self, node_name, num_gpus, num_cpus):
        self.node_name = node_name
        self.job_num = 0
        self.num_gpus = num_gpus
        self.num_cpus = num_cpus
        self.free_gpus = num_gpus
        self.free_cpus = num_cpus

    '''allocate'''

    def allocate_gpu(self, num_gpu):
        if num_gpu > self.free_gpus:
            return False
        else:
            self.free_gpus -= num_gpu
            self.job_num += 1
            # print("after allocated, now there are %d jobs and %d free gpus in node %d" %(self.job_num, self.free_gpus,self.node_name))
            return True

    def allocate_cpu(self, num_cpu):
        if num_cpu > self.free_cpus:
            return False
        else:
            self.free_cpus -= num_cpu
            return True

    '''release'''

    def release_gpu(self, num_gpu):
        assert self.free_gpus + num_gpu <= self.num_gpus
        self.free_gpus += num_gpu
        self.job_num -= 1
        # print("after released, now there are %d jobs and %d free gpus in node %d" %(self.job_num, self.free_gpus,self.node_name))
        return True

    def release_cpu(self, num_cpu):
        assert self.free_cpus + num_cpu <= self.num_cpus
        self.free_cpus += num_cpu
        return True

    def get_num_gpus(self):
        return self.num_gpus

    def get_free_gpus(self):
        return self.free_gpus

    def get_node_name(self):
        return self.node_name

    def get_node_job_num(self):
        return self.job_num
