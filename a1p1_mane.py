########################################
## Template Code for Big Data Analytics
## assignment 1 - part I, at Stony Brook Univeristy
## Fall 2017


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random
from collections import defaultdict


##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:  # [TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3):  # [DONE]
        self.data = data  # the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks  # how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks  # " " " as reduce tasks
        #print self.data
    ###########################################################
    # programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):  # [DONE]
        print("Need to override map")

    @abstractmethod
    def reduce(self, k, vs):  # [DONE]
        print("Need to override reduce")

    ###########################################################
    # System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r):  # [DONE]
        # runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            # run mappers:
            mapped_kvs = self.map(k, v)
            #print mapped_kvs
            # assign each kv pair to a reducer task
            if mapped_kvs is not None:
                for (k, v) in mapped_kvs:
                    namenode_m2r.append((self.partitionFunction(k), (k, v)))

    def partitionFunction(self, k):  # [TODO]
        if(type(k) is int):
            return hash(k) % self.num_reduce_tasks
        else:
            a = 0
            for b in k:
                a = ord(b)
            return hash(a) % self.num_reduce_tasks

    #given a key returns the reduce task to send it


    def reduceTask(self, kvs, namenode_fromR):  # [TODO]
        #print ('in reduceTask')
        #sort all values for each key (can use a list of dictionary)
        #print kvs
        # call reducers on each key with a list of values
        # and append the result for each key to namenode_fromR
        # [TODO]

        dict1 = defaultdict(list)
        for (k, v) in kvs:
            dict1[k].append(v)
            #print k, d1[k]
        for (k,v) in kvs:
            redu = self.reduce(k, dict1[k])
            if redu is not None:
                if redu not in namenode_fromR:
                    namenode_fromR.append(redu)



    @property
    def runSystem(self):  # [TODO]
        # runs the full map-reduce system processes on mrObject

        # the following two lists are shared by all processes
        # in order to simulate the communication
        # [DONE]
        namenode_m2r = Manager().list()  # stores the reducer task assignment and
        # each key-value pair returned from mappers
        # in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list()  # stores key-value pairs returned from reducers
        # in the form [(k, v), ...]

        # divide up the data into chunks accord to num_map_tasks, launch a new process
        # for each map task, passing the chunk of data to it.
        # hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()
        #  (it might be useful to keep the processes in a list)
        # [TODO]
        cnt = 0
        for line in self.data:
            cnt += 1
        size = int(cnt / self.num_map_tasks)
        map_chunk = [[] for i in range(self.num_map_tasks)]
        cnt = 0
        while cnt != self.num_map_tasks - 1:
            map_chunk[cnt] = self.data[size * cnt: size * (cnt + 1)]
            cnt += 1

        # [TODO]

        #done
        map_chunk[cnt] = self.data[size * cnt:]
        for i in range(self.num_map_tasks):
            # print to_reduce_task[i]
            p = Process(target=self.mapTask, args=(map_chunk[i], namenode_m2r))
            p.start()
            p.join()
        # join map task processes back


        # print output from map tasks
        # [DONE]
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))
        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        # [TODO]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]

        # launch the reduce tasks as a new process for each.
        # [TODO]

        for node in namenode_m2r:
            to_reduce_task[node[0]].append(node[1])


        # join the reduce tasks back
        # [TODO]

        for i in range(self.num_reduce_tasks):
            #print to_reduce_task[i]
            p = Process(target=self.reduceTask, args=(to_reduce_task[i], namenode_fromR))
            p.start()
            p.join()

        # print output from reducer tasks
        # [DONE]
        print("namenode_R after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        # return all key-value pairs:
        # [DONE]
        #return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes this case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs):  # [DONE]
        #print k,vs
        #print (k, np.sum(vs))
        return (k, np.sum(vs))


class SetDifferenceMR(MyMapReduce):  # [TODO]
    # contains the map and reduce function for set difference
    # Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, key, value):
        final = dict()
        for v in value:
            final[v] = key
        return final.items()
    def reduce(self, k, vs):
        cnt = 0
        for v in vs:
            if v == 'R':
                cnt += 1
            else:
                cnt -=1
        if cnt == 1:
            if vs[0] == 'R':
                return k





##########################################################################
##########################################################################


if __name__ == "__main__":

    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem

    ####################
    ##run SetDifference
    # (TODO: uncomment when ready to test)
    print("\n\n*****************\n Set Difference\n*****************\n")
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']), ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2 = [('R', [x for x in range(50) if random() > 0.5]), ('S', [x for x in range(50) if random() > 0.75])]
    mrObject = SetDifferenceMR(data1, 2, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(data2, 2, 2)
    mrObject.runSystem()
