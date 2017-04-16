# coding: utf-8

from multiprocessing import Pool
import os
import time
import random

class MyThread():
    def __init__(self):
        # self.func = func
        print('MyThread init...')

    def long_time_task(self, i):
        print('Run task %s (%s)...' % (i, os.getpid()))
        time.sleep(random.random() * 3)
        print(i)
        # return (i, os.getpid())

    # def parse_thread(self):
    #     print ('Parent process %s.' % os.getpid())
    #     p = Pool()
    #     # results = []
    #     for i in range(10):
    #         p.apply_async(long_time_task_wrapper, args=(self, i,))
    #     p.close()
    #     p.join()
        # Now can get the result
        # for res in results:
        #     print(res.get())
        #     print('Waiting for all subprocesses done...')
        #     p.close()
        #     p.join()
        #     print('All subprocesses done.')

def long_time_task_wrapper(cls_instance, i):
        cls_instance.long_time_task(i)

def main():
    print("start")
    p = Pool(processes=4)
    tt=MyThread()
    # tt.parse_thread()
    for i in range(10):
        p.apply_async(long_time_task_wrapper, args=(tt, i,))
    p.close()
    p.join()
if __name__=="__main__":
    main()