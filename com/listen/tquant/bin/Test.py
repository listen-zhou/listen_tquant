# coding: utf-8
import inspect
import tquant as tt
import multiprocessing
class Test:
    def __init__(self, msg):
        self.msg = msg

    def test(self):
        i = 0
        while i <= 1000000:
            print(self.msg, '-', str(i))
            i += 1


if __name__ == "__main__":
    cpu_count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cpu_count)
    for i in range(100):
        test = Test('msg' + str(i))
        pool.apply_async(test.test)
    pool.close()
    pool.join()
    print('done')