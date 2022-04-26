from unicodedata import name
import multiprocessing

def fun():
    print("process")


if __name__ == '__main__':
    # demo(1,2,3,4,5)
    # a = 0
    # assert a > 1
    process_num = 3
    pool = multiprocessing.Pool(processes=process_num) 
    for i in range(process_num):
        pool.apply_async(fun)
    
    pool.close()
    pool.join()
    print("done")