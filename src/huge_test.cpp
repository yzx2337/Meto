#include <cstdio>
#include <ctime>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <thread>
#include <vector>
#include <bitset>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <chrono>

// yzx--bind cpu
#include <sched.h>
#include <pthread.h>

#include "src/ElimDA.h"
using namespace std;

// #define POOL_SIZE (7194070220) // 10GB
// #define POOL_SIZE (214748364800) // 20GB
// #define POOL_SIZE (429496729600) // 40GB
#define POOL_SIZE (1ll*85899345920*6) // 80GB
#define VALUE_LENGTH 8
#define MIN_BCAKTHREAD 1
double *latency;
double iter_c;

void clear_cache()
{
    int *dummy = new int[1024 * 1024 * 256];
    for (int i = 0; i < 1024 * 1024 * 256; i++)
    {
        dummy[i] = i;
    }

    for (int i = 100; i < 1024 * 1024 * 256; i++)
    {
        dummy[i] = dummy[i - rand() % 100] + dummy[i + rand() % 100];
    }
    delete[] dummy;
}
double aver(double *var, long long len)
{
    double aver = 0;
    for (long long i = 0; i < len; i++)
    {
        aver += var[i] / len;
    }
    return aver;
}

int main(int argc, char *argv[])
{
    /*cpu_set_t mask;
    CPU_ZERO(&mask);
    for(int i=1;i<15;i=i+2){
        CPU_SET(i,&mask);
    }
    sched_setaffinity(0,sizeof(cpu_set_t), &mask);   //绑定线程*/
    if (argc < 3)
    {
        cerr << "Usage: " << argv[0] << "path numData" << endl;
        exit(1);
    }
    const size_t initialSize = 512//524288//8192//256//256//2097152//1048576//1048576 // 1048576//256; // 256  //500W不分裂采用2048 5000W用8192
    ;                               // 1024*16/256*4;
    char path[32];                  // pool2址
    strcpy(path, argv[1]);
    long long numData = atoll(argv[2]); // data大小
    long long numThreads = atoi(argv[3]);
    struct timespec start, end;
    uint64_t elapsed;
    PMEMobjpool *pop;
    bool exists = false;
    TOID(ElimDA)
    HashTable = OID_NULL;
    cout << path << endl;
    // if(access(path, 0) == 0){
    // 	remove(path);    //删除文件！！
    // }
    if (access(path, 0) != 0)
    {
        pop = pmemobj_create("/mnt/pmem1-node0/ElimDA", "ElimDA", POOL_SIZE, 0666); // 创建一个POOL_SIZE大小的文件
        cout << "open pop:\t" << pop << endl;
        if (!pop)
        {
            perror("pmemoj_create");
            exit(1);
        }
        HashTable = POBJ_ROOT(pop, ElimDA); // POBJ_ROOT创建一个根对象，类型为ElimDA
        D_RW(HashTable)->initElimDA(pop, initialSize, numThreads);
        D_RW(HashTable)->initdram();
    }
   
    //D_RW(HashTable)->initValuePool(pop, numThreads, numData / numThreads, VALUE_LENGTH);

    cout << "Params: numData(" << numData << "), numThreads(" << numThreads << ")" << endl;
    long long initailSize = 5e7;
    uint64_t *keys = new uint64_t[initailSize];
    //char *method = new char[numData];
    char buffer[8192];
    //char *_v = new char[numData];
    char * values = "01234567";

    ThreadPool pool(numThreads);
    long long loadthread = numThreads;
    double time[24][400];

    auto test = [&time, &HashTable, &pop, &keys, &values, &pool, &initailSize](long long from, long long to, int a_bucket)
    {
        struct timespec start, end;
        uint64_t elapsed;
        clock_gettime(CLOCK_MONOTONIC, &start);
        long long inner_size = initailSize * 1ll / 24;
        for (long long i = from; i < to; i++)
        {
            D_RW(HashTable)
                ->Insert(i, values, a_bucket, &pool); // 会不会影响性能  reinterpret_cast<Value_t>(keys[i])
            if(((i-from) % inner_size == 0 && i != from) || i == to-1) {
                clock_gettime(CLOCK_MONOTONIC, &end);
                elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
                time[a_bucket-MERGEBUCKET_SIZE][(i-from)/inner_size] = (uint64_t)(1000000 * ( initailSize / (elapsed / 1000.0)));
                clock_gettime(CLOCK_MONOTONIC, &start);
                if(a_bucket-MERGEBUCKET_SIZE == 0) {
                   cout << "now in "<< i/inner_size << "*50M " << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * ( initailSize / (elapsed / 1000.0))) << "\tOps/sec\tInsertion" << endl;
                }
            }
       }
    };

    vector<thread> InsertingThreads;
    // clear_cache();
    int t = 0;

    long long ten_million = 2e10;
    for(long long c = 0; c <= numData / ten_million; ++c) {
        long long slice_index = c*ten_million;
        long long slice_size = ten_million;
        
        if(c == numData / ten_million) {
            slice_size = numData % ten_million;
        }
        long long chunk_size = slice_size * 1ll / loadthread;
        for (long long i = 0; i < loadthread; i++)
        {
            if (i != loadthread - 1)
                InsertingThreads.emplace_back(thread(test, 1ll*chunk_size * i, 1ll*chunk_size * (i + 1), i + MERGEBUCKET_SIZE));
            else
                InsertingThreads.emplace_back(thread(test, 1ll*chunk_size * i,  slice_size, i + MERGEBUCKET_SIZE));
        }

        clock_gettime(CLOCK_MONOTONIC, &start);
        for (auto &t : InsertingThreads) {
            t.join();
        }

        InsertingThreads.clear();
     
        clock_gettime(CLOCK_MONOTONIC, &end);
        //long long size = pmemobj_alloc_usable_size(HashTable.oid);

        elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
        cout << "now in "<< c << "*50M " << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * ( ten_million/ (elapsed / 1000.0))) << "\tOps/sec\tInsertion" << endl;
            double average[241];
            for(int i=0; i<loadthread; ++i) {
                for(int j=0; j<241; ++j) {
                    average[j] += time[i][j];
                }
            }

            for(int i=0; i<241; ++i) {
                std::cout << i << "*50M: " << average[i]/24 << std::endl;
            }
    }


    dlock_exit();
    pmemobj_persist(pop, (char *)&D_RO(HashTable)->crashed, sizeof(bool));
    // cout << pmemobj_alloc_usable_size(D_RO(HashTable)->cache_bucket.oid) << endl;
    pmemobj_close(pop);
    return 0;
}
