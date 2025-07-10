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
// yzx--bind cpu
#include <sched.h>
#include <pthread.h>

#include "src/ElimDA.h"
using namespace std;

// #define POOL_SIZE (7194070220) // 10GB
// #define POOL_SIZE (214748364800) // 20GB
// #define POOL_SIZE (4294967296) // 40GB
#define POOL_SIZE (85899345920*2) // 80GB
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
double aver(double *var, int len)
{
    double aver = 0;
    for (int i = 0; i < len; i++)
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
    const size_t initialSize = 256; // 256  //500W不分裂采用2048 5000W用8192
    ;                               // 1024*16/256*4;
    char path[32];                  // pool2址
    strcpy(path, argv[1]);
    int numData = atoi(argv[2]); // data大小
    int numThreads = atoi(argv[3]);
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
        pop = pmemobj_create(path, "ElimDA", POOL_SIZE, 0666); // 创建一个POOL_SIZE大小的文件
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
   
    D_RW(HashTable)->initValuePool(pop, numThreads, numData / numThreads, VALUE_LENGTH);

    cout << "Params: numData(" << numData << "), numThreads(" << numThreads << ")" << endl;
    uint64_t *keys = new uint64_t[numData];
    char *method = new char[numData];
    char buffer[8192];
    char *_v = new char[numData];
    char * values = new char[VALUE_LENGTH+1];
    for(int i = 0; i<VALUE_LENGTH; ++i) {
        values[i] = '0' + i%10;
    }
    values[VALUE_LENGTH] = '\0';

    ifstream ifs;
    string dataset = "/home/yzx/dgy/ycsb/uniform-2b/load2b.txt";
    ifs.open(dataset);
    if (!ifs)
    {
        cerr << "No file." << endl;
        exit(1);
    }
    else
    {
        int cnt = 0;
        for (int i = 0; i < numData; i++)
        {
            ifs >> method[i];
            ifs >> keys[i];
            ifs.getline(buffer, 20);
            //cout << method[i] << ":" << keys[i] <<endl;

            if(method[i] == 'i') {
                cnt++;
            }
            if(cnt % 100000000 == 0) {
                cout << "now load in " << cnt/1000000 << "M" << endl;
            }
            // << "\t" << values[i] << "\t" << buffer << endl;
        }

        ifs.close();
        std::cout << dataset << " is used. Total insert: "  <<  cnt << std::endl;
    }
    // dlock_exit();
    // for (int i = 0; i < 10; i++)
    // {
    // 	D_RW(HashTable)->split(0, 0);   //主要是segment的迁移
    // }
    latency = (double *)calloc(numData, sizeof(double));
    auto backThreads = (numThreads > MIN_BCAKTHREAD) ? numThreads : MIN_BCAKTHREAD;
    ThreadPool pool(backThreads);
    int loadthread = numThreads;

    auto test = [&HashTable, &pop, &keys, &values, &method, &_v, &pool](int from, int to, int a_bucket)
    {
        for (int i = from; i < to; i++)
        {
            if (method[i] == 'i')
            {
                D_RW(HashTable)
                    ->Insert(keys[i], values, a_bucket, &pool); // 会不会影响性能  reinterpret_cast<Value_t>(keys[i])
            }
            else if (method[i] == 'r')
                D_RW(HashTable)->Get(keys[i]);
            else if (method[i] == 'u')
                D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
            else if (method[i] == 'd')
                D_RW(HashTable)->Update(keys[i], NONE);
       }
    };

    vector<thread> InsertingThreads;
    // clear_cache();
    int t = 0;
    // scanf("%d", &t);
    // 首先划分numData
    int ten_million = 5e7;
    for(int c = 0; c <= numData / ten_million; ++c) {
        int slice_index = c*ten_million;
        int slice_size = ten_million;
        if(c == numData / ten_million) {
            slice_size = numData % ten_million;
        }
        int chunk_size = slice_size / loadthread;
        clock_gettime(CLOCK_MONOTONIC, &start);
        for (int i = 0; i < loadthread; i++)
        {
            if (i != loadthread - 1)
                InsertingThreads.emplace_back(thread(test, chunk_size * i + slice_index, chunk_size * (i + 1) + slice_index, i + MERGEBUCKET_SIZE));
            else
                InsertingThreads.emplace_back(thread(test, chunk_size * i + slice_index, slice_index + slice_size, i + MERGEBUCKET_SIZE));
        }

        for (auto &t : InsertingThreads)
            t.join();

        InsertingThreads.clear();
        clock_gettime(CLOCK_MONOTONIC, &end);
        
        elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
        cout << "now in "<< c << "*50M " << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * ( ten_million/ (elapsed / 1000.0))) << "\tOps/sec\tInsertion" << endl;
    }
    dlock_exit();
    pmemobj_persist(pop, (char *)&D_RO(HashTable)->crashed, sizeof(bool));
    // cout << pmemobj_alloc_usable_size(D_RO(HashTable)->cache_bucket.oid) << endl;
    pmemobj_close(pop);
    return 0;
}
