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
#define POOL_SIZE (214748364800) // 200GB
// #define POOL_SIZE (429496729600) // 40GB
// #define POOL_SIZE (85899345920) // 80GB
#define VALUE_LENGTH 128
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
    else
    {
        exists = true;
        pop = pmemobj_open(path, "ElimDA");
        if (pop == NULL)
        {
            perror("pmemobj_open");
            exit(1);
        }
        HashTable = POBJ_ROOT(pop, ElimDA);
        clock_gettime(CLOCK_MONOTONIC, &start);
        D_RW(HashTable)->pop = pop;
        D_RW(HashTable)->Rebuild();
        clock_gettime(CLOCK_MONOTONIC, &end);
        elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
        cout << elapsed / 1000 << "\tusec to rebuild" << endl;
        if (D_RO(HashTable)->crashed)
        {
            D_RW(HashTable)->Recovery(pop);
        }
    }
    D_RW(HashTable)->initValuePool(pop, numThreads, numData / numThreads, VALUE_LENGTH);
    cout << "Params: numData(" << numData << "), numThreads(" << numThreads << ")" << endl;
    uint64_t *keys = new uint64_t[numData];
    char *method = new char[numData];
    Value_t *values = new Value_t[numData];
    char buffer[8192];
    char *_v = new char[numData];
    for (int i = 0; i < numThreads; i++)
        values[i] = new char[VALUE_LENGTH];

    ifstream ifs;
    string dataset = "/mnt/ycsb/uniform-200/load200m.txt";
    ifs.open(dataset);
    if (!ifs)
    {
        cerr << "No file." << endl;
        exit(1);
    }
    else
    {
        for (int i = 0; i < numData; i++)
        {
            ifs >> method[i];
            ifs >> keys[i];
            ifs.read(values[0], 16);
            ifs.getline(buffer, 256);
            if (method[i] == 0)
            {
                cout << "Test\t" << i << "\t" << (char)method[i] << "\t" << keys[i] << "\t" << values[i] << endl;
                break;
            }
        }

        ifs.close();
        std::cout << dataset << " is used." << std::endl;
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
        struct timespec start1, end1;
        clock_gettime(CLOCK_MONOTONIC, &start1);
        // cpu_set_t my_set;
        // CPU_ZERO(&my_set);
        // CPU_SET((a_bucket - MERGEBUCKET_SIZE) * 2, &my_set); // cpu亲和性
        // sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
        // struct timespec start_t = {0, 0};
        // struct timespec end_tt = {0, 0}; // get delay

        for (int i = from; i < to; i++)
        {
            // cout << i << "\t" << (char)method[i] << endl;
            // clock_gettime(CLOCK_REALTIME, &start_t);
            // auto start_t = std::chrono::system_clock::now();
            if (method[i] == 'i')
            {
                // if (i > 10000000)
                // cout << i << endl;
                // auto f_hash = hash_funcs[0](&keys[i], sizeof(Key_t), 0xc70697);   //若大于256，若已经分裂，即loc<finish，则不用管，因为会调用syncwrite写入，若大于则写入
                // auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
                // auto x=(f_hash >> (8*sizeof(f_hash) - D_RW(HashTable)->in_dram->DRAM_Index->D_depth));
                // auto seg=D_RW(HashTable)->in_dram->DRAM_Index->seg[x];
                // _v[i]=(char)seg->S_depth;
                // values[i]=&(_v[i]);
                // D_RW(HashTable)->isvariable = false;
                D_RW(HashTable)->Insert(keys[i], values[a_bucket - MERGEBUCKET_SIZE], a_bucket, &pool); // 会不会影响性能  reinterpret_cast<Value_t>(keys[i])
            }
            else if (method[i] == 'r')
                D_RW(HashTable)->Get(keys[i]);
            else if (method[i] == 'u')
                D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
            else if (method[i] == 'd')
                D_RW(HashTable)->Update(keys[i], NONE);
            // clock_gettime(CLOCK_REALTIME, &end_tt);
            // auto end_tt = std::chrono::system_clock::now();
            // auto t_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tt - start_t);
            // latency[i] = (double)(t_lat.count());
            // cout << latency[i] << endl;
            // float t_lat = (end_tt.tv_sec - start_t.tv_sec) * 1000000000 + (end_tt.tv_nsec - start_t.tv_nsec);
        }

        // clock_gettime(CLOCK_MONOTONIC, &end1);
        // auto elapsed = (end1.tv_sec - start1.tv_sec)*1000000000 + (end1.tv_nsec - start1.tv_nsec);
        // cout << elapsed/1000 << "\tusec\t thread:\t" << (uint64_t)(from) << endl;
        // cout << a_bucket - MERGEBUCKET_SIZE << "\t" << latency[a_bucket - MERGEBUCKET_SIZE] << "\t" << max_t[a_bucket - MERGEBUCKET_SIZE] << endl;
    };
    // sleep(2);
    // D_RW(HashTable)->split(0,0);
    vector<thread> InsertingThreads;
    vector<thread> SearchingThreads;
    vector<thread> NegThreads;
    // D_RW(HashTable)->split(0,0);
    // for(int p=0;p<4096*2-000;p+=2)
    // {
    // 	 D_RW(HashTable)->split_test(p,p,0);
    // }
    // D_RW(HashTable)->init_backup(16000);  //恢复时不能initbackup?
    // D_RW(HashTable)->split(0,0);    //不是目录分裂的问题
    // D_RW(HashTable)->split(0,0);
    // clear_cache();
    int t = 0;
    // scanf("%d", &t);
    int chunk_size = numData / loadthread;
    if (!exists)
    { // 改
        cout << "Start Insertion" << endl;
        clock_gettime(CLOCK_MONOTONIC, &start);
        if (loadthread == 0)
        {
            for (int j = 0; j < numData; j++)
            {
                if (method[j] == 'i')
                {
                    D_RW(HashTable)->Insert(keys[j], reinterpret_cast<Value_t>(keys[j]), MERGEBUCKET_SIZE, &pool);
                }
                else if (method[j] == 'r')
                    D_RW(HashTable)->Get(keys[j]);
                else if (method[j] == 'u')
                    D_RW(HashTable)->Update(keys[j], reinterpret_cast<Value_t>(keys[j]));
                else if (method[j] == 'd')
                    D_RW(HashTable)->Update(keys[j], NONE);
            }
        }
        else
        {
            for (int i = 0; i < loadthread; i++)
            {
                if (i != loadthread - 1)
                    InsertingThreads.emplace_back(thread(test, chunk_size * i, chunk_size * (i + 1), i + MERGEBUCKET_SIZE));
                else
                    InsertingThreads.emplace_back(thread(test, chunk_size * i, numData, i + MERGEBUCKET_SIZE));
            }
            for (auto &t : InsertingThreads)
                t.join();
        }
        // cout << D_RW(HashTable)->elapsed/1000 << "\tusec for alloc\t"  << endl;
        // cout << sizeof(struct Sorted_Segment) << "\t" << sizeof(S_Bucket) << endl;
        // pool.stop();
        clock_gettime(CLOCK_MONOTONIC, &end);
        int size = pmemobj_alloc_usable_size(HashTable.oid);
        // cout << "used size: " << size << endl;
        elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
        cout << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * (numData / (elapsed / 1000.0))) << "\tOps/sec\tInsertion" << endl;
        // sort(latency, latency + numData);
        // cout << "\t50%: " << latency[(int)(numData * 0.5)] << "\t1%: " << latency[(int)(numData * 0.01)] << "\tmax: " << latency[numData - 1] << "\tmax-1%: " << latency[(int)(numData - 2)] << endl;
        // cout << "aver: " << aver(latency, numData) << "\t99%: " << latency[(int)(numData * 0.99)] << "\t99.9%: " << latency[(int)(numData * 0.999)] << "\tmax: " << latency[numData - 1] << "\tmax-1%: " << latency[(int)(numData - 2)] << endl;
    }
    // int failedSearch = 0;
    // vector<int> searchFailed(numThreads);
    // chunk_size = numData / numThreads;
    // // scanf("%d", &t);
    // auto test1 = [&HashTable, &pop, &keys, &searchFailed, &values](int from, int to, int tid)
    // {
    // 	int fail_cnt = 0;
    // 	for (int i = from; i < to; i++)
    // 	{
    // 		// auto start_t = std::chrono::system_clock::now();
    // 		// auto ret = D_RW(HashTable)->Get(keys[i] - 1); // neg
    // 		auto ret = D_RW(HashTable)->Get(keys[i]); // pos
    // 		// D_RW(HashTable)->Update(keys[i], NONE); // update
    // 		// auto end_tt = std::chrono::system_clock::now();
    // 		// auto t_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tt - start_t);
    // 		// latency[i] = (double)(t_lat.count());
    // 		if (ret != reinterpret_cast<Value_t>(keys[i])) // pos,neg
    // 		{											   // reinterpret_cast<Value_t>(keys[i])
    // 			fail_cnt++;
    // 		}
    // 		searchFailed[tid] = fail_cnt;
    // 		// D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
    // 	}
    // };
    // auto neg = [&HashTable, &pop, &keys, &searchFailed, &values](int from, int to, int tid)
    // {
    // 	int fail_cnt = 0;
    // 	for (int i = from; i < to; i++)
    // 	{
    // 		auto start_t = std::chrono::system_clock::now();
    // 		auto ret = D_RW(HashTable)->Get(keys[i] - 1); // neg
    // 		// auto ret = D_RW(HashTable)->Get(keys[i]);	  // pos
    // 		// D_RW(HashTable)->Update(keys[i], NONE); // update
    // 		auto end_tt = std::chrono::system_clock::now();
    // 		auto t_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tt - start_t);
    // 		latency[i] = (double)(t_lat.count());
    // 		if (ret != reinterpret_cast<Value_t>(keys[i])) // pos,neg
    // 		{											   // reinterpret_cast<Value_t>(keys[i])
    // 			fail_cnt++;
    // 		}
    // 		searchFailed[tid] = fail_cnt;
    // 		// D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
    // 	}
    // };

    // cout << "Start Search" << endl;
    // // clear_cache();
    // clock_gettime(CLOCK_MONOTONIC, &start);
    // for (int i = 0; i < numThreads; i++)
    // {
    // 	if (i != numThreads - 1)
    // 		SearchingThreads.emplace_back(thread(test1, chunk_size * i, chunk_size * (i + 1), i));
    // 	else
    // 		SearchingThreads.emplace_back(thread(test1, chunk_size * i, numData, i));
    // }
    // for (auto &t : SearchingThreads)
    // 	t.join();
    // clock_gettime(CLOCK_MONOTONIC, &end);

    // elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
    // cout << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * (numData / (elapsed / 1000.0))) << "\tOps/sec\tSearch" << endl;

    // for (auto &v : searchFailed)
    // 	failedSearch += v;
    // cout << "Search Failed: " << failedSearch << endl;
    // sort(latency, latency + numData);
    // // cout << "inter: " << space_consumption(pop) << endl;
    // cout << "50%: " << latency[(int)(numData * 0.5)] << "\t1%: " << latency[(int)(numData * 0.01)] << "\tmax: " << latency[numData - 1] << "\tmax-1%: " << latency[(int)(numData - 2)] << endl;
    // cout << "aver: " << aver(latency, numData) << "\t99%: " << latency[(int)(numData * 0.99)] << "\t99.9%: " << latency[(int)(numData * 0.999)] << "\tmax: " << latency[numData - 1] << "\tmax-1%: " << latency[(int)(numData - 2)] << endl;

    // clock_gettime(CLOCK_MONOTONIC, &start);
    // for (int i = 0; i < numThreads; i++)
    // {
    // 	if (i != numThreads - 1)
    // 		NegThreads.emplace_back(thread(neg, chunk_size * i, chunk_size * (i + 1), i));
    // 	else
    // 		NegThreads.emplace_back(thread(neg, chunk_size * i, numData, i));
    // }
    // for (auto &t : NegThreads)
    // 	t.join();
    // clock_gettime(CLOCK_MONOTONIC, &end);

    // elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
    // cout << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * (numData / (elapsed / 1000.0))) << "\tOps/sec\tSearch" << endl;

    // for (auto &v : searchFailed)
    // 	failedSearch += v;
    // cout << "Search Failed: " << failedSearch << endl;
    // sort(latency, latency + numData);
    // cout << "50%: " << latency[(int)(numData * 0.5)] << "\t1%: " << latency[(int)(numData * 0.01)] << "\tmax: " << latency[numData - 1] << "\tmax-1%: " << latency[(int)(numData - 2)] << endl;
    // cout << "aver: " << aver(latency, numData) << "\t99%: " << latency[(int)(numData * 0.99)] << "\t99.9%: " << latency[(int)(numData * 0.999)] << "\tmax: " << latency[numData - 1] << "\tmax-1%: " << latency[(int)(numData - 2)] << endl;

    // cout << D_RW(HashTable)->in_dram->valt << endl;
    dlock_exit();
    pmemobj_persist(pop, (char *)&D_RO(HashTable)->crashed, sizeof(bool));
    // cout << pmemobj_alloc_usable_size(D_RO(HashTable)->cache_bucket.oid) << endl;
    pmemobj_close(pop);
    return 0;
}
