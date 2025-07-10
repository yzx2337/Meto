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

#include "../pcm/cpucounters.h"
#include "src/ElimDA.h"
using namespace std;
using namespace pcm;

#define LATENCY_TEST
//#define POOL_SIZE (10737418240) // 10GB
// #define POOL_SIZE (21474836480) // 20GB
//  #define POOL_SIZE (42949672960) // 40GB
#define POOL_SIZE (64424509440) // 60GB
// #define POOL_SIZE (85899345920) // 80GB
#define MIN_BCAKTHREAD 1
double *latency;
double iter_c;

bool lrcmp(l_r x, l_r y)
{
	return x.latency < y.latency;
}

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
	char path_pm[32];
	char path[32]; // pool2址

	int numData; // data大小
	int numThreads;
	if (argc < 3)
	{
		strcpy(path_pm, "/mnt/pmem1-node0");
		strcpy(path, "/mnt/pmem1-node0/ElimDA");
		numData = 5000000;
		numThreads = 1;
	}
	else
	{
		strcpy(path_pm, argv[1]);
		strcpy(path, argv[1]);

		strcat(path, "/ElimDA");
		numData = atoi(argv[2]); // data大小
		numThreads = atoi(argv[3]);
	}
	const size_t initialSize = 8; // 256  //500W不分裂采用2048 5000W用8192
	;								// 1024*16/256*4;
	struct timespec start, end;
	uint64_t elapsed;
	PMEMobjpool *pop;
	bool exists = false;
	TOID(ElimDA)
	HashTable = OID_NULL;
	cout << path << endl;
	// if (access(path, 0) == 0)
	// {
	// 	remove(path); // 删除文件！！
	// }
	dlock_exit(); // clear

	uint64_t *keys = new uint64_t[numData];
	char *method = new char[numData];
	Value_t *values = new Value_t[numData];
	char *_v = new char[numData];
	for (int i = 0; i < numData; i++)
		values[i] = new char[8];

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
			// values[i] = reinterpret_cast<Value_t>(keys[i]);
			ifs.getline(values[i], 20);
			// cout << "Test\t"  << method[i] << "\t" << keys[i] << "\t" << values[i] << endl;
		}

		ifs.close();
		// std::cout << dataset << " is used." << std::endl;
	}
	latency = (double *)calloc(numData, sizeof(double));
	auto backThreads = (numThreads > MIN_BCAKTHREAD) ? numThreads : MIN_BCAKTHREAD;
	ThreadPool pool(backThreads);
	int loadthread = numThreads;

	// cout << "my pid is " << getpid() << endl;
	int _;
	// scanf("%d", &_);

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
	// scanf("%d", &_);
	// D_RW(HashTable)->initValuePool(pop, numThreads, numData / numThreads, 16);
	cout << "Params: numData(" << numData << "), numThreads(" << numThreads << ")" << endl;

	// dlock_exit();
	// for (int i = 0; i < 10; i++)
	// {
	// 	D_RW(HashTable)->split(0, 0);   //主要是segment的迁移
	// }

	// set_signal_handlers();
	// PCM *m = PCM::getInstance();
	// auto status = m->program();
	// if (status != PCM::Success) {
	// 	std::cout << "Error opening PCM: " << status << std::endl;
	// 	if (status == PCM::PMUBusy)
	// 	m->resetPMU();
	// 	else
	// 	exit(0);
	// }
	// print_cpu_details();

	auto test = [&HashTable, &pop, &keys, &values, &method, &_v, &pool](int from, int to, int a_bucket)
	{
		// struct timespec start1, end1;
		// clock_gettime(CLOCK_MONOTONIC, &start1);
		// cpu_set_t my_set;
		// CPU_ZERO(&my_set);
		// CPU_SET((a_bucket - MERGEBUCKET_SIZE) * 2 - 1, &my_set); // cpu亲和性
		// sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
		// struct timespec start_t = {0, 0};
		// struct timespec end_tt = {0, 0}; // get delay

		for (int i = from; i < to; i++)
		{
#ifdef LATENCY_TEST
			auto start_t = std::chrono::system_clock::now();
#endif
			if (method[i] == 'i')
			{
				// auto f_hash = hash_funcs[0](&keys[i], sizeof(Key_t), 0xc70697);   //若大于256，若已经分裂，即loc<finish，则不用管，因为会调用syncwrite写入，若大于则写入
				// auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
				// auto x=(f_hash >> (8*sizeof(f_hash) - D_RW(HashTable)->in_dram->DRAM_Index->D_depth));
				// auto seg=D_RW(HashTable)->in_dram->DRAM_Index->seg[x];
				// _v[i]=(char)seg->S_depth;
				// values[i]=&(_v[i]);
				D_RW(HashTable)->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]), a_bucket, &pool); // 会不会影响性能  reinterpret_cast<Value_t>(keys[i])
																									   // memcpy(&latency[i], &_, sizeof(l_r));
			}
			else if (method[i] == 'r')
				D_RW(HashTable)
					->Get(keys[i]);
			else if (method[i] == 'u')
				D_RW(HashTable)
					->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
			else if (method[i] == 'd')
				D_RW(HashTable)
					->Update(keys[i], NONE);
#ifdef LATENCY_TEST
			auto end_tt = std::chrono::system_clock::now();
			auto t_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tt - start_t);
			latency[i] = (double)(t_lat.count());
#endif
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
	vector<thread> UpdateThreads;
	// D_RW(HashTable)->split(0,0);
	// for(int p=0;p<4096*2-000;p+=2)
	// {
	// 	 D_RW(HashTable)->split_test(p,p,0);
	// }
	// D_RW(HashTable)->init_backup(16000);  //恢复时不能initbackup?
	// D_RW(HashTable)->split(0,0);    //不是目录分裂的问题
	// D_RW(HashTable)->split(0,0);
	// clear_cache();
	int chunk_size = numData / loadthread;
	if (!exists)
	{ // 改
		cout << "Start Insertion" << endl;
		int _;
		// scanf("%d", &_);
		// int test12;
		// scanf("%d", &test12);
		// pcm
		// auto before_state = getSystemCounterState();
		clock_gettime(CLOCK_MONOTONIC, &start);
		for (int i = 0; i < loadthread; i++)
		{
			if (i != loadthread - 1)
				InsertingThreads.emplace_back(thread(test, chunk_size * i, chunk_size * (i + 1), i + MERGEBUCKET_SIZE));
			else
				InsertingThreads.emplace_back(thread(test, chunk_size * i, numData, i + MERGEBUCKET_SIZE));
		}
		for (auto &t : InsertingThreads)
			t.join();
		// cout << D_RW(HashTable)->elapsed/1000 << "\tusec for alloc\t"  << endl;
		// cout << sizeof(struct Sorted_Segment) << "\t" << sizeof(S_Bucket) << endl;
		// pool.stop();
		clock_gettime(CLOCK_MONOTONIC, &end);
		int size = pmemobj_alloc_usable_size(HashTable.oid);
		// cout << "used size: " << size << endl;
		elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
		cout << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * (numData / (elapsed / 1000.0))) << "\tOps/sec\tInsertion" << endl;

#ifdef LATENCY_TEST
		sort(latency, latency + numData);
		// cout << "inter: " << space_consumption(pop) << endl;
		cout << "max:" << latency[(int)(numData - 1)] << "\t90%: " << latency[(int)(numData * 0.90)] << "\t95%: " << latency[(int)(numData * 0.95)] << endl;
		cout << "aver: " << aver(latency, numData) << "\t99%: " << latency[(int)(numData * 0.99)] << "\t99.9%: " << latency[(int)(numData * 0.999)] << endl;
#endif

		// double lratio = 0.9999;
		// for (int i = 0; i < numData * (1 - lratio); i++)
		// {
		// 	if (latency[(int)(numData * lratio) + i].split_retry == 0)
		// 		cout << "latency:" << latency[(int)(numData * lratio) + i].latency << "\tinsert retry:" << latency[(int)(numData * lratio) + i].insert_retry << "\tdram retry:" << latency[(int)(numData * lratio) + i].dram_retry
		// 			 << "\tsplit retry:" << latency[(int)(numData * lratio) + i].split_retry << "\tnum:" << latency[(int)(numData * lratio) + i].num << endl;
		// }

		// auto after_sstate = getSystemCounterState();
		// cout << "MB ReadFromPMM: "
		// 	 << getBytesReadFromPMM(before_state, after_sstate) / 1000000 << " "
		// 	 << (getBytesReadFromPMM(before_state, after_sstate) / 1000000.0) /
		// 			(elapsed / 1000000000.0)
		// 	 << " MB/s" << endl;
		// cout << "MB WrittenToPMM: "
		// 	 << getBytesWrittenToPMM(before_state, after_sstate) / 1000000 << " "
		// 	 << (getBytesWrittenToPMM(before_state, after_sstate) / 1000000.0) /
		// 			(elapsed / 1000000000.0)
		// 	 << " MB/s" << endl;
	}
		int failedSearch = 0;
		vector<int> searchFailed(numThreads);
		chunk_size = numData / numThreads;
		// scanf("%d", &t);
		auto pos = [&HashTable, &pop, &keys, &searchFailed, &values](int from, int to, int tid)
		{
			int fail_cnt = 0;
			for (int i = from; i < to; i++)
			{
	#ifdef LATENCY_TEST
				auto start_t = std::chrono::system_clock::now();
	#endif
				auto ret = D_RW(HashTable)->Get(keys[i]); // pos
	#ifdef LATENCY_TEST
				auto end_tt = std::chrono::system_clock::now();
				auto t_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tt - start_t);
				latency[i] = (double)(t_lat.count());
	#endif
				if (ret != reinterpret_cast<Value_t>(keys[i])) // pos,neg
				{											   // reinterpret_cast<Value_t>(keys[i])
					// cout << i << "\t" << keys[i] << endl;
					fail_cnt++;
				}
				searchFailed[tid] = fail_cnt;
				// D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
			}
		};
		auto neg = [&HashTable, &pop, &keys, &searchFailed, &values](int from, int to, int tid)
		{
			int fail_cnt = 0;
			for (int i = from; i < to; i++)
			{
	#ifdef LATENCY_TEST
				auto start_t = std::chrono::system_clock::now();
	#endif
				auto ret = D_RW(HashTable)->Get(keys[i] - 1); // neg
	#ifdef LATENCY_TEST
				auto end_tt = std::chrono::system_clock::now();
				auto t_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tt - start_t);
				latency[i] = (double)(t_lat.count());
	#endif
				if (ret != reinterpret_cast<Value_t>(keys[i])) // pos,neg
				{											   // reinterpret_cast<Value_t>(keys[i])
															   // fail_cnt++;
				}
				// searchFailed[tid] = fail_cnt;
				// D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
			}
		};

		auto update = [&HashTable, &pop, &keys, &searchFailed, &values](int from, int to, int tid)
		{
			int fail_cnt = 0;
			for (int i = from; i < to; i++)
			{
	#ifdef LATENCY_TEST
				auto start_t = std::chrono::system_clock::now();
	#endif
				// auto ret = D_RW(HashTable)->Get(keys[i] - 1); // neg
				D_RW(HashTable)->Update(keys[i], NULL); // pos
														// D_RW(HashTable)->Update(keys[i], NONE); // update
	#ifdef LATENCY_TEST
				auto end_tt = std::chrono::system_clock::now();
				auto t_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tt - start_t);
				latency[i] = (double)(t_lat.count());
	#endif
				// D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i]));
			}
		};

		cout << "Start Positive Search" << endl;
		// clear_cache();
		clock_gettime(CLOCK_MONOTONIC, &start);
		for (int i = 0; i < numThreads; i++)
		{
			if (i != numThreads - 1)
				SearchingThreads.emplace_back(thread(pos, chunk_size * i, chunk_size * (i + 1), i));
			else
				SearchingThreads.emplace_back(thread(pos, chunk_size * i, numData, i));
		}
		for (auto &t : SearchingThreads)
			t.join();
		clock_gettime(CLOCK_MONOTONIC, &end);

		elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
		cout << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * (numData / (elapsed / 1000.0))) << "\tOps/sec\tSearch" << endl;
		for (auto &v : searchFailed)
			failedSearch += v;
		cout << "Search Failed: " << failedSearch << endl;
	#ifdef LATENCY_TEST
		sort(latency, latency + numData);
		// cout << "inter: " << space_consumption(pop) << endl;
		cout << "max:" << latency[(int)(numData - 1)] << "\t90%: " << latency[(int)(numData * 0.90)] << "\t95%: " << latency[(int)(numData * 0.95)] << endl;
		cout << "aver: " << aver(latency, numData) << "\t99%: " << latency[(int)(numData * 0.99)] << "\t99.9%: " << latency[(int)(numData * 0.999)] << endl;
	#endif

		cout << "Start Negative Search" << endl;
		clock_gettime(CLOCK_MONOTONIC, &start);
		for (int i = 0; i < numThreads; i++)
		{
			if (i != numThreads - 1)
				NegThreads.emplace_back(thread(neg, chunk_size * i, chunk_size * (i + 1), i));
			else
				NegThreads.emplace_back(thread(neg, chunk_size * i, numData, i));
		}
		for (auto &t : NegThreads)
			t.join();
		clock_gettime(CLOCK_MONOTONIC, &end);

		elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
		cout << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * (numData / (elapsed / 1000.0))) << "\tOps/sec\tSearch" << endl;

		for (auto &v : searchFailed)
			failedSearch += v;
		cout << "Search Failed: " << failedSearch << endl;
	#ifdef LATENCY_TEST
		sort(latency, latency + numData);
		// cout << "inter: " << space_consumption(pop) << endl;
		cout << "max:" << latency[(int)(numData - 1)] << "\t90%: " << latency[(int)(numData * 0.90)] << "\t95%: " << latency[(int)(numData * 0.95)] << endl;
		cout << "aver: " << aver(latency, numData) << "\t99%: " << latency[(int)(numData * 0.99)] << "\t99.9%: " << latency[(int)(numData * 0.999)] << endl;
	#endif

		cout << "Start Delete" << endl;
		clock_gettime(CLOCK_MONOTONIC, &start);
		for (int i = 0; i < numThreads; i++)
		{
			if (i != numThreads - 1)
				UpdateThreads.emplace_back(thread(update, chunk_size * i, chunk_size * (i + 1), i));
			else
				UpdateThreads.emplace_back(thread(update, chunk_size * i, numData, i));
		}
		for (auto &t : UpdateThreads)
			t.join();
		clock_gettime(CLOCK_MONOTONIC, &end);

		elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
		cout << elapsed / 1000 << "\tusec\t" << (uint64_t)(1000000 * (numData / (elapsed / 1000.0))) << "\tOps/sec\tSearch" << endl;

	#ifdef LATENCY_TEST
		sort(latency, latency + numData);
		// cout << "inter: " << space_consumption(pop) << endl;
		cout << "max:" << latency[(int)(numData - 1)] << "\t90%: " << latency[(int)(numData * 0.90)] << "\t95%: " << latency[(int)(numData * 0.95)] << endl;
		cout << "aver: " << aver(latency, numData) << "\t99%: " << latency[(int)(numData * 0.99)] << "\t99.9%: " << latency[(int)(numData * 0.999)] << endl;
	#endif

	cout << D_RW(HashTable)->in_dram->valt << endl;
	dlock_exit();
	pmemobj_persist(pop, (char *)&D_RO(HashTable)->crashed, sizeof(bool));
	// cout << pmemobj_alloc_usable_size(D_RO(HashTable)->cache_bucket.oid) << endl;
	pmemobj_close(pop);
	return 0;
}
