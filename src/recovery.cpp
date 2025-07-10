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
//yzx--bind cpu
#include <sched.h>
#include <pthread.h>

#include "src/ElimDA.h"
using namespace std;

// #define POOL_SIZE (1073741824) // 1GB
#define POOL_SIZE (21474836480)  //20GB
// #define POOL_SIZE (42949672960)  //40GB
#define MIN_BCAKTHREAD 1
#define LOAD_THREAD 16

void clear_cache() {
    int* dummy = new int[1024*1024*256];
    for (int i=0; i<1024*1024*256; i++) {
	dummy[i] = i;
    }

    for (int i=100;i<1024*1024*256;i++) {
	dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
    }
    delete[] dummy;
}


int main (int argc, char* argv[])
{
    if(argc < 3){
	cerr << "Usage: " << argv[0] << "path numData" << endl;
	exit(1);
    }
    const size_t initialSize=8  //256  //500W不分裂采用2048
	;  //1024*16/256*4;
    char path[32];    //pool2址
    strcpy(path, argv[1]);
    int loadnum = atoi(argv[2]);     //data大小
    int numThreads = atoi(argv[3]);
	
    struct timespec start, end;
    uint64_t elapsed;
    PMEMobjpool* pop;
    bool exists = false;
    TOID(ElimDA) HashTable = OID_NULL;
	cout << path <<endl;
	// if(access(path, 0) == 0){
	// 	remove(path);    //删除文件！！
	// }
    if(access(path, 0) != 0){
	pop = pmemobj_create(path, "ElimDA", POOL_SIZE, 0666);   //创建一个POOL_SIZE大小的文件
	if(!pop){
	    perror("pmemoj_create");
	    exit(1);
	}
	HashTable = POBJ_ROOT(pop, ElimDA);     //POBJ_ROOT创建一个根对象，类型为ElimDA
	D_RW(HashTable)->initElimDA(pop, initialSize, numThreads);
	D_RW(HashTable)->initdram();
    }
    else{
	exists=true;
	pop = pmemobj_open(path, "ElimDA");
	if(pop == NULL){
	    perror("pmemobj_open");
	    exit(1);
	}
	HashTable = POBJ_ROOT(pop, ElimDA);
	D_RW(HashTable)->STRUCTURETHREAD = atoi(argv[4]);
	D_RW(HashTable)->DATATHREAD = atoi(argv[5]);
	cout << D_RW(HashTable)->DATATHREAD << endl;
	clock_gettime(CLOCK_MONOTONIC, &start);
	D_RW(HashTable)->Rebuild();
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
	cout << elapsed/1000 << "\tusec to rebuild" << endl;	
	if(D_RO(HashTable)->crashed){
	    D_RW(HashTable)->Recovery(pop);
	}
    }
    cout << "Params: loadnum(" << loadnum << "), numThreads(" << numThreads << ")" << endl;
    uint64_t* keys = new uint64_t[loadnum];
	char* method = new char[loadnum];
	Value_t* values = new Value_t [loadnum];
	char * _v=new char[loadnum];
	for(int i=0;i<loadnum;i++)
		values[i]=new char[8];

    ifstream ifs;
    string dataset = "/home/yzx/dgy/ycsb/uniform-200/load200m.txt";
    ifs.open(dataset);
    if (!ifs){
	cerr << "No file." << endl;
	exit(1);
    }
    else{
	for(int i=0; i<loadnum; i++)
	{
		ifs >> method[i];
		ifs >> keys[i];
		ifs.getline(values[i],20);
		//cout << "Test\t"  << method[i] << "\t" << keys[i] << "\t" << values[i] << endl;
	}
	    
	ifs.close();
	cout << dataset << " is used." << endl;
    }
	//dlock_exit();
	auto backThreads= (numThreads > MIN_BCAKTHREAD) ? numThreads : MIN_BCAKTHREAD ;
	ThreadPool pool(backThreads);
	auto test = [&HashTable,&pop,&keys,&values,&method,&_v,&pool](int from, int to,int a_bucket){
		// cpu_set_t my_set;
		// CPU_ZERO(&my_set);
		// CPU_SET(a_bucket*2, &my_set);  //cpu亲和性
		// sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
		for(int i=from; i<to; i++){
			// if(i%10000000==0){
			// 	D_RW(HashTable)->getinfo(i);
			// }
			if(method[i]=='i'){
				D_RW(HashTable)->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]) ,a_bucket,&pool);   //会不会影响性能  reinterpret_cast<Value_t>(keys[i])
			}		
			else if(method[i]=='r'){
				D_RW(HashTable)->Get(keys[i]);
			}
			else if(method[i]=='u')
				D_RW(HashTable)->Update(keys[i], reinterpret_cast<Value_t>(keys[i])); //reinterpret_cast<Value_t>(keys[i])
			else if(method[i]=='d')
				D_RW(HashTable)->Update(keys[i], NONE);	
		}
    };
	vector<thread> InsertingThreads;
	vector<thread> OperThreads;
	// clear_cache();
	// sleep(2);
	int chunk_size = loadnum/LOAD_THREAD;
	if(!exists){
		cout << "Start Insertion" << endl;
		clock_gettime(CLOCK_MONOTONIC, &start);
		for(int i=0; i<LOAD_THREAD; i++){
		if(i != LOAD_THREAD-1)
			InsertingThreads.emplace_back(thread(test, chunk_size*i, chunk_size*(i+1),i+MERGEBUCKET_SIZE));
		else
			InsertingThreads.emplace_back(thread(test, chunk_size*i, loadnum,i+MERGEBUCKET_SIZE));
		}
		for(auto& t: InsertingThreads) t.join();
		cout << D_RW(HashTable)->elapsed/1000 << "\tusec for alloc\t"  << endl;
		// cout << sizeof(struct Sorted_Segment) << "\t" << sizeof(S_Bucket) << endl;
		// pool.stop();
		clock_gettime(CLOCK_MONOTONIC, &end);
		elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
		cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(loadnum/(elapsed/1000.0))) << "\tOps/sec\tInsertion" << endl;
	}

	// D_RW(HashTable)->getinfo(240000000);
    D_RW(HashTable)->crashed = false;
	dlock_exit();
    pmemobj_persist(pop, (char*)&D_RO(HashTable)->crashed, sizeof(bool));
	// cout << pmemobj_root_size(pop) << endl;
	// cout << pmemobj_alloc_usable_size(D_RO(HashTable)->cache_bucket.oid) << endl;
    pmemobj_close(pop);
    return 0;
} 
