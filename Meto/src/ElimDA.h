#ifndef ElimDA_H_
#define ElimDA_H_

#include <cstring>
#include <vector>
#include <queue>
#include <cmath>
#include <thread>
#include <vector>
#include <cstdlib>
#include <mutex>
#include <pthread.h>
#include <libpmemobj.h>
#include <time.h>
#include <sys/mman.h>
#include "src/util.h"
#include <immintrin.h>
#include <unistd.h> //获取线程id 可删除

#include "include/dlock.h"
#include "src/ThreadPool.h"

// level 对应threadnum bucket对应pagenum num对应lognum
using namespace std;

#define INVALID_THREAD 255UL
#define TOID_ARRAY(x) TOID(x)
#define TOID_POINT(x) TOID(x)
#define U_Bucket_Size 4096UL // Log 大小 一次分配16MB ，可以写入91W左右的数据 分配1MB 写入 6W 最多到4096 lognum还得×16
#define S_Bucket_Size 256
#define D_Bucket_Size 16
#define Log_Count_Size D_Bucket_Size
#define MAX_FLUSH 1UL
#define MAX_THREAD 128UL
#define MEMCPY_FLAG PMEMOBJ_F_MEM_NONTEMPORAL
#define MERGEBUCKET_SIZE 1024
#define ALTER_NUM 128UL
// #define STRUCTURETHREAD 4
// #define DATATHREAD 8
#define LISTNUM 96UL
#define DATATHREAD 8UL
// #define likely(x) __builtin_expect((x), 1)
// #define unlikely(x) __builtin_expect((x), 0)
#define likely(x) (x)
#define unlikely(x) (x)

typedef size_t Key_t;
typedef unsigned short Finger_t;
typedef char *Value_t;
typedef uint64_t Version_t;

constexpr int8_t set_flush = 1 << 7; // 采用异或，直接将头一位至0
constexpr int8_t set_used = 1 << 6;
constexpr int8_t get_num = 15;	// 采用与
constexpr int8_t reset_num = 0; // 采用与
constexpr size_t Finger_Mask = (1 << 16) - 1;
constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits) - 1; // 后8位
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 8;
constexpr size_t kCuckooThreshold = 16;
// constexpr size_t kCuckooThreshold = 32;
constexpr size_t kBucketNum = 32; // 一个段桶的大小
constexpr size_t numperblock = (kNumPairPerCacheLine * kNumCacheLine);
constexpr size_t numpernvm = (kNumPairPerCacheLine * 4);
constexpr int16_t allfinish = 32767; // 14byte全为1
constexpr size_t map_ratio = U_Bucket_Size;
constexpr size_t map_size = U_Bucket_Size / map_ratio;

const Key_t SENTINEL = -2;
const Key_t INVALID = -1;
const Finger_t ALL_INVALID_Fingerprint = -1;
const Value_t NONE = 0x0;
struct DRAM_Index_Segment;

typedef struct Pair
{
	Key_t key;
	Value_t value;
} Pair;

typedef struct write_locate
{
	void give_val(uint8_t thread, uint32_t log, uint16_t page)
	{
		threadnum = thread;
		pagenum = page;
		lognum = log;
	}
	uint8_t threadnum; // 线程数
	uint16_t lognum;   // 页内第几个log
	uint32_t pagenum;  // 第几页
} write_locate;

typedef struct Merge_Bucket
{
	Pair m_bucket[15];
	Version_t Version; // 8B作为版本控制，全局版本号，同时作为finish_flag 前32位作为shadow log的查看。
	uint8_t flag;	   // 最后4位作为计数器，头一位作为是否被刷写，第二位作为是否被占用,第三位作为锁  把锁搬出去？？
	// bool used; //1B作为是否被占用
	// bool is_flush; //1B作为是否被刷写
	uint8_t threadnum;
	uint16_t lognum;
	uint32_t pagenum; // write_locate，正常为0
} M_Bucket;

typedef struct Sorted_Bucket
{
	Pair s_bucket[S_Bucket_Size]; // 有序区的数据无需版本号,代替pair用的
} S_Bucket;

typedef struct Unsorted_Bucket
{
	M_Bucket u_bucket[U_Bucket_Size]; // 无序区64个一个桶 dram级别加入无序区位图
} U_Bucket;

typedef struct bitmap_t
{
	// int16_t map[U_Bucket_Size]; // bitmap是否有用存疑
	uint num[map_size]; // 1kb一个NUM 初始全为0，每刷写一个加一
						// int num;
} bitmap_t;

typedef struct Sorted_locate
{
	int32_t seg;
	unsigned short bucket;
	unsigned short num;
} s_locate;

typedef struct Log_Count
{
	uint8_t count;
	uint8_t threadnum;
	uint16_t lognum;
	uint32_t pagenum;
} Log_Count;

typedef struct DRAM_Index_Bucket // 需要初始化
{
	Pair bucket[D_Bucket_Size];			   // 小索引每个桶的大小，不存在本地深度
	write_locate u_address[D_Bucket_Size]; // 相应无序区位置每个*16+i
	Log_Count count[D_Bucket_Size];
	int16_t dram_num;	// dram目前的num
	int16_t sorted_num; // 有序区目前的num
	uint8_t flush_num;
	mutex B_Lock;
	// s_locate s_point;  //刷写
} D_I_Bucket;

typedef struct DRAM_Index_Segment
{
	D_I_Bucket bucket[kBucketNum];
	int32_t S_depth;
	struct DRAM_Index_Segment *copy_ptr1;
	struct DRAM_Index_Segment *copy_ptr2;
	Finger_t **copy_Finger1;
	Finger_t **copy_Finger2;
	size_t finish; // finish for split
	Version_t version;
	bool S_Lock;
} D_I_Segment;

typedef struct DRAM_Index
{
	D_I_Segment **seg;
	bool D_Lock;
	int32_t D_depth;
} D_Index;

// template <typename T>
// class SafeQueue
// {
// public:
// 	void init()
// 	{
// 		for (int i = 0; i < LISTNUM; i++)
// 		{
// 			queue<T> test;
// 			lists.push_back(test);
// 		}
// 		in_num = 0;
// 		out_num = 0;
// 	}
// 	void range_push(int8_t level, int from, int to)
// 	{
// 		auto gap = (to - from) / LISTNUM;
// 		for (int m = 0; m < LISTNUM; m++)
// 		{
// 			lock(m);
// 			for (int i = from + gap * m; i < from + gap * (m + 1); i++)
// 			{
// 				for (int j = 0; j < U_Bucket_Size; j++)
// 				{
// 					T s;
// 					s.give_val(level, i, j);
// 					lists[m].push(s);
// 				}
// 			}
// 			unlock(m);
// 		}
// 	}
// 	void push(int8_t level, int32_t bucket, int16_t num)
// 	{
// 		T s;
// 		s.give_val(level, bucket, num);
// 		int val = in_num;
// 		while (!CAS(&in_num, &val, (val + 1) % LISTNUM))
// 		{
// 			val = in_num;
// 		}
// 		lock(val);
// 		lists[val].push(s);
// 		unlock(val);
// 	}
// 	void pop(int8_t &level, int32_t &bucket, int16_t &num)
// 	{
// 		int val = out_num;
// 		while (!CAS(&out_num, &val, (val + 1) % LISTNUM))
// 		{
// 			val = out_num;
// 		}
// 		lock(val);
// 		T r = lists[val].front();
// 		lists[val].pop();
// 		unlock(val);
// 		level = r.level;
// 		bucket = r.bucket;
// 		num = r.num;
// 	}
// 	bool empty()
// 	{
// 		return lists[0].empty();
// 	}
// 	void unlock(int num)
// 	{
// 		bool val = l_val[num];
// 		CAS(&l_val[num], &val, false);
// 	}
// 	void lock(int num)
// 	{
// 		bool val = l_val[num];
// 		do
// 		{
// 			while (val == true)
// 			{ // 等到不被占用
// 				std::this_thread::yield();
// 				val = l_val[num];
// 			}
// 		} while (!CAS(&l_val[num], &val, true));
// 	}

// private:
// 	bool l_val[LISTNUM];
// 	int in_num, out_num;
// 	vector<queue<T>> lists;
// };

typedef struct latency_retry
{
	double latency;
	size_t insert_retry;
	size_t dram_retry;
	size_t split_retry;
	size_t num;
} l_r;

typedef struct Bloom_Filter
{
	uint64_t filter[64];
	void set(Key_t key);
	bool get(Key_t key);
} Bloom_Filter;

struct splitarg
{
	splitarg(void) {}
	~splitarg(void) {}
	splitarg(Key_t key1, size_t loc1)
	{
		key = key1;
		loc = loc1;
	}
	Key_t key;
	size_t loc;
};

class ElimDA;
struct Directory;
struct Segment;
struct Unsorted_Area;
POBJ_LAYOUT_BEGIN(HashTable);
POBJ_LAYOUT_ROOT(HashTable, ElimDA);
POBJ_LAYOUT_TOID(HashTable, struct Sorted_Segment);
POBJ_LAYOUT_TOID(HashTable, TOID(struct Sorted_Segment));
POBJ_LAYOUT_TOID(HashTable, struct Directory);
// POBJ_LAYOUT_TOID(HashTable, struct cache_lock);
POBJ_LAYOUT_TOID(HashTable, struct Pair);
POBJ_LAYOUT_TOID(HashTable, struct cache_merge_bucket);
POBJ_LAYOUT_TOID(HashTable, TOID(M_Bucket));
POBJ_LAYOUT_TOID(HashTable, U_Bucket);
POBJ_LAYOUT_TOID(HashTable, PMEMmutex)
POBJ_LAYOUT_TOID(HashTable, mutex)
POBJ_LAYOUT_TOID(HashTable, TOID(U_Bucket));
POBJ_LAYOUT_TOID(HashTable, TOID_POINT(TOID(U_Bucket)));
POBJ_LAYOUT_TOID(HashTable, struct Unsorted_Area);
POBJ_LAYOUT_TOID(HashTable, M_Bucket);
POBJ_LAYOUT_TOID(HashTable, write_locate);
POBJ_LAYOUT_TOID(HashTable, int64_t);
POBJ_LAYOUT_TOID(HashTable, char);
POBJ_LAYOUT_END(HashTable);

struct In_DRAM
{
	void initemptylist(size_t);
	void initDRAM_Index(size_t);
	void initbitmap(size_t);
	void initFilter(size_t);

	char valt = 0;
	bool isback = false;
	queue<write_locate> emptylist[MAX_THREAD];
	D_Index *DRAM_Index;
	// Bloom_Filter **Filter;
	bitmap_t ***bitmap;
};

struct Unsorted_Area
{
	Unsorted_Area();
	~Unsorted_Area();
	void initUnsorted(PMEMobjpool *, size_t);
	TOID_ARRAY(TOID_POINT(TOID(U_Bucket)))
	unsorted_bucket;
	size_t numThread;
	size_t used[MAX_THREAD];
};

struct cache_merge_bucket
{
	TOID(M_Bucket)
	merge_bucket; // merge_bucket+alterbucket
	TOID(int64_t)
	bucket_point; // 记录当前对应的
	int64_t size; // 上次有2的多少次方个桶
	int64_t max;  // 最多的桶，恢复时用于从max开始往后

	cache_merge_bucket(void) {}
	~cache_merge_bucket(void) {}

	void init_bucket(PMEMobjpool *, int64_t);

	void lock_bucket()
	{
		// dlock_init(D_RW(dmutex), sizeof(mutex) * (pow(2, size)), 0, 0);
		// dlock_init(D_RW(merge_bucket), sizeof(M_Bucket) * (pow(2, size) / BUCKET_AMPLIFY + pow(2, size)), 0, 0);
		// dlock_init(D_RW(finish_flag), sizeof(int8_t) * (pow(2, size) / BUCKET_AMPLIFY + pow(2, size)), 0, 0);
		// dlock_init(D_RW(bucket_point), sizeof(int16_t) * (pow(2, size) / BUCKET_AMPLIFY + pow(2, size)), 0, 0);
	}
	void unlock_bucket()
	{
		dlock_exit();
	}

	void Recovery()
	{ // 未完成
		;
	}
}; //____cacheline_aligned;加入缓存行对齐？

struct Sorted_Segment
{
	// static const size_t kNumSlot = kSegmentSize/sizeof(Pair);  //一个桶1024个条目 一个条目4KB可以放256条

	Sorted_Segment(void) {}
	~Sorted_Segment(void) {}

	void initSortedSegment(size_t depth, PMEMobjpool *pop)
	{						 // 去掉了对每个段初始化为INVALID
		local_depth = depth; // 与DRAM同步
	}

	S_Bucket sorted_bucket[kBucketNum];
	size_t local_depth;
	TOID(struct Sorted_Segment)
	copy_ptr1;
	TOID(struct Sorted_Segment)
	copy_ptr2;
};

struct Directory
{
	static const size_t kDefaultDepth = 10; // 哈希表最大容量2的10次方个段
	TOID_ARRAY(TOID(struct Sorted_Segment))
	segment;
	size_t capacity;
	size_t depth;

	Directory(void) {}
	~Directory(void) {}

	void initDirectory(void)
	{
		depth = kDefaultDepth;
		capacity = pow(2, depth);
	}

	void initDirectory(size_t _depth)
	{
		depth = _depth;
		capacity = pow(2, _depth);
	}
};

class ElimDA
{
public:
	ElimDA(void) {}
	~ElimDA(void) {}
	void initElimDA(PMEMobjpool *, size_t, int);
	void initValuePool(PMEMobjpool *, int threadnum, int valuenum, int length); // valuenum为value的数量 放在initElimDA后
	void initFingerprint();
	void final_exit(); /// 用于释放malloc的vector与fingerprint
	void initdram();
	void Recovery(PMEMobjpool *);
	void Rebuild();
	void Insert(Key_t, Value_t, int, ThreadPool *);
	void Update(Key_t, Value_t);
	void split(size_t, size_t);
	Value_t Get(Key_t);
	bool Get4Split(Key_t, bool, int, int);
	bool Get4New(Key_t, D_I_Bucket *, Pair *, Finger_t *);
	void flush2unsorted(int);
	void insertDRAM_Index(Key_t, Value_t, uint8_t, uint32_t, uint16_t, int);
	void flush2sorted(size_t, unsigned short, unsigned short, bool);
	void struct_rebuild(int, int, Version_t ***);
	void unsorted_rebuild(int, Version_t ***, int, int);
	void range_unsorted_rebuild(int, int, Version_t ***);
	// DEBUG
	void init_backup(int num);
	void AllocLog(PMEMobjpool *, size_t);

	void compress()
	{ // 当满了之后显式调用compress 未完成
		cout << "full!" << endl;
		exit(-1);
	}
	void unlock(bool &Lock)
	{
		// bool val = Lock;
		__atomic_store_n(&Lock, false, __ATOMIC_RELEASE);
		// if(Lock==true)
		// 	cout << "error!!" << endl;
	}
	void lock(bool &Lock)
	{
		bool val = Lock;
		do
		{
			while (val == true)
			{ // 等到不被占用
				val = Lock;
			}
		} while (!CAS(&Lock, &val, true));
	}
	bool lock(bool &Lock, bool *S_Lock)
	{
		bool val = Lock;
		do
		{
			while (val == true)
			{ // 等到不被占用
				val = Lock;
				if (*S_Lock == true)
					return false;
			}
		} while (!CAS(&Lock, &val, true));
		return true;
	}
	bool try_lock(bool &Lock)
	{
		bool val = Lock;
		if (val)
		{
			return false; // lock失败
		}
		else
		{
			return CAS(&Lock, &val, true);
		}
	}
	void getinfo(size_t num)
	{
		size_t capacity = pow(2, in_dram->DRAM_Index->D_depth);
		size_t cnt = 0;
		for (int i = 0; i < capacity; cnt++)
		{
			auto target = in_dram->DRAM_Index->seg[i];
			int stride = pow(2, in_dram->DRAM_Index->D_depth - target->S_depth);
			i += stride;
		}
		cout << "cnt:\t" << cnt << endl;
		cnt = cnt * (S_Bucket_Size + D_Bucket_Size) * kBucketNum + MERGEBUCKET_SIZE * 16;
		cout << "num:\t" << num << "\tcnt:\t" << cnt << endl;
		cout << "loadfactor:\t" << num / (double)cnt << "\n"
			 << endl;
	}

	bool crashed = true;
	PMEMobjpool *pop;
	TOID(struct cache_merge_bucket)
	cache_bucket;
	TOID(struct Directory)
	dir;
	struct Directory *direct_dir;
	// TOID(struct cache_lock) c_lock;
	TOID(struct Unsorted_Area)
	_unsorted_area;
	struct Unsorted_Area *unsorted_area;
	TOID(struct Sorted_Segment) * _segment;
	TOID_ARRAY(char)
	_ValuePool[32];
	char **ValuePool;
	int *PoolLocate;
	M_Bucket *_merge_bucket;
	// TOID(M_Bucket)* _alter_bucket;
	int64_t *_bucket_point;
	PMEMmutex *_mutex;
	In_DRAM *in_dram;
	Finger_t ***Fingerprint;			   // 最小一个256
	TOID(struct Sorted_Segment) * _backup; // 用于测试是否是分配问题
	Version_t global_version = 0;
	int _backup_num = 0;
	struct timespec start, end;
	uint64_t elapsed;
	size_t STRUCTURETHREAD = 4;
	int test = 0;
	int valuelength = 16;
	int isvariable = 0;

private:
	// TOID(struct Directory) dir;
};

double space_consumption(PMEMobjpool *pop);

#endif
