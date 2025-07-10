
#include <iostream>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include <stdio.h>
#include <vector>
#include <x86intrin.h>
#include "src/ElimDA.h"
#include "src/hash.h"
#include "../pcm/cpucounters.h"

#define f_seed 0xc70697UL
#define s_seed 0xc70697UL // s_seed取多少
#define INVALID_Finger 65535
// #define ADR

#define MAX_LOG (65536*2) // 4096  7千多万 65536 4亿  对应pagenum
#define INIT_LOG 1
#define MERGE_BUCKET _merge_bucket[_bucket_point[i_bucket]] // ibucket看看能不能改成带参数
#define ALTER_BUCKET _merge_bucket[_bucket_point[a_bucket]]
#define MUTEX _mutex
#define DMUTEX (D_RW(D_RW(cache_bucket)->dmutex))
// #define f_seed 0xc70f6907UL
// #define s_seed 0xc70f6907UL
#define LOCK_SPACE (D_RW(D_RW(c_lock)->lock_space))
#define LOCK_POINT (D_RW(D_RW(c_lock)->lock_point))
#define HASH_CHOICE 1
#define SYNC_ON_WRITE                                                                                                                                                                 \
	do                                                                                                                                                                                \
	{                                                                                                                                                                                 \
		if (f_hash & pattern)                                                                                                                                                         \
		{                                                                                                                                                                             \
			now_bucket = &(now_seg->copy_ptr2->bucket[loc]);                                                                                                                          \
		}                                                                                                                                                                             \
		else                                                                                                                                                                          \
		{                                                                                                                                                                             \
			now_bucket = &(now_seg->copy_ptr1->bucket[loc]);                                                                                                                          \
		}                                                                                                                                                                             \
		if (now_bucket->dram_num == D_Bucket_Size)                                                                                                                                    \
		{                                                                                                                                                                             \
			auto Finger = &(now_seg->copy_Finger2[loc][now_bucket->sorted_num]);                                                                                                      \
			auto sorted_bucket = &(D_RW(D_RW(_segment[x])->copy_ptr2)->sorted_bucket[loc].s_bucket[now_bucket->sorted_num]);                                                          \
			if (D_RW(D_RW(_segment[x])->copy_ptr2) == NULL)                                                                                                                           \
				goto TRY_INSERT;                                                                                                                                                      \
			memcpy(sorted_bucket, now_bucket->bucket, 16 * D_Bucket_Size);                                                                                                            \
			/*pmemobj_memcpy(pop, sorted_bucket,now_bucket->bucket, 256,MEMCPY_FLAG);*/                                                                                               \
			for (int i = 0; i < D_Bucket_Size; i++)                                                                                                                                   \
			{                                                                                                                                                                         \
				Finger[i] = now_bucket->bucket[i].key >> 24;                                                                                                                          \
				if (now_bucket->u_address[i].threadnum != INVALID_THREAD)                                                                                                             \
				{                                                                                                                                                                     \
					int r = check_and_add(now_bucket->count, now_bucket->u_address[i]);                                                                                               \
					if (r >= 0)                                                                                                                                                       \
					{                                                                                                                                                                 \
						uint _ = 0;                                                                                                                                                   \
						do                                                                                                                                                            \
						{                                                                                                                                                             \
							_ = in_dram->bitmap[now_bucket->count[r].threadnum][now_bucket->count[r].pagenum]->num[0];                                                                \
						} while (!CAS(&in_dram->bitmap[now_bucket->count[r].threadnum][now_bucket->count[r].pagenum]->num[0], &_, _ + now_bucket->count[r].count));                   \
						if (_ + now_bucket->count[r].count == U_Bucket_Size * 15)                                                                                                     \
						{                                                                                                                                                             \
							cout << "ifree:\t" << (int)now_bucket->count[r].threadnum << "\t" << (int)now_bucket->count[r].pagenum << "\t" << _ + now_bucket->count[r].count << endl; \
							free(in_dram->bitmap[now_bucket->count[r].threadnum][now_bucket->count[r].pagenum]);                                                                      \
							in_dram->bitmap[now_bucket->count[r].threadnum][now_bucket->count[r].pagenum] = NULL;                                                                     \
							POBJ_FREE(&(D_RW(D_RW(unsorted_area->unsorted_bucket)[now_bucket->count[r].threadnum]))[now_bucket->count[r].pagenum].oid);                               \
						}                                                                                                                                                             \
					}                                                                                                                                                                 \
				}                                                                                                                                                                     \
			}                                                                                                                                                                         \
			/*flush - adress*/                                                                                                                                                        \
			now_bucket->sorted_num += D_Bucket_Size;                                                                                                                                  \
			now_bucket->dram_num = 0;                                                                                                                                                 \
		}                                                                                                                                                                             \
		now_bucket->bucket[now_bucket->dram_num].key = key;                                                                                                                           \
		now_bucket->bucket[now_bucket->dram_num].value = value;                                                                                                                       \
		now_bucket->u_address[now_bucket->dram_num].threadnum = threadnum;                                                                                                            \
		now_bucket->u_address[now_bucket->dram_num].pagenum = pagenum;                                                                                                                \
		now_bucket->u_address[now_bucket->dram_num].lognum = lognum * 16 + i;                                                                                                         \
		now_bucket->dram_num++;                                                                                                                                                       \
	} while (0)

using namespace std;
using namespace pcm;

inline void pmem_persist(const void *addr, const size_t len)
{
	char *addr_ptr = (char *)addr;
	char *end_ptr = addr_ptr + len;
	for (; addr_ptr < end_ptr; addr_ptr += 64)
	{
		_mm_clwb((void *)addr_ptr);
	}
	_mm_sfence();
}

int mmap_pmem_file(const std::string &filename, size_t max_size, char **target)
{
	int fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
	if (fd < 0)
	{
		throw std::runtime_error("Could not open file at storage location: " + filename);
	}

	if ((errno = ftruncate(fd, max_size)) != 0)
	{
		throw std::runtime_error(
			"Could not allocate " + std::to_string(max_size) + " bytes at storage location: " + filename);
	}

	*target = static_cast<char *>(mmap(nullptr, max_size, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE | MAP_POPULATE, fd, 0));

	madvise(target, max_size, MADV_SEQUENTIAL);
	return fd;
}

int check_and_add(Log_Count *count, write_locate w)
{
	for (int i = 0; i < Log_Count_Size; i++)
	{
		if (count[i].threadnum == w.threadnum && count[i].pagenum == w.pagenum && (count[i].lognum >> 4) == (w.lognum >> 4))
		{
			count[i].count++;
			if (count[i].count == 255) // 满了
				return i;
			else
				return -1;
		}
		else if (count[i].threadnum == INVALID_THREAD)
		{
			count[i].threadnum = w.threadnum;
			count[i].pagenum = w.pagenum;
			count[i].lognum = w.lognum >> 4;
			count[i].count = 1;
			return -1;
		}
	}
	return -2; // 需要驱逐
}
void Bloom_Filter::set(Key_t key)
{
	uint16_t f0 = (hash_funcs[0](&key, sizeof(Key_t), f_seed) >> 8) & 2047;
	uint16_t f1 = (hash_funcs[1](&key, sizeof(Key_t), f_seed) >> 8) & 2047;
	uint16_t f2 = (hash_funcs[2](&key, sizeof(Key_t), s_seed) >> 8) & 2047;
	uint16_t f3 = (hash_funcs[3](&key, sizeof(Key_t), s_seed) >> 8) & 2047;
	filter[f0 / 64] |= (1 >> (f0 % 64));
	filter[f1 / 64] |= (1 >> (f1 % 64));
	filter[f2 / 64] |= (1 >> (f2 % 64));
	filter[f3 / 64] |= (1 >> (f3 % 64));
}
bool Bloom_Filter::get(Key_t key)
{
	uint16_t f0 = (hash_funcs[0](&key, sizeof(Key_t), f_seed) >> 8) & 2047;
	uint16_t f1 = (hash_funcs[1](&key, sizeof(Key_t), f_seed) >> 8) & 2047;
	uint16_t f2 = (hash_funcs[2](&key, sizeof(Key_t), s_seed) >> 8) & 2047;
	uint16_t f3 = (hash_funcs[3](&key, sizeof(Key_t), s_seed) >> 8) & 2047;
	if ((filter[f0 / 64] >> (f0 % 64)) % 2 != 0 && (filter[f1 / 64] >> (f1 % 64)) % 2 != 0 && (filter[f2 / 64] >> (f2 % 64)) % 2 != 0 && (filter[f3 / 64] >> (f3 % 64)) % 2 != 0)
		return true;
	return false;
}

void cache_merge_bucket::init_bucket(PMEMobjpool *pop, int64_t num)
{
	// TODO maybe delete?
	POBJ_ZALLOC(pop, &merge_bucket, M_Bucket, sizeof(M_Bucket) * (num + ALTER_NUM));
	POBJ_ALLOC(pop, &bucket_point, int64_t, sizeof(int64_t) * (num + ALTER_NUM), NULL, NULL);
	// POBJ_ZALLOC(pop, &pmutex, PMEMmutex, sizeof(M_Bucket)*num);
	// POBJ_ZALLOC(pop, &busy_mutex, bool, sizeof(bool)*num);
	// POBJ_ZALLOC(pop, &dmutex, mutex, sizeof(M_Bucket)*num);
	// dlock_init(D_RW(merge_bucket), sizeof(M_Bucket) * num, 0, 0);
	size = log2(num);
	for (int i = 0; i < (num + ALTER_NUM); i++)
	{
		D_RW(bucket_point)
		[i] = i;
	}
	// lock_bucket();
}

void Unsorted_Area::initUnsorted(PMEMobjpool *pop, size_t _numThread)
{
	numThread = _numThread;
	POBJ_ALLOC(pop, &unsorted_bucket, TOID_POINT(TOID(U_Bucket)), sizeof(TOID_POINT(TOID(U_Bucket))) * numThread, NULL, NULL);
	for (int i = 0; i < numThread; i++)
	{
		POBJ_ALLOC(pop, &(D_RW(unsorted_bucket)[i]), TOID(U_Bucket), sizeof(TOID(U_Bucket)) * MAX_LOG / numThread, NULL, NULL);
		for (int j = 0; j < INIT_LOG; j++)
			POBJ_ALLOC(pop, &(D_RW(D_RW(unsorted_bucket)[i]))[j], U_Bucket, sizeof(U_Bucket), NULL, NULL); // alloc 16 MB
		used[i] = INIT_LOG;
	}
}
void ElimDA::AllocLog(PMEMobjpool *pop, size_t thread_num)
{
	POBJ_ALLOC(pop, &(D_RW(D_RW(unsorted_area->unsorted_bucket)[thread_num]))[unsorted_area->used[thread_num]], U_Bucket, sizeof(U_Bucket), NULL, NULL); // alloc 16 MB
	in_dram->bitmap[thread_num][unsorted_area->used[thread_num]] = (bitmap_t *)malloc(sizeof(bitmap_t));
	in_dram->bitmap[thread_num][unsorted_area->used[thread_num]]->num[0] = 0;
	for (int j = 0; j < U_Bucket_Size; j++)
	{
		write_locate w;
		w.give_val(thread_num, j, unsorted_area->used[thread_num]);
		in_dram->emptylist[thread_num].push(w); // 不能用q
	}
	unsorted_area->used[thread_num]++;
	if (unsorted_area->used[thread_num] >= MAX_LOG)
	{
		unsorted_area->used[thread_num] = 0; // 重复
	}
}

void In_DRAM::initbitmap(size_t numThread)
{
	// 一个bitmap一个U_bucket  一个指针数组
	bitmap = (bitmap_t ***)malloc(sizeof(bitmap_t **) * numThread);
	for (int i = 0; i < numThread; i++)
	{
		bitmap[i] = (bitmap_t **)malloc(MAX_LOG / numThread * sizeof(bitmap_t *));
		for (int k = 0; k < INIT_LOG; k++)
		{
			bitmap[i][k] = (bitmap_t *)malloc(sizeof(bitmap_t));
			for (int j = 0; j < map_size; j++)
				bitmap[i][k]->num[j] = 0;
		}
	}
}

void In_DRAM::initemptylist(size_t numThread)
{ // 第0层，从0开始push到initCap
	for (int i = 0; i < numThread; i++)
	{
		// queue<write_locate> q;
		for (int k = 0; k < INIT_LOG; k++)
		{
			for (int j = 0; j < U_Bucket_Size; j++)
			{
				write_locate w;
				w.give_val(i, j, k);
				emptylist[i].push(w); // 不能用q
			}
		}
	}
}

void In_DRAM::initDRAM_Index(size_t initCap)
{ // 多少个段
	DRAM_Index = new D_Index();
	DRAM_Index->D_depth = log2(initCap);
	DRAM_Index->seg = (D_I_Segment **)malloc(sizeof(D_I_Segment *) * initCap); // 分配n个段指针

	for (int i = 0; i < initCap; ++i)
	{
		// std::cout << "bucket size" << sizeof(D_I_Bucket) << std::endl;
		// std::cout << "malloc size" <<  sizeof(D_I_Segment) << "initcap:" << initCap << std::endl;
		DRAM_Index->seg[i] = (D_I_Segment *)calloc(1, sizeof(D_I_Segment)); // 每个段指针分配一个段的大小
		DRAM_Index->seg[i]->S_depth = log2(initCap);
		for (int j = 0; j < kBucketNum; j++)
		{
			for (int k = 0; k < D_Bucket_Size; k++)
			{
				DRAM_Index->seg[i]->bucket[j].count[k].threadnum = INVALID_THREAD;
			}
			// unique_lock<mutex> lck(DRAM_Index->seg[i]->bucket[j].B_Lock, defer_lock);
			// 	if(!lck.try_lock()){
			// 		std::cout << "lock fail" << std::endl;
			// 	}else{
			// 		std::cout << "test" << std::endl;
			// 		lck.unlock();
			// 	}
		}
	}
}

void In_DRAM::initFilter(size_t initCap)
{
	// Filter = (Bloom_Filter **)malloc(sizeof(Bloom_Filter *) * initCap);
	// for (int i = 0; i < initCap; ++i)
	// {
	// 	Filter[i] = (Bloom_Filter *)calloc(kBucketNum, sizeof(Bloom_Filter));
	// }
}

void ElimDA::Insert(Key_t key, Value_t value, int a_bucket, ThreadPool *pool)
{
	int threadnum = a_bucket - MERGEBUCKET_SIZE;
	if (isvariable)
	{
		// cout << "test" << (void *)ValuePool[threadnum] << endl;
		memcpy(&ValuePool[threadnum][PoolLocate[threadnum]], value, valuelength);
		// cout << "test2:" << endl;
		value = &ValuePool[threadnum][PoolLocate[threadnum]];
		PoolLocate[threadnum] += valuelength;
	}
	uint64_t f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
	uint8_t i_bucket = (f_hash >> (64 - D_RO(cache_bucket)->size)); // i_bucket有问题？？
	// i_bucket = threadnum * 8;
	int8_t num = MERGE_BUCKET.flag; // 若直接获取 &get_num则会导致CAS不一样无法交换
									// MERGE_BUCKET.flag++;
RETRY:
	do
	{
		__atomic_store_n(&num, MERGE_BUCKET.flag, __ATOMIC_RELEASE);
		if ((num & get_num) >= 15)
		{
			goto RETRY;
		}
	} while (!CAS(&MERGE_BUCKET.flag, &num, num + 1));
	num = num & get_num;
	num = (num % 4) * 4 + num / 4;
	MERGE_BUCKET.m_bucket[num].value = value;
	MERGE_BUCKET.m_bucket[num].key = key;
#ifdef ADR
	pmem_persist(&MERGE_BUCKET.m_bucket[num], 16);
#endif
	int64_t _ = MERGE_BUCKET.Version;
	do
	{
		_ = MERGE_BUCKET.Version;
	} while (!CAS(&MERGE_BUCKET.Version, &_, _ | (1 << num))); // Version作为是否complete的计算
	if (num == 11)
	{ // 即插入之后满了 11相当于原来14
		// MERGE_BUCKET.flag = 0;
		while ((MERGE_BUCKET.Version & ((1UL << 16) - 1)) != allfinish)
		{				// 仅剩一个15
			asm("nop"); // 这里要不换成最后一个完成的线程就行？不用等待
		}
		if (in_dram->emptylist[threadnum].empty())
		{
			AllocLog(pop, threadnum);
		}
		// 要写入的log的地方正常为0
		write_locate w;
		w = in_dram->emptylist[threadnum].front();
		in_dram->emptylist[threadnum].pop();

		MERGE_BUCKET.threadnum = w.threadnum;
		MERGE_BUCKET.pagenum = w.pagenum;
		MERGE_BUCKET.lognum = w.lognum;

		Version_t _v = 0; // TODO 换成桶版本号
		_v = chrono::duration_cast<chrono::nanoseconds>(chrono::system_clock::now().time_since_epoch()).count();
		// do
		// {
		// 	__atomic_store_n(&_v, global_version, __ATOMIC_RELEASE);
		// } while (!CAS(&global_version, &_v, _v + 1));

		int64_t _p = _bucket_point[i_bucket];
		ALTER_BUCKET.Version += (unsigned long)a_bucket << 32;								   // a_bucket一般不会为0因为他在所有merge_bucket后面 这时候正在刷写至DRAM内
		__atomic_store_n(&_bucket_point[i_bucket], _bucket_point[a_bucket], __ATOMIC_RELEASE); // 用a_bucket替换当前
		_bucket_point[a_bucket] = _p;
		ALTER_BUCKET.Version = _v; // 目前已换成shadow

		for (int i = 0; i < 15; i++)
		{
			insertDRAM_Index(ALTER_BUCKET.m_bucket[i].key, ALTER_BUCKET.m_bucket[i].value, ALTER_BUCKET.threadnum, ALTER_BUCKET.pagenum, ALTER_BUCKET.lognum, i);
		}
		flush2unsorted(a_bucket); // 这里pop
		// flush2unsorted(i_bucket); // 这里pop
		// for (int i = 0; i < 15; i++)
		// {
		// 	insertDRAM_Index(MERGE_BUCKET.m_bucket[i].key, MERGE_BUCKET.m_bucket[i].value, MERGE_BUCKET.threadnum, MERGE_BUCKET.lognum, MERGE_BUCKET.pagenum, i);
		// }
		_ = MERGE_BUCKET.Version;
		do
		{
			_ = MERGE_BUCKET.Version;
		} while (!CAS(&MERGE_BUCKET.Version, &_, _ & ((1UL << 32) - 1))); // 清空指向shadow
	}
	return; // test
			// DMUTEX[i_bucket].unlock();
}

void ElimDA::flush2unsorted(int a_bucket)
{
	ALTER_BUCKET.flag ^= set_flush; // 表示正在处于刷写状态
	// memcpy(&(D_RW(D_RW(D_RW(unsorted_area->unsorted_bucket)[ALTER_BUCKET.threadnum])[ALTER_BUCKET.pagenum])->u_bucket[ALTER_BUCKET.lognum]), &ALTER_BUCKET, 256);
	pmemobj_memcpy(pop, &(D_RW(D_RW(D_RW(unsorted_area->unsorted_bucket)[ALTER_BUCKET.threadnum])[ALTER_BUCKET.pagenum])->u_bucket[ALTER_BUCKET.lognum]), &ALTER_BUCKET, 256, MEMCPY_FLAG); // PMEMOBJ_F_MEM_TEMPORAL
	// cout << "flush1" << endl;
	// uint _ = 0;
	// do
	// {
	// 	_ = in_dram->bitmap[ALTER_BUCKET.threadnum][ALTER_BUCKET.pagenum]->num[ALTER_BUCKET.lognum / map_ratio];
	// } while (!CAS(&in_dram->bitmap[ALTER_BUCKET.threadnum][ALTER_BUCKET.pagenum]->num[ALTER_BUCKET.lognum / map_ratio], &_, _ + 15));

	// cout << "flush2" << endl;
	ALTER_BUCKET.Version = 0;
	ALTER_BUCKET.flag = ALTER_BUCKET.flag & reset_num; // reset_num & isflush
}

void ElimDA::Update(Key_t key, Value_t value)
{ // 还没有做到缓冲区
TRY_GET:
	uint64_t f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
	auto f_idx = (f_hash & kMask);																			  // 第几个桶
	auto x = (f_hash >> (8 * sizeof(f_hash) - in_dram->DRAM_Index->D_depth));								  // 不要采用1
	auto now_seg = in_dram->DRAM_Index->seg[x];																  // target是段指针，对于锁应该用指针targetpoint保留
	if (now_seg != in_dram->DRAM_Index->seg[(f_hash >> (8 * sizeof(f_hash) - in_dram->DRAM_Index->D_depth))]) // 目录分裂
	{
		std::this_thread::yield();
		goto TRY_GET;
	}
	auto loc = f_idx % kBucketNum;
	auto now_bucket = &(now_seg->bucket[loc]);
	auto s_num = now_bucket->sorted_num;					  // 根源根本不在Optane在DRAM
	x = (f_hash >> (8 * sizeof(f_hash) - direct_dir->depth)); // 若此时正处于目录分裂 dir在特征数组前替换
	auto snap_Finger = Fingerprint;
	auto Finger = snap_Finger[x][loc]; // 此时Finger被替换但是seg还没被换  替换顺序seg->Fingerprint->dramindex
	short key_mask = key >> 24 & Finger_Mask;
	auto snap_seg = D_RW(_segment[x]);
	unsigned int mask = 0;
	if (now_seg->S_Lock)
	{
		int gap1 = pow(2, in_dram->DRAM_Index->D_depth - now_seg->S_depth);
		int gap2 = pow(2, in_dram->DRAM_Index->D_depth - snap_seg->local_depth);
		if (snap_Finger[x - x % gap1] == snap_Finger[x - x % gap1 + gap2])
		{
			goto TRY_GET;
		}
	}
	for (int i = 0; i < s_num; i += 16)
	{
		SSE_CMP16(Finger + i, key_mask);
		if (mask != 0)
		{
			int j = 0;
			while (j < 16 && i + j < s_num)
			{ // 按照缓存行比较
				if (CHECK_BIT(mask, j) && i + j < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j].key == key))
				{
					D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j].value = value;
				}
				if (CHECK_BIT(mask, j + 1) && i + j + 1 < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 1].key == key))
				{
					D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 1].value = value;
				}
				if (CHECK_BIT(mask, j + 2) && i + j + 2 < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 2].key == key))
				{
					D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 2].value = value;
				}
				if (CHECK_BIT(mask, j + 3) && i + j + 3 < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 3].key == key))
				{
					D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 3].value = value;
				}
				j += 4;
			}
		}
	}
	if (Finger != Fingerprint[x][loc])
	{ // Fingerprint已经被替换
		goto TRY_GET;
	}
	auto d_num = now_bucket->dram_num;
	for (int i = d_num - 1; i >= 0; i--)
	{
		if (now_bucket->bucket[i].key == key)
		{
			now_bucket->bucket[i].value = value;
		}
	}
	uint8_t i_bucket = (f_hash >> (64 - D_RO(cache_bucket)->size)); // alter_bucket的怎么查？？
	auto num = MERGE_BUCKET.flag & get_num;
	for (int j = 0; j < num; j++)
	{
		if (MERGE_BUCKET.m_bucket[(j % 4) * 4 + j / 4].key == key)
		{
			MERGE_BUCKET.m_bucket[(j % 4) * 4 + j / 4].value = value;
		}
	}
}

Value_t ElimDA::Get(Key_t key)
{ // 插入时不搜索读取缓冲区，搜索时正在分裂，若没搜索到且分裂完成  //目前替代桶还没搜
TRY_GET:
	uint64_t f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
	auto f_idx = (f_hash & kMask);																						// 第几个桶
	auto x = (f_hash >> (8 * sizeof(f_hash) - in_dram->DRAM_Index->D_depth));											// 不要采用1
	auto now_seg = in_dram->DRAM_Index->seg[x];																			// target是段指针，对于锁应该用指针targetpoint保留
	if (unlikely(now_seg != in_dram->DRAM_Index->seg[(f_hash >> (8 * sizeof(f_hash) - in_dram->DRAM_Index->D_depth))])) // 目录分裂
	{
		std::this_thread::yield();
		goto TRY_GET;
	}
	auto loc = f_idx % kBucketNum;
	auto now_bucket = &(now_seg->bucket[loc]);
	auto s_num = now_bucket->sorted_num;					  // 根源根本不在Optane在DRAM
	x = (f_hash >> (8 * sizeof(f_hash) - direct_dir->depth)); // 若此时正处于目录分裂 dir在特征数组前替换
	auto snap_Finger = Fingerprint;
	auto Finger = snap_Finger[x][loc]; // 此时Finger被替换但是seg还没被换  替换顺序seg->Fingerprint->dramindex
	short key_mask = key >> 24 & Finger_Mask;
	auto snap_seg = D_RW(_segment[x]);
	unsigned int mask = 0;
	if (unlikely(now_seg->S_Lock))
	{
		int gap1 = pow(2, in_dram->DRAM_Index->D_depth - now_seg->S_depth);
		int gap2 = pow(2, in_dram->DRAM_Index->D_depth - snap_seg->local_depth);
		if (snap_Finger[x - x % gap1] == snap_Finger[x - x % gap1 + gap2])
		{
			goto TRY_GET;
		}
	}
	for (int i = 0; i < s_num; i += 16)
	{
		SSE_CMP16(Finger + i, key_mask);
		if (likely(mask != 0))
		{
			int j = 0;
			while (j < 16 && i + j < s_num)
			{ // 按照缓存行比较
				if (CHECK_BIT(mask, j) && i + j < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j].key == key))
				{
					return D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j].value;
				}
				if (CHECK_BIT(mask, j + 1) && i + j + 1 < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 1].key == key))
				{
					return D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 1].value;
				}
				if (CHECK_BIT(mask, j + 2) && i + j + 2 < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 2].key == key))
				{
					return D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 2].value;
				}
				if (CHECK_BIT(mask, j + 3) && i + j + 3 < s_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 3].key == key))
				{
					return D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 3].value;
				}
				j += 4;
			}
		}
	}
	if (Finger != Fingerprint[x][loc])
	{ // Fingerprint已经被替换
		goto TRY_GET;
	}
	auto d_num = now_bucket->dram_num;
	for (int i = d_num - 1; i >= 0; i--)
	{
		if (now_bucket->bucket[i].key == key)
		{
			return now_bucket->bucket[i].value;
		}
	}
	uint8_t i_bucket = (f_hash >> (64 - D_RO(cache_bucket)->size)); // alter_bucket的怎么查？？
	uint64_t now_ibucket = 0;
	do
	{
		now_ibucket = _bucket_point[i_bucket]; // 赋正确值
	} while (now_ibucket > (MERGEBUCKET_SIZE + ALTER_NUM));
	auto num = _merge_bucket[now_ibucket].flag & get_num;
	for (int j = 0; j < num; j++)
	{
		if (_merge_bucket[now_ibucket].m_bucket[(j % 4) * 4 + j / 4].key == key)
		{
			return _merge_bucket[now_ibucket].m_bucket[(j % 4) * 4 + j / 4].value;
		}
	}
	int shadow = _merge_bucket[now_ibucket].Version >> 32;
	if (shadow != 0 && shadow < (MERGEBUCKET_SIZE + ALTER_NUM))
	{
		// if (shadow > 2048) // 会不会被置换？
		// 	cout << "shadow:" << shadow << "Version" << _merge_bucket[now_ibucket].Version << endl;
		for (int j = 0; j < 15; j++)
		{
			if (_merge_bucket[_bucket_point[shadow]].m_bucket[(j % 4) * 4 + j / 4].key == key)
			{
				return _merge_bucket[_bucket_point[shadow]].m_bucket[(j % 4) * 4 + j / 4].value;
			}
		}
	}
	return NONE;
}

void ElimDA::insertDRAM_Index(Key_t key, Value_t value, uint8_t threadnum, uint32_t pagenum, uint16_t lognum, int i)
{															  // 加了cukoo性能下降
	auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed); // 若大于256，若已经分裂，即loc<finish，则不用管，因为会调用syncwrite写入，若大于则写入
	auto f_idx = (f_hash & kMask);							  // 第几个桶
	unsigned long x;
	D_I_Segment *now_seg;
TRY_INSERT:
	// x = f_hash >> (8 * sizeof(f_hash) - in_dram->DRAM_Index->D_depth);   //不能用
	// now_seg = in_dram->DRAM_Index->seg[x];
	__atomic_store_n(&x, (f_hash >> (8 * sizeof(f_hash) - in_dram->DRAM_Index->D_depth)), __ATOMIC_RELEASE);
	__atomic_store_n(&now_seg, (in_dram->DRAM_Index->seg[x]), __ATOMIC_RELEASE);
	unsigned short loc = f_idx % kBucketNum;
	auto now_bucket = &(now_seg->bucket[loc]); // 确保获取不是旧段 若释放旧段锁则插入也是旧段！！
	if (now_seg->S_Lock == true)
	{ // 与SYNC_ON_WRITE共轭
		std::this_thread::yield();
		goto TRY_INSERT; // 确保不获取旧段！！
	}
	// if(now_bucket->B_Lock.try_lock())
	// {
	// 	std::cout << "test"  << std::endl;
	// 	now_bucket->B_Lock.unlock();
	// }
	unique_lock<mutex> lck(now_bucket->B_Lock, defer_lock); // 不知道为什么分裂会有true的情况！！若split中改变不获取seg锁
	while (!lck.try_lock())
	{
		if (now_seg != in_dram->DRAM_Index->seg[x])
		{
			lck.release();
			goto TRY_INSERT;
		}
	}
	// cout << "lock success\t"  << x << "\t" << loc << "\t" << now_bucket->dram_num << endl;
	auto target_check = (f_hash >> (8 * sizeof(f_hash) - direct_dir->depth));
	if (now_seg != in_dram->DRAM_Index->seg[target_check])
	{ // 当时正在分裂目录则可能获取错误的锁，释放
		lck.unlock();
		// std::this_thread::yield();  //让渡时间片
		goto TRY_INSERT;
	}
	// *value=loc;    //修改value值
TRY_SPLIT:
	if (now_bucket->dram_num == D_Bucket_Size)
	{ // 16时会有bug
		if (now_bucket->sorted_num >= S_Bucket_Size)
		{ // 第一个先不

			if (now_seg->copy_Finger2 != NULL || (!try_lock(now_seg->S_Lock)))
			{
				// if(false){
				lck.unlock(); // 让渡给要分裂的段
				goto TRY_INSERT;
			}
			split(f_hash, loc); // 判断目录这时候在分裂，则释放段锁  //若没有清空则需要检测是否有一个是满的，这里已经清空了  //test
			goto TRY_INSERT;
		}
		else
		{
			if (loc < now_seg->finish)
			{
				flush2sorted(f_hash, loc, now_bucket->sorted_num, false); // 包括了unlock
			}
			else
			{
				flush2sorted(f_hash, loc, now_bucket->sorted_num, true); // 包括了unlock
			}
			now_bucket->bucket[now_bucket->dram_num].key = key;
			now_bucket->bucket[now_bucket->dram_num].value = value;
			now_bucket->u_address[now_bucket->dram_num].threadnum = threadnum; // 在插入的时候基本很少发生争用，不像cache_bucket中
			now_bucket->u_address[now_bucket->dram_num].pagenum = pagenum;
			now_bucket->u_address[now_bucket->dram_num].lognum = lognum * 16 + i; // 最多不超过4096
			now_bucket->dram_num++;
			if (loc < now_seg->finish)
			{ // sync on write
				// cout << "flush!" << endl;
				auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - now_seg->S_depth - 1));
				// SYNC_ON_WRITE; // 确认在同步时桶没被替换
			}
			// unlock(now_bucket->B_Lock);
			return;
		}
	}
	else
	{
		now_bucket->bucket[now_bucket->dram_num].key = key;
		now_bucket->bucket[now_bucket->dram_num].value = value;
		now_bucket->u_address[now_bucket->dram_num].threadnum = threadnum; // 在插入的时候基本很少发生争用，不像cache_bucket中
		now_bucket->u_address[now_bucket->dram_num].pagenum = pagenum;
		now_bucket->u_address[now_bucket->dram_num].lognum = lognum * 16 + i; // 最多不超过4096
		now_bucket->dram_num++;
		if (loc < now_seg->finish)
		{ // sync on write
			auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - now_seg->S_depth - 1));
			// SYNC_ON_WRITE;
		}
		// unlock(now_bucket->B_Lock);
		return;
	}

	// auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
	// auto s_idx = (s_hash & kMask);

	// loc = s_idx % kBucketNum;
	// now_bucket = &now_seg->bucket[loc];

	// val = now_bucket->B_Lock; // 不知道为什么分裂会有true的情况！！若split中改变不获取seg锁
	// do
	// {
	// 	while (val == true)
	// 	{ // 等到不被占用
	// 		val = now_bucket->B_Lock;
	// 		if (now_seg != in_dram->DRAM_Index->seg[x])
	// 			goto TRY_INSERT;
	// 	}
	// } while (!CAS(&now_bucket->B_Lock, &val, true));

	// if (now_bucket->dram_num == D_Bucket_Size)
	// { // 16时会有bug
	// 	if (now_bucket->sorted_num >= S_Bucket_Size)
	// 	{ // 第一个先不
	// 		// 加一个如果有old则等待old分裂完
	// 		if (now_seg->copy_Finger2 != NULL || (!try_lock(now_seg->S_Lock)))
	// 		{
	// 			// if(false){
	// 			unlock(now_bucket->B_Lock); // 让渡给要分裂的段
	// 			goto TRY_INSERT;
	// 		}
	// 		// std::thread SplitThread(&ElimDA::split,this,key,loc); //创建deatch线程
	// 		// SplitThread.detach();
	// 		split(key, loc); // 判断目录这时候在分裂，则释放段锁  //若没有清空则需要检测是否有一个是满的，这里已经清空了
	// 		// now_bucket->sorted_num=0;
	// 		// now_bucket->dram_num=0;
	// 		// unlock(now_bucket->B_Lock);
	// 		// unlock(now_seg->S_Lock);
	// 		goto TRY_INSERT;
	// 	}
	// 	else
	// 	{
	// 		flush2sorted(key, loc, now_bucket->sorted_num); // 包括了unlock
	// 	}
	// }
	// now_bucket->bucket[now_bucket->dram_num].key = key;
	// now_bucket->bucket[now_bucket->dram_num].value = value;
	// now_bucket->u_address[now_bucket->dram_num].threadnum = threadnum; // 在插入的时候基本很少发生争用，不像cache_bucket中
	// now_bucket->u_address[now_bucket->dram_num].pagenum = pagenum;
	// now_bucket->u_address[now_bucket->dram_num].lognum = lognum * 16 + i;
	// now_bucket->dram_num++;
	// if (loc < now_seg->finish)
	// { // sync on write
	// 	auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - now_seg->S_depth - 1));
	// 	// SYNC_ON_WRITE;
	// }
	// unlock(now_bucket->B_Lock);
	return;
}

void ElimDA::flush2sorted(size_t hash, unsigned short bucket_num, unsigned short num, bool flag)
{
FLUSH_RETRY:
	auto seg_num = (hash >> (8 * sizeof(hash) - in_dram->DRAM_Index->D_depth));
	auto dram_bucket = &in_dram->DRAM_Index->seg[seg_num]->bucket[bucket_num];
	if (seg_num != (hash >> (8 * sizeof(hash) - in_dram->DRAM_Index->D_depth)))
	{
		std::this_thread::yield();
		goto FLUSH_RETRY;
	}
	auto Finger = &Fingerprint[seg_num][bucket_num][num];
	auto sorted_bucket = &(D_RW(_segment[seg_num])->sorted_bucket[bucket_num].s_bucket[num]);
	memcpy(sorted_bucket, dram_bucket->bucket, 16 * D_Bucket_Size);
	// pmemobj_memcpy(pop, sorted_bucket, dram_bucket->bucket, 256, PMEMOBJ_F_MEM_TEMPORAL); // PMEMOBJ_F_MEM_TEMPORAL
	for (int i = 0; i < D_Bucket_Size; i++)
	{
		Finger[i] = dram_bucket->bucket[i].key >> 24;
		if (flag)
		{
			if (dram_bucket->u_address[i].threadnum != INVALID_THREAD)
			{
				int r = check_and_add(dram_bucket->count, dram_bucket->u_address[i]);
				if (r >= 0)
				{
					uint _ = 0;
					do
					{
						_ = in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum]->num[0]; // lognum *16 所以要是分块需要 >> 4！！！！
					} while (!CAS(&in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum]->num[0], &_, _ + dram_bucket->count[i].count));
					if (_ + dram_bucket->count[i].count == U_Bucket_Size * 15)
					{
						// // 直接free不用push 以1MB为粒度
						free(in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum]);
						in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum] = NULL;
						POBJ_FREE(&(D_RW(D_RW(unsorted_area->unsorted_bucket)[dram_bucket->count[i].threadnum]))[dram_bucket->count[i].pagenum].oid);
					}
				}
				else if (r == -2)
				{
					// cout << "full" << endl;
					int j = rand() % Log_Count_Size;
					uint _ = 0;
					do
					{
						_ = in_dram->bitmap[dram_bucket->count[j].threadnum][dram_bucket->count[j].pagenum]->num[0];
					} while (!CAS(&in_dram->bitmap[dram_bucket->count[j].threadnum][dram_bucket->count[j].pagenum]->num[0], &_, _ + dram_bucket->count[j].count));
					if (_ + dram_bucket->count[j].count == U_Bucket_Size * 15)
					{
						cout << "jfree:\t" << (int)dram_bucket->count[j].threadnum << "\t" << (int)dram_bucket->count[j].pagenum << "\t" << _ + dram_bucket->count[j].count << endl;
						// 直接free不用push 以1MB为粒度
						free(in_dram->bitmap[dram_bucket->count[j].threadnum][dram_bucket->count[j].pagenum]);
						in_dram->bitmap[dram_bucket->count[j].threadnum][dram_bucket->count[j].pagenum] = NULL;
						POBJ_FREE(&(D_RW(D_RW(unsorted_area->unsorted_bucket)[dram_bucket->count[j].threadnum]))[dram_bucket->count[j].pagenum].oid);
					}
					dram_bucket->count[j].threadnum = dram_bucket->u_address[i].threadnum;
					dram_bucket->count[j].lognum = dram_bucket->u_address[i].lognum >> 4;
					dram_bucket->count[j].pagenum = dram_bucket->u_address[i].pagenum; // split?
				}
			}
		}
	}
	if (flag)
	{
		dram_bucket->flush_num++;
		if (dram_bucket->flush_num == MAX_FLUSH)
		{
			for (int i = 0; i < Log_Count_Size; i++)
			{

				if (dram_bucket->count[i].threadnum != INVALID_THREAD)
				{

					uint _ = 0;
					do
					{
						_ = in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum]->num[0]; // 原本有多个现在只有一个
					} while (!CAS(&in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum]->num[0], &_, _ + dram_bucket->count[i].count));
					if (_ + dram_bucket->count[i].count == U_Bucket_Size * 15) // 总共15*4096
					{
						// cout << "insert maxfree:\t" << (int)dram_bucket->count[i].threadnum << "\t" << (int)dram_bucket->count[i].pagenum << "\t" << _ + dram_bucket->count[i].count << endl;
						// 直接free不用push 以1MB为粒度
						free(in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum]);
						in_dram->bitmap[dram_bucket->count[i].threadnum][dram_bucket->count[i].pagenum] = NULL;
						POBJ_FREE(&(D_RW(D_RW(unsorted_area->unsorted_bucket)[dram_bucket->count[i].threadnum]))[dram_bucket->count[i].pagenum].oid);
					}
					dram_bucket->count[i].threadnum = INVALID_THREAD;
				}
			}
			dram_bucket->flush_num = 0;
		}
	}
	dram_bucket->sorted_num += D_Bucket_Size;
	dram_bucket->dram_num = 0;
	// unlock(dram_bucket->B_Lock);
}

void ElimDA::initElimDA(PMEMobjpool *pop, size_t initCap, int numThread)
{ // 16k个段，16M个条目
	crashed = true;
	this->pop = pop;
	POBJ_ALLOC(pop, &dir, struct Directory, sizeof(struct Directory), NULL, NULL);
	direct_dir = D_RW(dir);
	// POBJ_ALLOC(pop, &c_lock, struct cache_lock, sizeof(struct cache_lock), NULL, NULL);
	POBJ_ALLOC(pop, &cache_bucket, struct cache_merge_bucket, sizeof(struct cache_merge_bucket), NULL, NULL);
	POBJ_ALLOC(pop, &_unsorted_area, struct Unsorted_Area, sizeof(struct Unsorted_Area), NULL, NULL);
	direct_dir->initDirectory(static_cast<size_t>(log2(initCap)));
	// D_RW(c_lock)->initcache_lock(pop,static_cast<size_t>(log2(initCap)));
	unsorted_area = D_RW(_unsorted_area);
	unsorted_area->initUnsorted(pop, numThread);
	in_dram = new In_DRAM();
	in_dram->initemptylist(numThread);
	in_dram->initbitmap(numThread);
	in_dram->initDRAM_Index(initCap);
	// in_dram->initFilter(initCap);
	D_RW(cache_bucket)->init_bucket(pop, MERGEBUCKET_SIZE);
	_merge_bucket = D_RW(D_RW(cache_bucket)->merge_bucket);
	// _alter_bucket=D_RW(D_RW(cache_bucket)->alter_bucket);
	_bucket_point = D_RW(D_RW(cache_bucket)->bucket_point);
	STRUCTURETHREAD = 4;
	// _mutex=(D_RW(D_RW(cache_bucket)->pmutex));
	POBJ_ALLOC(pop, &direct_dir->segment, TOID(struct Sorted_Segment), sizeof(TOID(struct Sorted_Segment)) * initCap, NULL, NULL); // 分配n个段指针
	_segment = D_RW(direct_dir->segment);
	// POBJ_ALLOC(pop, _segment, struct Sorted_Segment, sizeof(struct Sorted_Segment) * initCap, NULL, NULL); // 统筹分配一个大的内存空间，待优化 可能PMDK自己已经优化好了？
	for (int i = 0; i < initCap; ++i)
	{
		// _segment[i] = (_segment + i)[0];
		POBJ_ALLOC(pop, &(_segment[i]), struct Sorted_Segment, sizeof(struct Sorted_Segment), NULL, NULL);
		// POBJ_ZALLOC(pop, &_segment[i], struct Sorted_Segment, sizeof(struct Sorted_Segment));
		D_RW(_segment[i])->initSortedSegment(static_cast<size_t>(log2(initCap)), pop);
	}
}

void ElimDA::initValuePool(PMEMobjpool *pool, int threadnum, int valuenum, int length)
{
	isvariable = 1;
	valuelength = length;
	ValuePool = (char **)malloc(threadnum * sizeof(char *));
	PoolLocate = (int *)calloc(sizeof(int), threadnum);
	for (int i = 0; i < threadnum; i++)
	{
		POBJ_ALLOC(pop, &_ValuePool[i], char, sizeof(char) * length * valuenum, NULL, NULL);
		ValuePool[i] = &D_RW(_ValuePool[i])[0];
		if (i == threadnum - 1)
			cout << "init:" << i << "\t" << (void *)ValuePool[i] << endl;
	}
}

void ElimDA::initFingerprint()
{
	Fingerprint = (Finger_t ***)malloc(direct_dir->capacity * sizeof(Finger_t **));
	for (int i = 0; i < direct_dir->capacity; i++)
	{
		Fingerprint[i] = (Finger_t **)malloc(kBucketNum * sizeof(Finger_t *)); // 一个项目对应一个特征值
		for (int j = 0; j < kBucketNum; j++)
		{
			Fingerprint[i][j] = (Finger_t *)malloc(S_Bucket_Size * sizeof(Finger_t)); // 一个项目对应一个特征值
		}
	}

	// Fingerprint=(Finger_t **)new Finger_t[direct_dir->capacity][Segment::kNumSlot];
	for (int i = 0; i < direct_dir->capacity; ++i)
	{
		auto target = _segment[i];
		for (int j = 0; j < kBucketNum; j++)
		{
			for (int k = 0; k < S_Bucket_Size; k++)
				if (D_RO(target)->sorted_bucket[j].s_bucket[k].key != INVALID)
					Fingerprint[i][j][k] = D_RO(target)->sorted_bucket[j].s_bucket[k].key >> 24; // 取出前2B
				else
					Fingerprint[i][j][k] = INVALID_Finger;
		}
	}
}

bool ElimDA::Get4Split(Key_t key, bool flag, int num, int x)
{ // num表示在桶排第几个,只搜索桶内的，只查一个桶
	auto seg = D_RW(_segment[x]);
	if (flag == true)
	{ // 说明其采用了CUKO
		num = -1;
	}
	auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
	auto f_idx = (f_hash & kMask); // 第几个桶  第二个哈希也放在同一个段内
	auto loc = f_idx % kBucketNum;
	Finger_t *Finger = Fingerprint[x][loc];
	unsigned int mask = 0;
	// cout << Finger+1 << "\t" << &Finger[1] << endl;    //数组取下一个直接加i就行 一下子可以比1个
	Finger_t key_mask = (key >> 24 & Finger_Mask);
	for (int i = num + 1; i < in_dram->DRAM_Index->seg[x]->bucket[loc].sorted_num; i += 16)
	{
		SSE_CMP16(Finger + i, key_mask);
		if (mask != 0)
		{
			int j = 0;
			while (j < 16 && i + j < in_dram->DRAM_Index->seg[x]->bucket[loc].sorted_num)
			{ // 按照缓存行比较
				if (CHECK_BIT(mask, j) && i + j < in_dram->DRAM_Index->seg[x]->bucket[loc].sorted_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j].key == key))
					return true;
				if (CHECK_BIT(mask, j + 1) && i + j + 1 < in_dram->DRAM_Index->seg[x]->bucket[loc].sorted_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 1].key == key))
					return true;
				if (CHECK_BIT(mask, j + 2) && i + j + 2 < in_dram->DRAM_Index->seg[x]->bucket[loc].sorted_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 2].key == key))
					return true;
				if (CHECK_BIT(mask, j + 3) && i + j + 3 < in_dram->DRAM_Index->seg[x]->bucket[loc].sorted_num && (D_RW(_segment[x])->sorted_bucket[loc].s_bucket[i + j + 3].key == key))
					return true;
				j += 4;
			}
		}
	}
	return false;
}

bool ElimDA::Get4New(Key_t key, D_I_Bucket *dram_bucket, Pair *_bucket, Finger_t *Finger_bucket)
{ // num表示在桶排第几个,只搜索桶内的，只查一个桶
	unsigned int mask = 0;
	// cout << Finger+1 << "\t" << &Finger[1] << endl;    //数组取下一个直接加i就行 一下子可以比1个
	Finger_t key_mask = (key >> 24 & Finger_Mask);
	for (int i = 0; i < dram_bucket->sorted_num; i += 16)
	{
		// std::cout << Finger_bucket + i << " " << key_mask << std::endl;
		SSE_CMP16(Finger_bucket + i, key_mask);
		if (mask != 0)
		{
			int j = 0;
			while (j < 16 && i + j < dram_bucket->sorted_num)
			{ // 按照缓存行比较
				if (CHECK_BIT(mask, j) && i + j < dram_bucket->sorted_num && (_bucket[i + j].key == key))
				{
					return true;
				}
				if (CHECK_BIT(mask, j + 1) && i + j + 1 < dram_bucket->sorted_num && (_bucket[i + j + 1].key == key))
				{
					return true;
				}
				if (CHECK_BIT(mask, j + 2) && i + j + 2 < dram_bucket->sorted_num && (_bucket[i + j + 2].key == key))
				{
					return true;
				}
				if (CHECK_BIT(mask, j + 3) && i + j + 3 < dram_bucket->sorted_num && (_bucket[i + j + 3].key == key))
				{
					return true;
				}
				j += 4;
			}
		}
	}
	return false;
}

void ElimDA::split(size_t hash, size_t loc)
{ // 当有序桶满后进行分裂,分为目录分裂与段分裂 目录分裂优先于段分裂，当目录分裂时所有试图段分裂的程序被阻塞
  // 若没更新则直接将其按照16个16个移动至无序区，加载至DRAM再删除特征数组
	
DIR_RETRY:
	// auto f_hash = hash_funcs[0](&_key, sizeof(Key_t), f_seed);
	auto x = (hash >> (8 * sizeof(hash) - in_dram->DRAM_Index->D_depth));
	auto now_seg = in_dram->DRAM_Index->seg[x]; // 万一这时候目录在分裂获取了错误的怎么办？ A:已经获取锁了不会进行目录分裂！不用传入这么多参数？  这个x可能要改改？
	if (now_seg->S_depth == direct_dir->depth)
	{ // 先进行一次目录分裂在进行一次段分裂
		cout << "dir split\t" << (int)direct_dir->depth << endl;
		if (!try_lock(in_dram->DRAM_Index->D_Lock))
		{
			unlock(now_seg->S_Lock);   // 让渡给要分裂的目录
			std::this_thread::yield(); // 到时候加入到lock里面看看?
			lock(now_seg->S_Lock);
			goto DIR_RETRY;
		}
		for (int i = 0; i < pow(2, now_seg->S_depth); i++)
		{
			if (i != x)
			{
				bool val;
				__atomic_store_n(&val, in_dram->DRAM_Index->seg[i]->S_Lock, __ATOMIC_RELEASE);
				do
				{
					while (val == true)
					{ // 等到不被占用
						__atomic_store_n(&val, in_dram->DRAM_Index->seg[i]->S_Lock, __ATOMIC_RELEASE);
					}
				} while (!CAS(&in_dram->DRAM_Index->seg[i]->S_Lock, &val, true));
				if (now_seg->S_depth != in_dram->DRAM_Index->seg[i]->S_depth)
				{
					i += pow(2, now_seg->S_depth - in_dram->DRAM_Index->seg[i]->S_depth) - 1;
				}
			}
		}
		auto dir_old = dir;
		auto dram_dir_old = in_dram->DRAM_Index;
		TOID_ARRAY(TOID(struct Sorted_Segment))
		d = direct_dir->segment;
		TOID(struct Directory)
		_dir;
		POBJ_ALLOC(pop, &_dir, struct Directory, sizeof(struct Directory), NULL, NULL);
		POBJ_ALLOC(pop, &D_RO(_dir)->segment, TOID(struct Sorted_Segment), sizeof(TOID(struct Sorted_Segment)) * direct_dir->capacity * 2, NULL, NULL);
		D_RW(_dir)->initDirectory(direct_dir->depth + 1);
		Finger_t ***_Fingerprint = (Finger_t ***)malloc(direct_dir->capacity * sizeof(Finger_t **) * 2); // 补DRAMindex
		D_Index *_DRAM_Index = new D_Index();
		_DRAM_Index->D_depth = in_dram->DRAM_Index->D_depth + 1;
		_DRAM_Index->seg = (D_I_Segment **)malloc(sizeof(D_I_Segment *) * pow(2, _DRAM_Index->D_depth)); // 分配n个段指针
		_DRAM_Index->D_Lock = true;																		 // 先置锁使得交换时不会
		for (int i = 0; i < direct_dir->capacity; ++i)
		{
			D_RW(D_RW(_dir)->segment)
			[2 * i] = D_RO(d)[i];
			D_RW(D_RW(_dir)->segment)
			[2 * i + 1] = D_RO(d)[i];
			_Fingerprint[2 * i] = Fingerprint[i];
			_Fingerprint[2 * i + 1] = Fingerprint[i];
			_DRAM_Index->seg[2 * i] = in_dram->DRAM_Index->seg[i];
			_DRAM_Index->seg[2 * i + 1] = in_dram->DRAM_Index->seg[i];
		}
		// POBJ_ALLOC(pop, &(D_RW(unsorted_area->unsorted_bucket)[unsorted_area->size]), U_Bucket, sizeof(U_Bucket) * UNSORTED_AMPLIFY * direct_dir->capacity * 2, NULL, NULL);
		// // bitmap初始化
		// in_dram->bitmap[unsorted_area->size] = (bitmap_t *)calloc(UNSORTED_AMPLIFY * direct_dir->capacity * 2, sizeof(bitmap_t));
		// unsorted_area->size++;
		// if (unsorted_area->size == 13)
		// {
		// 	cout << "split alloc!!\t" << D_RW(D_RW(unsorted_area->unsorted_bucket)[12]) << "\t" << sizeof(U_Bucket) * UNSORTED_AMPLIFY * direct_dir->capacity * 2 << endl;
		// }
		// if(direct_dir->capacity == 2048) {
		// 	cout <<  "925 " << Fingerprint[925][0] << endl;
		// 	cout << "2973 " << _Fingerprint[2973][0] << endl;
		// }
		Fingerprint = _Fingerprint; // 若Finger与dir在搜索到的时候发现深度不一样则重新搜索
		// if(direct_dir->capacity == 2048) {
		// 	cout <<  "925 " << Fingerprint[925][0][0] << endl;
		// 	cout << "2973 " << Fingerprint[2973][0][0] << endl;
		// }
		in_dram->DRAM_Index = _DRAM_Index;
		_segment = D_RW(D_RW(_dir)->segment);
		direct_dir = D_RW(_dir);
		dir = _dir; // 不需要persist?
		POBJ_FREE(&d.oid);
		POBJ_FREE(&dir_old.oid);
		free(dram_dir_old);
		x = 2 * x;
		unlock(in_dram->DRAM_Index->D_Lock);
		for (int i = 0; i < direct_dir->capacity; i += 2)
		{
			if (i != x)
				unlock(in_dram->DRAM_Index->seg[i]->S_Lock); // 解锁释放回段锁
		}
		goto DIR_RETRY;
	}
	else
	{ // 不用担心有目录
		// unlock(now_seg->S_Lock);
		// if(direct_dir->capacity==4096&&(x==2973||x==925||x==1949||x==413||x==157 || x==29 || x==1 || x == 5))
		// 	{
		// 		cout << "seg split\t" << x <<endl;
		// 		cout << "Fingerprint[2973][0][0]" << Fingerprint[2973][0][0] << endl;
		// 	}
		TOID(struct Sorted_Segment)
		seg1, seg2;
		// int test = _backup_num;			//不知道为什么分裂会有true的情况！！若split中改变不获取seg锁
		// do{
		// 	test = _backup_num;
		// }while(!CAS(&_backup_num, &test, test+2));
		// seg1=_backup[_backup_num];
		// seg2=_backup[_backup_num+1];
		// clock_gettime(CLOCK_MONOTONIC, &start);
		POBJ_ZALLOC(pop, &seg1, struct Sorted_Segment, sizeof(struct Sorted_Segment)); // 瓶颈！！一个size 131096
		POBJ_ZALLOC(pop, &seg2, struct Sorted_Segment, sizeof(struct Sorted_Segment)); // ZALLOC
		TOID(struct Sorted_Segment)													   // free
		old_seg = _segment[x];
		// clock_gettime(CLOCK_MONOTONIC, &end);
		// elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
		// if(elapsed>1000000)
		// cout << elapsed/1000 << "\tusec for alloc\t"  << endl;
		// clock_gettime(CLOCK_MONOTONIC, &start);
		struct Sorted_Segment *_seg1 = D_RW(seg1);
		struct Sorted_Segment *_seg2 = D_RW(seg2);
		_seg1->initSortedSegment(now_seg->S_depth + 1, pop);
		_seg2->initSortedSegment(now_seg->S_depth + 1, pop);
		D_RW(_segment[x])->copy_ptr1 = seg1;
		D_RW(_segment[x])->copy_ptr2 = seg2;
		// __atomic_store_n(&D_RW(_segment[x])->copy_ptr1,_seg1, __ATOMIC_RELEASE);
		// __atomic_store_n(&D_RW(_segment[x])->copy_ptr2,_seg2, __ATOMIC_RELEASE);
		D_I_Segment *dram_seg1 = (D_I_Segment *)calloc(1, sizeof(D_I_Segment));
		D_I_Segment *dram_seg2 = (D_I_Segment *)calloc(1, sizeof(D_I_Segment));
		for (int j = 0; j < kBucketNum; j++)
		{
			for (int k = 0; k < D_Bucket_Size; k++)
			{
				dram_seg1->bucket[j].count[k].threadnum = INVALID_THREAD;
				dram_seg2->bucket[j].count[k].threadnum = INVALID_THREAD;
				dram_seg1->bucket[j].u_address[k].threadnum = INVALID_THREAD;
				dram_seg2->bucket[j].u_address[k].threadnum = INVALID_THREAD;
			}
		}
		D_I_Segment *old_dram_seg = now_seg;
		now_seg->copy_ptr1 = dram_seg1;
		now_seg->copy_ptr2 = dram_seg2;
		// __atomic_store_n(&now_seg->copy_ptr1,dram_seg1, __ATOMIC_RELEASE);
		// __atomic_store_n(&now_seg->copy_ptr2,dram_seg2, __ATOMIC_RELEASE);
		dram_seg1->S_depth = now_seg->S_depth + 1;
		dram_seg2->S_depth = now_seg->S_depth + 1;
		// dram_seg1->S_Lock=true;   //提前上锁 更换指针后now_seg已经变了   //桶锁还没设置  get也要修改，若大于finish则搜旧的，若小于则搜新的 有没有必要加true
		// dram_seg2->S_Lock=true;
		dram_seg1->copy_Finger2 = NULL;
		dram_seg2->copy_Finger2 = NULL; // 用于验证是否分裂

		Finger_t **_Finger1 = (Finger_t **)malloc(kBucketNum * sizeof(Finger_t *));
		Finger_t **_Finger2 = (Finger_t **)malloc(kBucketNum * sizeof(Finger_t *));
		_Finger1[0] = (Finger_t *)malloc(S_Bucket_Size * sizeof(Finger_t) * kBucketNum * 2);
		for (int j = 0; j < kBucketNum; j++)
		{
			_Finger1[j] = _Finger1[0] + (2 * j * S_Bucket_Size);
			_Finger2[j] = _Finger1[0] + ((2 * j + 1) * S_Bucket_Size);
			// _Finger1[j]=(Finger_t *)malloc(S_Bucket_Size*sizeof(Finger_t));//一个项目对应一个特征值
			// _Finger2[j]=(Finger_t *)malloc(S_Bucket_Size*sizeof(Finger_t));//一个项目对应一个特征值
		}
		now_seg->copy_Finger1 = _Finger1;
		// __atomic_store_n(&now_seg->copy_Finger1,_Finger1, __ATOMIC_RELEASE);
		__atomic_store_n(&now_seg->copy_Finger2, _Finger2, __ATOMIC_RELEASE);
		// unlock(now_seg->S_Lock);
		auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - dram_seg1->S_depth)); // pattern有问题！
		for (int i = 0; i < kBucketNum; i++)
		{ // 第i个桶分裂

			unique_lock<mutex> lck(now_seg->bucket[i].B_Lock, defer_lock);
			if (i != loc)
				lck.lock();
			// if (i != loc)
			// 	now_seg->bucket[i].B_Lock.lock(); // todo
			for (int j = 0; j < now_seg->bucket[i].dram_num; j++)
			{
				Key_t key = now_seg->bucket[i].bucket[j].key;
				auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
				if (f_hash & pattern)
				{
					dram_seg2->bucket[i].bucket[dram_seg2->bucket[i].dram_num] = now_seg->bucket[i].bucket[j];
					dram_seg2->bucket[i].u_address[dram_seg2->bucket[i].dram_num] = now_seg->bucket[i].u_address[j]; // 赋值的时候连同锁一起赋值了！！！
					dram_seg2->bucket[i].dram_num++;																 // 若这里为16怎么办，还没处理
				}
				else
				{
					dram_seg1->bucket[i].bucket[dram_seg1->bucket[i].dram_num] = now_seg->bucket[i].bucket[j];
					dram_seg1->bucket[i].u_address[dram_seg1->bucket[i].dram_num] = now_seg->bucket[i].u_address[j];
					dram_seg1->bucket[i].dram_num++; // 无序区已经分裂有序区还没
				}
			}
			// reinsert  now_seg->bucket[i].sorted_num + 16 - 1
			for (int j = 0; j < now_seg->bucket[i].sorted_num; j++)
			{ // 第i个桶第j个数据  有序区！
				Key_t key = (D_RW(_segment[x])->sorted_bucket[i].s_bucket[j]).key;
				if (key != 0)
				{
					auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed); // 分裂后的数据默认放在第一个，因此数据产生永远都是f_hash的先于s_hash，然后同一个hash内从下往上分别为最新
					auto is_flag = ((f_hash & kMask)) % kBucketNum;
					if (f_hash & pattern)
					{
						// if (true)
						if (likely(!Get4New(key, &dram_seg2->bucket[i], _seg2->sorted_bucket[i].s_bucket, _Finger2[i])))
						{ // 若分裂一个桶释放一个桶，则给予变量存旧段用于搜索？ 瓶颈不在get在split
							_Finger2[i][dram_seg2->bucket[i].sorted_num] = Fingerprint[x][i][j];
							_seg2->sorted_bucket[i].s_bucket[dram_seg2->bucket[i].sorted_num] = D_RW(_segment[x])->sorted_bucket[i].s_bucket[j];
							dram_seg2->bucket[i].sorted_num++;
						}
					}
					else
					{
						// if (true)
						if (likely(!Get4New(key, &dram_seg1->bucket[i], _seg1->sorted_bucket[i].s_bucket, _Finger1[i])))
						{
							_Finger1[i][dram_seg1->bucket[i].sorted_num] = Fingerprint[x][i][j];
							_seg1->sorted_bucket[i].s_bucket[dram_seg1->bucket[i].sorted_num] = D_RW(_segment[x])->sorted_bucket[i].s_bucket[j];
							dram_seg1->bucket[i].sorted_num++;
						}
					}
				}
				// }
			}
			dram_seg1->bucket[i].sorted_num = (dram_seg1->bucket[i].sorted_num / D_Bucket_Size + 1) * D_Bucket_Size;
			dram_seg2->bucket[i].sorted_num = (dram_seg2->bucket[i].sorted_num / D_Bucket_Size + 1) * D_Bucket_Size;
			now_seg->finish++;
		}
		// unlock(now_seg->S_Lock);   //解锁获取锁
		if (direct_dir->depth == now_seg->S_depth + 1)
		{
			if (x % 2 == 0)
			{
				_segment[x + 1] = seg2;
				_segment[x] = seg1;
				Fingerprint[x + 1] = _Finger2;
				Fingerprint[x] = _Finger1;
				in_dram->DRAM_Index->seg[x + 1] = dram_seg2;
				in_dram->DRAM_Index->seg[x] = dram_seg1;
				unlock(in_dram->DRAM_Index->seg[x + 1]->S_Lock);
				unlock(in_dram->DRAM_Index->seg[x]->S_Lock);
			}
			else
			{
				_segment[x] = seg2;
				_segment[x - 1] = seg1;
				Fingerprint[x] = _Finger2;
				Fingerprint[x - 1] = _Finger1;
				in_dram->DRAM_Index->seg[x] = dram_seg2;
				in_dram->DRAM_Index->seg[x - 1] = dram_seg1;
				unlock(in_dram->DRAM_Index->seg[x - 1]->S_Lock);
				unlock(in_dram->DRAM_Index->seg[x]->S_Lock);
			}
		}
		else
		{
			int stride = pow(2, direct_dir->depth - now_seg->S_depth);
			auto start = x - (x % stride);
			for (int i = 0; i < stride / 2; ++i)
			{
				_segment[start + stride / 2 + i] = seg2;
				Fingerprint[start + stride / 2 + i] = _Finger2;
				in_dram->DRAM_Index->seg[start + stride / 2 + i] = dram_seg2;
			}
			unlock(in_dram->DRAM_Index->seg[start + stride / 2]->S_Lock);
			for (int i = 0; i < stride / 2; ++i)
			{
				_segment[start + i] = seg1;
				Fingerprint[start + i] = _Finger1;
				in_dram->DRAM_Index->seg[start + i] = dram_seg1;
			}
			unlock(in_dram->DRAM_Index->seg[start]->S_Lock);
		}
		POBJ_FREE(&old_seg.oid);
		free(old_dram_seg);
		// clock_gettime(CLOCK_MONOTONIC, &end);
		// elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
		// cout << elapsed/1000 << "\tusec for split\t"  << endl;
	} // seg_split
	return;
}

void ElimDA::init_backup(int num)
{
	_backup = (TOID(struct Sorted_Segment) *)malloc(num * sizeof(TOID(struct Sorted_Segment)));
	for (int i = 0; i < num; i++)
		POBJ_ALLOC(pop, &(_backup[i]), struct Sorted_Segment, sizeof(struct Sorted_Segment), NULL, NULL);
	_backup_num = 0;
}

void ElimDA::initdram()
{
	initFingerprint();
}

void ElimDA::final_exit()
{
	;
}

void ElimDA::struct_rebuild(int start, int end, Version_t ***Version_tree)
{
	for (int i = start; i < end; i++)
	{									// 有序区数据恢复+无序区与DRAM索引树结构构建
		auto s_seg = D_RW(_segment[i]); // 初始化段
		D_I_Segment *_seg = (D_I_Segment *)calloc(1, sizeof(D_I_Segment));
		Finger_t **_Finger = (Finger_t **)malloc(kBucketNum * sizeof(Finger_t *));
		Version_t **_Version = (Version_t **)malloc(kBucketNum * sizeof(Version_t *));

		for (int j = 0; j < kBucketNum; j++)
		{
			_Finger[j] = (Finger_t *)malloc(S_Bucket_Size * sizeof(Finger_t)); // 一个有序区项目对应一个特征值
		}
		for (int j = 0; j < kBucketNum; j++)
		{																		  // 第j个桶
			_Version[j] = (Version_t *)malloc(D_Bucket_Size * sizeof(Version_t)); // 一个DRAM_Index项目对应一个版本号 ，保证能够容纳所有无序区
		}
		_seg->S_depth = s_seg->local_depth;
		for (int j = 0; j < kBucketNum; j++)
		{ // 每个桶有序区重建
			Pair kvs[16];
			int k = 0;

			while (k < S_Bucket_Size)
			{
				memcpy(kvs, &(s_seg->sorted_bucket[j].s_bucket[k]), 256);
				int m;
				bool flag = false;
				for (m = 0; m < 16; m++)
				{
					// f_hash = hash_funcs[0](&kvs[m].key, sizeof(Key_t), f_seed);
					// f_idx = (f_hash& kMask);   //第几个桶
					// x = (f_hash >> (8*sizeof(f_hash) - in_dram->DRAM_Index->D_depth));   //不要采用
					// loc = f_idx % kBucketNum;
					if (kvs[m].key != 0)
					{ // 决定要不要改成INVALID  loc == j && x < i+pow(2,(in_dram->DRAM_Index->D_depth-_seg->S_depth))
						_Finger[j][k + m] = kvs[m].key >> 24;
						flag = true;
					}
					else
					{
						continue;
					}
				}
				k += m;
				// if (!flag) // 全是空的   有可能全是空后面也有数据，得看
				// 	break;
			}
			_seg->bucket[j].sorted_num = k; // 目前drarebm_num没初始化
			_seg->bucket[j].dram_num = 0;
		}
		for (int j = 0; j < pow(2, (in_dram->DRAM_Index->D_depth - _seg->S_depth)); j++)
		{ // 结构构建
			Fingerprint[i + j] = _Finger;
			in_dram->DRAM_Index->seg[i + j] = _seg;
			Version_tree[i + j] = _Version;
		}
		i = i + pow(2, (in_dram->DRAM_Index->D_depth - _seg->S_depth)) - 1; // 越过相同段
	}
}

void ElimDA::Rebuild() // 在Recovery之前
{					   // 重建bitmap，无序区dram索引，特征数组，缓存聚合桶
	in_dram = new In_DRAM();
	in_dram->DRAM_Index = new D_Index();
	direct_dir = D_RW(dir);
	in_dram->DRAM_Index->D_depth = direct_dir->depth;
	in_dram->DRAM_Index->seg = (D_I_Segment **)malloc(sizeof(D_I_Segment *) * direct_dir->capacity);
	_merge_bucket = D_RW(D_RW(cache_bucket)->merge_bucket); //_初始化
	_bucket_point = D_RW(D_RW(cache_bucket)->bucket_point);
	_segment = D_RW(direct_dir->segment);
	unsorted_area = D_RW(_unsorted_area);
	Fingerprint = (Finger_t ***)malloc(direct_dir->capacity * sizeof(Finger_t **));
	Version_t ***Version_tree = (Version_t ***)malloc(direct_dir->capacity * sizeof(Version_t **)); // 用作无序区恢复
	vector<thread> StructureThreads, DataThreads;
	clock_gettime(CLOCK_MONOTONIC, &start);
	auto chunk_size = direct_dir->capacity / STRUCTURETHREAD;
	for (int i = 0; i < STRUCTURETHREAD; i++)
	{
		StructureThreads.emplace_back(thread(&ElimDA::struct_rebuild, this, chunk_size * i, chunk_size * (i + 1), Version_tree));
	}
	for (auto &t : StructureThreads)
		t.join();
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
	cout << elapsed / 1000 << "\tusec for rebuild structure\tDATATHREAD:" << DATATHREAD << endl;
	// 初始化bitmap
	auto numThread = unsorted_area->numThread;
	in_dram->bitmap = (bitmap_t ***)malloc(sizeof(bitmap_t **) * numThread);
	for (int i = 0; i < numThread; i++)
	{
		in_dram->bitmap[i] = (bitmap_t **)malloc(MAX_LOG / numThread * sizeof(bitmap_t *));
		for (int k = 0; k < unsorted_area->used[i]; k++)
		{
			in_dram->bitmap[i][k] = (bitmap_t *)malloc(sizeof(bitmap_t));
			for (int j = 0; j < map_size; j++)
				in_dram->bitmap[i][k]->num[j] = 0;
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &start);
	if (DATATHREAD > numThread)
	{
		for (int i = 0; i < DATATHREAD; i++)
		{
			auto interval = DATATHREAD / numThread;
			DataThreads.emplace_back(thread(&ElimDA::unsorted_rebuild, this, i / interval, Version_tree, i % interval, interval));
		}
	}
	else
	{
		for (int i = 0; i < DATATHREAD; i++)
		{
			size_t chunk = numThread / DATATHREAD;
			if (i != DATATHREAD - 1)
				DataThreads.emplace_back(thread(&ElimDA::range_unsorted_rebuild, this, i * chunk, (i + 1) * chunk, Version_tree));
			else
				DataThreads.emplace_back(thread(&ElimDA::range_unsorted_rebuild, this, i * chunk, numThread, Version_tree));
		}
	}

	for (auto &t : DataThreads)
		t.join();
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
	cout << elapsed / 1000 << "\tusec for recover\t" << endl;
}

void ElimDA::range_unsorted_rebuild(int start_thread, int end_thread, Version_t ***Version_tree)
{
	for (int i = start_thread; i < end_thread; i++)
	{
		unsorted_rebuild(i, Version_tree, 0, 1);
	};
}

void ElimDA::unsorted_rebuild(int thread_num, Version_t ***Version_tree, int start, int interval)
{ // 无序区恢复 即back_up_log
	// auto capacity = pow(2, in_dram->DRAM_Index->D_depth) * UNSORTED_AMPLIFY;
	int j, k, m, n, min = 0;
	size_t i;
	uint64_t f_hash, f_idx, x; // 如果错了会不会是这里问题
	int8_t loc;
	D_I_Segment *now_seg;
	D_I_Bucket *now_bucket;
	for (i = start; i < unsorted_area->used[thread_num]; i += interval)
	{
		auto th_b = D_RW(D_RW(D_RW(unsorted_area->unsorted_bucket)[thread_num])[i]);
		if (th_b == NULL)
			continue;
		for (j = 0; j < U_Bucket_Size; j++)
		{
			Version_t _v = th_b->u_bucket[j].Version;
			for (k = 0; k < 15; k++)
			{
				f_hash = hash_funcs[0](&th_b->u_bucket[j].m_bucket[k].key, sizeof(Key_t), f_seed);
				f_idx = (f_hash & kMask);											 // 第几个桶
				x = (f_hash >> (8 * sizeof(f_hash) - in_dram->DRAM_Index->D_depth)); // 不要采用
				loc = f_idx % kBucketNum;
				now_seg = in_dram->DRAM_Index->seg[x];
				now_bucket = &(now_seg->bucket[loc]);
				unique_lock<mutex> lck(now_bucket->B_Lock, defer_lock);
				lck.lock();
				if (now_bucket->dram_num != D_Bucket_Size)
				{
					now_bucket->bucket[now_bucket->dram_num] = th_b->u_bucket[j].m_bucket[k];
					now_bucket->u_address[now_bucket->dram_num].threadnum = thread_num; // 第i层
					now_bucket->u_address[now_bucket->dram_num].pagenum = i;			// 第j个桶
					Version_tree[x][loc][now_bucket->dram_num] = _v;					// 60位版本号，4位恢复位
					now_bucket->u_address[now_bucket->dram_num].lognum = j * 16 + k;	// k*16   让cachebucket固定
					now_bucket->dram_num++;
				}
				else
				{
					min = -1;
					for (n = 0; n < D_Bucket_Size; n++)
					{
						if (Version_tree[x][loc][n] > _v)
						{ // 若相等，则后来的(m较大的)为新的直接跳过
							continue;
						}
						else /// 小于或等于
						{
							if (min == -1 || Version_tree[x][loc][n] < Version_tree[x][loc][min]) // 等于的时候上面较老，不用换
								min = n;
							else if (Version_tree[x][loc][n] == Version_tree[x][loc][min])
							{
								if (now_bucket->u_address[n].lognum < now_bucket->u_address[min].lognum)
								{
									min = n;
								}
							}
						}
					}
					if (min != -1)
					{
						size_t _;
						do
						{
							_ = in_dram->bitmap[now_bucket->u_address[min].threadnum][now_bucket->u_address[min].pagenum]->num[0];
						} while (!CAS(&in_dram->bitmap[now_bucket->u_address[min].threadnum][now_bucket->u_address[min].pagenum]->num[0], &_, _ + 1));
						now_bucket->bucket[min] = th_b->u_bucket[j].m_bucket[k];
						now_bucket->u_address[min].threadnum = thread_num; // 第i层
						now_bucket->u_address[min].pagenum = i;			   // 第j个桶
						Version_tree[x][loc][min] = _v;					   // 60位版本号，4位恢复位
						now_bucket->u_address[min].lognum = j * 16 + k;	   // k*16   让cachebucket固定
					}
				}
			}
		}
	}
}

void ElimDA::Recovery(PMEMobjpool *pop)
{ // 检查unsorted有没有多分配，是否分裂
	for (int i = 0; i < direct_dir->depth; i++)
	{
		if (D_RW(D_RW(_segment[i])->copy_ptr1) != NULL)
		{
			auto now_seg = in_dram->DRAM_Index->seg[i];
			D_I_Segment *dram_seg1 = (D_I_Segment *)calloc(1, sizeof(D_I_Segment));
			D_I_Segment *dram_seg2 = (D_I_Segment *)calloc(1, sizeof(D_I_Segment));
			now_seg->copy_ptr1 = dram_seg1;
			now_seg->copy_ptr2 = dram_seg2;
			dram_seg1->S_depth = now_seg->S_depth + 1;
			dram_seg2->S_depth = now_seg->S_depth + 1;
			for (int j = 0; j < kBucketNum; j++)
			{
				for (int k = 0; k < 16; k++)
				{
					dram_seg1->bucket[j].count[k].threadnum = INVALID_THREAD;
					dram_seg2->bucket[j].count[k].threadnum = INVALID_THREAD;
					dram_seg1->bucket[j].u_address[k].threadnum = INVALID_THREAD;
					dram_seg2->bucket[j].u_address[k].threadnum = INVALID_THREAD;
				}
			}
			Finger_t **_Finger1 = (Finger_t **)malloc(kBucketNum * sizeof(Finger_t *));
			Finger_t **_Finger2 = (Finger_t **)malloc(kBucketNum * sizeof(Finger_t *));
			_Finger1[0] = (Finger_t *)malloc(S_Bucket_Size * sizeof(Finger_t) * kBucketNum * 2);
			for (int j = 0; j < kBucketNum; j++)
			{
				_Finger1[j] = _Finger1[0] + (2 * j * S_Bucket_Size);
				_Finger2[j] = _Finger1[0] + ((2 * j + 1) * S_Bucket_Size);
			}
			now_seg->copy_Finger1 = _Finger1;
			now_seg->copy_Finger2 = _Finger2;
			auto seg1 = D_RW(_segment[i])->copy_ptr1;
			auto seg2 = D_RW(_segment[i])->copy_ptr2;
			auto _seg1 = D_RW(seg1);
			auto _seg2 = D_RW(seg2);
			int j;
			auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - dram_seg1->S_depth));
			for (j = 0; j < now_seg->bucket[i].dram_num; j++)
			{
				Key_t key = now_seg->bucket[i].bucket[j].key;
				auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
				if (f_hash & pattern)
				{
					dram_seg2->bucket[i].bucket[dram_seg2->bucket[i].dram_num] = now_seg->bucket[i].bucket[j];
					dram_seg2->bucket[i].u_address[dram_seg2->bucket[i].dram_num] = now_seg->bucket[i].u_address[j]; // 赋值的时候连同锁一起赋值了！！！
					dram_seg2->bucket[i].dram_num++;
				}
				else
				{
					dram_seg1->bucket[i].bucket[dram_seg1->bucket[i].dram_num] = now_seg->bucket[i].bucket[j];
					dram_seg1->bucket[i].u_address[dram_seg1->bucket[i].dram_num] = now_seg->bucket[i].u_address[j];
					dram_seg1->bucket[i].dram_num++;
				}
			}
			j = 0;
			while (1)
			{
				int m1 = 0, m2 = 0;
				while (_seg1->sorted_bucket[j].s_bucket[m1].key != 0)
				{
					_Finger1[j][m1] = _seg1->sorted_bucket[j].s_bucket[m1].key >> 24;
					m1++;
				}
				dram_seg1->bucket[j].sorted_num = m1;
				while (_seg2->sorted_bucket[j].s_bucket[m2].key != 0)
				{
					_Finger2[j][m2] = _seg2->sorted_bucket[j].s_bucket[m2].key >> 24;
					m2++;
				}
				dram_seg2->bucket[j].sorted_num = m2;
				if (m1 + m2 == 0)
				{
					break;
				}
				else
				{
					j++;
				}
			}
			for (; j < kBucketNum; j++)
			{
				for (int k = 0; k < now_seg->bucket[j].sorted_num; k++)
				{ // 第i个桶第j个数据  有序区！

					Key_t key = (D_RW(_segment[i])->sorted_bucket[j].s_bucket[k]).key;
					auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed); // 分裂后的数据默认放在第一个，因此数据产生永远都是f_hash的先于s_hash，然后同一个hash内从下往上分别为最新
					auto is_flag = ((f_hash & kMask)) % kBucketNum;
					if (f_hash & pattern)
					{
						// if (true)
						if (likely(!Get4New(key, &dram_seg2->bucket[j], _seg2->sorted_bucket[j].s_bucket, _Finger2[j])))
						{ // 若分裂一个桶释放一个桶，则给予变量存旧段用于搜索？ 瓶颈不在get在split
							_Finger2[j][dram_seg2->bucket[j].sorted_num] = Fingerprint[i][j][k];
							_seg2->sorted_bucket[j].s_bucket[dram_seg2->bucket[j].sorted_num] = D_RW(_segment[i])->sorted_bucket[j].s_bucket[k];
							dram_seg2->bucket[j].sorted_num++;
						}
					}
					else
					{
						// if (true)
						if (likely(!Get4New(key, &dram_seg1->bucket[j], _seg1->sorted_bucket[j].s_bucket, _Finger1[j])))
						{
							_Finger1[j][dram_seg1->bucket[j].sorted_num] = Fingerprint[i][j][k];
							_seg1->sorted_bucket[j].s_bucket[dram_seg1->bucket[j].sorted_num] = D_RW(_segment[i])->sorted_bucket[j].s_bucket[k];
							dram_seg1->bucket[j].sorted_num++;
						}
					}
					// }
				}
				dram_seg1->bucket[j].sorted_num = (dram_seg1->bucket[j].sorted_num / 16 + 1) * 16;
				dram_seg2->bucket[j].sorted_num = (dram_seg2->bucket[j].sorted_num / 16 + 1) * 16;
				now_seg->finish++;
			}
			if (direct_dir->depth == now_seg->S_depth + 1)
			{
				if (i % 2 == 0)
				{
					_segment[i + 1] = seg2;
					_segment[i] = seg1;
					Fingerprint[i + 1] = _Finger2;
					Fingerprint[i] = _Finger1;
					in_dram->DRAM_Index->seg[i + 1] = dram_seg2;
					in_dram->DRAM_Index->seg[i] = dram_seg1;
					unlock(in_dram->DRAM_Index->seg[i + 1]->S_Lock);
					unlock(in_dram->DRAM_Index->seg[i]->S_Lock);
				}
				else
				{
					_segment[i] = seg2;
					_segment[i - 1] = seg1;
					Fingerprint[i] = _Finger2;
					Fingerprint[i - 1] = _Finger1;
					in_dram->DRAM_Index->seg[i] = dram_seg2;
					in_dram->DRAM_Index->seg[i - 1] = dram_seg1;
					unlock(in_dram->DRAM_Index->seg[i - 1]->S_Lock);
					unlock(in_dram->DRAM_Index->seg[i]->S_Lock);
				}
			}
			else
			{
				int stride = pow(2, direct_dir->depth - now_seg->S_depth);
				auto start = i - (i % stride);
				for (int j = 0; j < stride / 2; ++j)
				{
					_segment[start + stride / 2 + j] = seg2;
					Fingerprint[start + stride / 2 + j] = _Finger2;
					in_dram->DRAM_Index->seg[start + stride / 2 + j] = dram_seg2;
				}
				unlock(in_dram->DRAM_Index->seg[start + stride / 2]->S_Lock);
				for (int j = 0; j < stride / 2; ++j)
				{
					_segment[start + j] = seg1;
					Fingerprint[start + j] = _Finger1;
					in_dram->DRAM_Index->seg[start + j] = dram_seg1;
				}
				unlock(in_dram->DRAM_Index->seg[start]->S_Lock);
			}
		}
	}
}

double space_consumption(PMEMobjpool *pop)
{
	double space = 0;
	PMEMoid iter;
	POBJ_FOREACH(pop, iter)
	{
		space += pmemobj_alloc_usable_size(iter)*1.0 / ((double)1024.0 * 1024);
		// if(space>480000)
		//  std::cout << "test" << std::endl;
	}
	return space;
}
// POBJ_LAYOUT_ROOT(HashTable, ElimDA);
// POBJ_LAYOUT_TOID(HashTable, struct Sorted_Segment);
// POBJ_LAYOUT_TOID(HashTable, TOID(struct Sorted_Segment));
// POBJ_LAYOUT_TOID(HashTable, struct Directory);
// // POBJ_LAYOUT_TOID(HashTable, struct cache_lock);
// POBJ_LAYOUT_TOID(HashTable, struct Pair);
// POBJ_LAYOUT_TOID(HashTable, struct cache_merge_bucket);
// POBJ_LAYOUT_TOID(HashTable, TOID(M_Bucket));
// POBJ_LAYOUT_TOID(HashTable, U_Bucket);
// POBJ_LAYOUT_TOID(HashTable, PMEMmutex)
// POBJ_LAYOUT_TOID(HashTable, mutex)
// POBJ_LAYOUT_TOID(HashTable, TOID(U_Bucket));
// POBJ_LAYOUT_TOID(HashTable, struct Unsorted_Area);
// POBJ_LAYOUT_TOID(HashTable, M_Bucket);
// POBJ_LAYOUT_TOID(HashTable, write_locate);
// POBJ_LAYOUT_TOID(HashTable, int64_t);