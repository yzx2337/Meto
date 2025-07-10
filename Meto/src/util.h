#ifndef UTIL_H_
#define UTIL_H_

#include <cstdlib>
#include <cstdint>
#include <immintrin.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <stdint.h>

#define CPU_FREQ_MHZ (1994) // cat /proc/cpuinfo
#define CAS(_p, _u, _v) (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
#define kCacheLineSize (64)

static inline void CPUPause(void){
    __asm__ volatile("pause":::"memory");
}

inline void mfence(void){
    asm volatile("mfence":::"memory");
}

#define ADD(_p, _v) (__atomic_add_fetch(_p, _v, __ATOMIC_SEQ_CST))
#define SUB(_p, _v) (__atomic_sub_fetch(_p, _v, __ATOMIC_SEQ_CST))
#define LOAD(_p) (__atomic_load_n(_p, __ATOMIC_SEQ_CST))
#define STORE(_p, _v) (__atomic_store_n(_p, _v, __ATOMIC_SEQ_CST))

#define SIMD 1
#define SIMD_CMP8(src, key)                                         \
  do {                                                              \
    const __m256i key_data = _mm256_set1_epi8(key);                 \
    __m256i seg_data =                                              \
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src)); \
    __m256i rv_mask = _mm256_cmpeq_epi8(seg_data, key_data);        \
    mask = _mm256_movemask_epi8(rv_mask);                           \
  } while (0)

/*
 * 比较指纹的函数
 * __m128i 128bits的整数
 * _mm_set1_epi8(char a)将128划分成16个8位的部分 将a赋值给__m128i
 * _mm_loadu_si128(__m128i const* mem_addr) 将mem_addr起始的128位的值取出来
 * _mm_cmpeq_epi8(seg_data, key_data)比较函数 将seg_data和key_data 8位8位的进行比较，相等返回1，不相等返回0
 * _mm_movemask_epi8(__m128i a) 取a的MSB位（每8位取一次），返回的是个int类型的函数 int的高位补0，低16位根据a的MSB来得到
 */
#define SSE_CMP8(src, key)                                       \
  do {                                                           \
    const __m128i key_data = _mm_set1_epi8(key);                 \
    __m128i seg_data =                                           \
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)); \
    __m128i rv_mask = _mm_cmpeq_epi8(seg_data, key_data);        \
    mask = _mm_movemask_epi8(rv_mask);                           \
  } while (0)

//判断var在pos处的数据是否为1 是的话 返回1 否的话返回0
#define CHECK_BIT(var, pos) ((((var) & (1 << pos)) > 0) ? (1) : (0))
#define SSE_CMP16(src, key)                                       \
  do {                                                           \
    const __m256i key_data = _mm256_set1_epi16(key);                 \
    __m256i seg_data =                                           \
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src)); \
    __mmask16 rv_mask = _mm256_cmpeq_epi16_mask(seg_data, key_data);        \
    mask = _cvtmask16_u32(rv_mask);                           \
  } while (0)


#endif
