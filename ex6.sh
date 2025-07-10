#!/bin/bash
#no clear cache
dir=/mnt/pmem1-node0/
perf_dir=./insert_perf/
TOP=$(dirname $0)
cd $TOP
distribution_type=( "uniform")
thread_num=1
# 指定测试负载文件

# outdir=./data/latency
size=200000000
oflag=-O3
FLAG='-mavx -mavx2 -mavx512f -mavx512bw -mavx512vl -mclwb'  
PCM='-I ./pcm -L ./pcm'
sed -i '17s/.*/\/\/ #define ADR/' ./src/ElimDA.cpp
g++ $oflag -std=c++17 -I./ -lrt -c -o src/ElimDA.o src/ElimDA.cpp -DINPLACE -lpmemobj -lpmem $FLAG -lprofiler -lunwind  -gdwarf  #-O0 #-pg #-O0
g++ $oflag -std=c++17 -I./ -lrt  -o bin/latency src/latency.cpp src/ElimDA.o include/dlock.o  $PCM -lpmemobj -lpmem -lpthread -DMULTITHREAD -lpqos  #-gdwarf #-O0 #-pg #-O0

data_prefix=/home/scae/sc

total_test_num=1;
for now_test_num in $(seq 1 $total_test_num);
do
    rm -rf $dir/ElimDA
    current_time_temp=$(date)

    out_dir="${data_prefix}/data/ex6/Meto"
    echo " 
    >>>>> run time: $current_time_temp
    >>>>> now run in: $now_dist_type $now_test_num "
    current_directory=$(pwd)

    echo " 
    >>>> now in $current_directory
    >>>>> run time: $current_time_temp
    >>>>> run_file: "$run_file"
    >>>>> sh: "./bin/multi_threaded_ElimDA $dir $size"
    >>>>> now run in: $now_dist_type $now_test_num " >> "$out_dir"

    numactl -N 0 -m 0 \
    ./bin/latency $dir $size "$thread_num" >> "$out_dir"
done
