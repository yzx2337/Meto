#!/bin/bash
#no clear cache
dir=/mnt/pmem1-node0
perf_dir=./insert_perf/
TOP=$(dirname $0)
cd $TOP

if [ $# -eq 0 ]; then
    echo "Error: No thread_type parameters provided."
    echo "Usage: $0 thread_type1 [thread_type2 ...]"
    exit 1
fi

thread_type=("$@")


sed -i '17s/.*/\/\/ #define ADR/' ./src/ElimDA.cpp
distribution_type=("uniform" "zipfian")
data_prefix=/home/scae/sc

# 指定测试负载文件
for now_dist_type in "${distribution_type[@]}"
do
    if [ "$now_dist_type" = "zipfian" ]; then
        sed -i '128s/.*/string dataset = "\/mnt\/ycsb\/zipfian-200\/load200m.txt";/' ./src/test.cpp
        sed -i '129s/.*/string readDs = "\/mnt\/ycsb\/zipfian-200\/read200m.txt";/' ./src/test.cpp
        run_file="/mnt\/ycsb\/zipfian-200\/read200m.txt ./src/test.cpp"
    else
        sed -i '128s/.*/string dataset = "\/mnt\/ycsb\/uniform-200\/load200m.txt";/' ./src/test.cpp
        sed -i '129s/.*/string readDs = "\/mnt\/ycsb\/uniform-200\/read200m.txt";/' ./src/test.cpp
        run_file="/mnt\/ycsb\/uniform-200\/read200m.txt ./src/test.cpp"
    fi

    size=200000000
    oflag=-O3
    FLAG='-mavx -mavx2 -mavx512f -mavx512bw -mavx512vl -mclwb'  
    PCM='-I ./pcm -L ./pcm'

    g++ $oflag -std=c++17 -I./ -lrt -c -o src/ElimDA.o src/ElimDA.cpp -DINPLACE -lpmemobj -lpmem $FLAG -lprofiler -lunwind -gdwarf
    g++ $oflag -std=c++17 -I./ -lrt -o bin/metoex1 src/test.cpp src/ElimDA.o include/dlock.o $PCM -lpmemobj -lpmem -lpthread -DMULTITHREAD -lpqos

    total_test_num=1
    for thread_num in "${thread_type[@]}"
    do
        for now_test_num in $(seq 1 $total_test_num)
        do
            rm -rf $dir/ElimDA
            current_time_temp=$(date)

            out_dir=$data_prefix/data/ex1/Meto_"$now_dist_type"_"$thread_num"
            echo "
            >>>>> run time: $current_time_temp
            >>>>> now run in: $now_dist_type $now_test_num $thread_num"
            current_directory=$(pwd)
            echo "
            >>>> now in $current_directory
            >>>>> run time: $current_time_temp
            >>>>> run_file: "$run_file"
            >>>>> sh: "./bin/multi_threaded_ElimDA $dir $size "$thread_num""
            >>>>> now run in: $now_dist_type $now_test_num $thread_num" >> "$out_dir"

            numactl -N 0 -m 0 \
            ./bin/metoex1 $dir $size "$thread_num" | grep -a -E "num|Ops|Start"  >> "$out_dir"
        done
    done
done