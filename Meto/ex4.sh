#!/bin/bash
#no clear cache
dir=ElimDA
TOP=$(dirname $0)
cd $TOP
num=24
size=200000000
perf_dir=./insert_perf/
FLAG='-mavx -mavx2 -mavx512f -mavx512bw -mavx512vl -mclwb'  
thread_type=(24)
data_prefix=/home/scae/sc

sed -i '17s/.*/#define ADR/' ./src/ElimDA.cpp

for thread_num in "${thread_type[@]}"
do
    for i in 1
    do
        outdir="${data_prefix}/data/ex4/Meto_$thread_num"
        current_time_temp=$(date)
        echo "now: $current_time_temp" >> $outdir
        echo "round: $i" >> $outdir
        
        echo "now: $current_time_temp"
        echo "round: $i"
        rm -f /mnt/pmem1-node0/$dir

        g++ -O3 -std=c++17 -I./ -lrt -c -o src/ElimDA.o src/ElimDA.cpp -DINPLACE -lpmemobj -lpmem $FLAG -lprofiler -lunwind #-O0 #-pg #-O0
        g++ -O3 -std=c++17 -I./ -lrt  -o bin/variable src/variable2.cpp src/ElimDA.o include/dlock.o -lpmemobj -lpmem -lpthread -DMULTITHREAD -lpqos #-O0 #-pg #-O0
        numactl -N 0 -m 0 ./bin/variable /mnt/pmem1-node0/$dir $size $thread_num | grep "usec" >> $outdir
    done
done

sed -i '17s/.*/\/\/ #define ADR/' ./src/ElimDA.cpp
#r /mnt/pmem1-node0/pool1 50000000 8