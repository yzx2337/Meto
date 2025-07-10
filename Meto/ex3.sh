#!/bin/bash
#no clear cache
dir=/mnt/pmem1-node0/ElimDA

num=24
size=200000000
perf_dir=./insert_perf/
FLAG='-mavx -mavx2 -mavx512f -mavx512bw -mavx512vl -mclwb'  
sed -i '17s/.*/\/\/ #define ADR/' ./src/ElimDA.cpp
data_prefix=/home/scae/sc
for i in 1
    do
    for valuesize in 8
    do
        outdir="${data_prefix}/data/ex3/Meto_test.csv"
        current_time_temp=$(date)
        echo "now: $current_time_temp" >> $outdir
        echo "round: $i" >> $outdir
        echo "size: "$valuesize >> $outdir
        
        echo "now: $current_time_temp"
        echo "round: $i"
        echo "size: "$valuesize"" 
        rm -f $dir
        sleep 5
        g++ -O3 -std=c++17 -I./ -lrt -c -o src/ElimDA.o src/ElimDA.cpp -DINPLACE -lpmemobj -lpmem $FLAG -lprofiler -lunwind #-O0 #-pg #-O0
        g++ -O3 -std=c++17 -I./ -lrt  -o bin/variable src/variable2.cpp src/ElimDA.o include/dlock.o  -lpmemobj -lpmem -lpthread -DMULTITHREAD -lpqos #-O0 #-pg #-O0
        numactl -N 0 -m 0 ./bin/variable $dir $size $num | grep "usec" # >> $outdir
    done
done

#r /mnt/pmem1-node0/pool1 50000000 8