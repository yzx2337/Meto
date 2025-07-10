#!/bin/bash
TOP=$(dirname $0)
cd $TOP
perf_dir=./insert_perf/
FLAG='-mavx -mavx2 -mavx512f -mavx512bw -mavx512vl -mclwb'   
sed -i '17s/.*/\/\/ #define ADR/' ./src/ElimDA.cpp
g++ -O3 -std=c++17 -I./ -lrt -c -o  src/ElimDA.o src/ElimDA.cpp -DINPLACE -lpmemobj -lpmem $FLAG -lprofiler -lunwind 
g++ -O3 -std=c++17 -I./ -lrt  -o bin/run_all_ycsb src/run_all_ycsb.cpp src/ElimDA.o include/dlock.o  -lpmemobj -lpmem -lpthread -DMULTITHREAD -lpqos 

# 删除自己对应的数据块 dir = $1, 为了安全，只需要传入后续的后缀
# delete_mnt() {
#     rm -rf "/mnt/pmem1-node0/$1"
# }

dir=/mnt/pmem1-node0
loadsize=200000000
runsize=200000000
distribution_type=("zipfian" "uniform")
ycsb_config_type=("u5r95.txt" "u50r50.txt" "w5r95.txt" "w50r50.txt" "w20r80.txt" "w80r20.txt")

if [ $# -eq 0 ]; then
    echo "Error: No thread_type parameters provided."
    echo "Usage: $0 thread_type1 [thread_type2 ...]"
    exit 1
fi

thread_type=("$@")
total_test_num=1
data_prefix=/home/scae/sc

# 指定测试负载文件
for now_dist_type in  "${distribution_type[@]}"
do
    for now_ycsb_type in "${ycsb_config_type[@]}"
    do
        # 指定测试线程数量
        for thread_num in "${thread_type[@]}"
        do
            # 指定测试次数
            for now_test_num in $(seq 1 $total_test_num);
            do
            rm -rf "/mnt/pmem1-node0/ElimDA"
            out_dir="${data_prefix}/data/ex2/Meto_${now_dist_type}_${now_ycsb_type}_${thread_num}"
            echo ">>>>>now run in: $now_test_num $now_dist_type $now_ycsb_type $thread_num"
            # 输出当前运行时间
            current_time_temp=$(date)
        
            echo " 
            >>>>> run time: $current_time_temp
            >>>>> now run in: $now_test_num $now_dist_type $now_ycsb_type $thread_num" >> "$out_dir"
            
            # 指定负载load和run的详细位置
            ycsb_dir="/mnt/ycsb/$now_dist_type-200/$now_ycsb_type"
            load_dir="/mnt/ycsb/$now_dist_type-200/load200m.txt"
            echo "./bin/run_all_ycsb $dir $loadsize $runsize $thread_num $ycsb_dir $load_dir"
            numactl -N 0 -m 0 \
            ./bin/run_all_ycsb "$dir" "$loadsize" "$runsize" "$thread_num" "$ycsb_dir" "$load_dir"  |grep -a -E "num|Ops|Start" >> "$out_dir"   
            done
        done    
    done
done
