# MetoHash: A Memory-Efficient and Traffic-Optimized Hashing Index on Hybrid PMem-DRAM Memories

This is the implementation repository of our SC'25 paper: MetoHash: A Memory-Efficient and Traffic-Optimized Hashing Index on Hybrid PMem-DRAM Memories.
This artifact provides the source code of MetoHash and scripts to reproduce all the experiment results in our paper.
MetoHash, a <u>**M**</u>emory-<u>**E**</u>fficient and <u>**T**</u>raffic-<u>**O**</u>ptimized hashing index on hybrid PMem-DRAM memories.
MetoHash proposes a three-layer index structure spanning across CPU
caches, DRAM, and PMem for data management.

- [MetoHash](#title-id)
  * [Supported Platform](#supported-platform)
  * [Source Code *(Artifacts Available)*](#source-code-artifacts-available)
  * [Environment Setup](#environment-setup)
  * [YCSB Workloads](#ycsb-workloads)
  * [Getting Started *(Artifacts Functional)*](#getting-started-artifacts-functional)
  * [Reproduce All Experiment Results *(Results Reproduced)*](#reproduce-all-experiment-results-results-reproduced)
  * [Paper](#paper)
  * [Acknowledgments](#acknowledgments)



## Supported Platform

To run MetoHash, your system must be equipped with Intel® Optane™ PMem 200 series persistent memory.

For optimal performance and data integrity, the host system's CPU and platform must support eADR (enhanced Asynchronous DRAM Refresh).

If your persistent memory or CPU configuration does not support eADR, you may still run MetoHash by using the MetoHash-ADR version instead.



## Source Code *(Artifacts Available)*

Using the following command to clone this github repo:

```bash
git clone https://github.com/yzx2337/Meto
```



## Environment Setup

To all AE Reviewers: The environment is prepared and ready for you.

We will be providing the setup guide for the community later on.



## YCSB Workloads

Also, we have generated the necessary YCSB workloads for the experiments.

You can find them under the `/mnt/ycsb` directory.



## Getting Started *(Artifacts Functional)*

Running the script this way is suitable for observing the performance of MetoHash in isolation, but it is not intended for reproducing the full results. 

For a more automated script, please see the section **'Reproduce All Experiment Results (Results Reproduced)'**.

On the machine we have prepared for you, please follow these steps:

1. **Log in** to the `scae` account.

2. **Navigate** to the `/home/scae/sc/ElimDA` directory. (Note: `ElimDA` is the alias for MetoHash used in the codebase).

3. **Execute** the following command:

   Bash

   ```
   ./ex1.sh 24
   ```

   *(The parameter `24` specifies that 24 threads should be used for the execution.)*

The results will be written to `/home/scae/sc/data/ex1/Meto_uniform_24` and `/home/scae/sc/data/ex1/Meto_zipfian_24`.

The `ex2.sh` script is used for running various workloads that have a mix of read and write operations. Similar to the first experiment, you must also pass a single argument to define the number of threads.



## Reproduce All Experiment Results *(Results Reproduced)*

While running scripts directly works well for testing MetoHash in isolation, it is ill-suited for reproducing the results across all eight of our compared indexes. 

**To address this, we offer reviewers a one-click solution below that automates the entire process: it runs all benchmarks, collects the resulting data, and generates the reproduction figures.**



To all AE Reviewers: Before reproduce experiment results, please make sure that your working directory is under `/home/scae/sc` and conduct following steps as root using `sudo -s`. Meanwhile, due to the **limited execution time(8 hours)** of AE, we 1)only perform the result of uniform distribution in Exp#1 and Exp#2; 2)evaluate all indexes exclude Level and CLevel, because both of them take too much time to get the evaluation result; 3) only evaluate the result where thread number is increased from 8 to 24 in Exp#1, Exp#2 and Exp#4. We also provide a optional python script(`scripts/clean_data_cache.py`) to remove all the previous resulting data and experiment figures before you start to reproduce all the results.

### Exp#1 - Figure 10 (~1 hour)

1. Do `cd scripts/`
2. Do `sudo python3 ex1_fig10.py`
3. After the experiment is finished, this will generate `ex1_fig10.png` in `output/` folder, and the resulting data is in `../data/ex1` folder.

### Exp#2 - Figure 11 (~4 hours)

1. Do `cd scripts/`
2. Do `sudo python3 ex2_fig11.py`
3. After the experiment is finished, this will generate `ex2_fig11.png` in `output/` folder, and the resulting data is in `../data/ex2/` folder.

### Exp#4 - Figure 13 (~5 min)

1. Do `cd scripts/`
2. Do `sudo python3 ex4_fig13.py`
3. After the experiment is finished, this will generate `ex4_fig13.png` in `output/` folder, and the resulting data is in `../data/ex4/` folder.

### Exp#5 - Figure 14 (~30 min)

1. Do `cd scripts/`
2. Do `sudo python3 ex5_fig14.py`
3. After the experiment is finished, this will generate `ex5_fig14.png` in `output/` folder, and the resulting data is in `../data/ex5/` folder.

### Exp#6 - Figure 15 (~2 hours)

1. Do `cd scripts/`
2. Do `sudo python3 ex6_fig15.py`
3. After the experiment is finished, this will generate `ex6_fig15.png` in `output/` folder, and the resulting data is in `../data/ex6/` folder.



## Paper

If you use MetoHash in your research, please cite our paper:

```bibtex
@inproceedings{meto2025,
  author    = {Zixiang Yu, Guangyang Deng, Zhirong Shen, Qiangsheng Su, Ronglong Wu, Xinbin Hu, Xiaoli Wang, Quanqing Xu, Chuanhui Yang, Zhifeng Bao, Yiming Zhang, Jiwu Shu},
  title     = {{Meto}: A Memory-Efficient and Traffic-Optimized Hashing Index on Hybrid PMem-DRAM Memories},
  booktitle = {Proceedings of the International Conference for High Performance Computing,
                  Networking, Storage, and Analysis, {SC} 2025},
  year      = {2025},
  address   = {St. louis, MO, USA},
  pages     = {XXX},
  url       = {XXX},
  publisher = {{XXX}},
  month     = nov,
}
```