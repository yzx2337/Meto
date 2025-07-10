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
// yzx--bind cpu
#include <sched.h>
#include <pthread.h>
#define VMRSS_LINE 22

using namespace std;

int main(int argc, char *argv[])
{
    size_t pid = atoi(argv[1]);
    char file_name[64] = {0};
    FILE *fd;
    char line_buff[512] = {0};
    sprintf(file_name, "/proc/%d/status", pid);

    fd = fopen(file_name, "r");
    if (nullptr == fd)
        return 0;

    char name[64];
    int vmrss = 0;
    for (int i = 0; i < VMRSS_LINE - 1; i++)
    {
        fgets(line_buff, sizeof(line_buff), fd);
        // cout << line_buff << endl;
    }

    fgets(line_buff, sizeof(line_buff), fd);
    // cout << "final\t" << line_buff << endl;
    sscanf(line_buff, "%s %d", name, &vmrss);
    fclose(fd);
    printf("DRAM occupied %f MB\n", vmrss / 1024.0);
    // cnvert VmRSS from KB to MB
    return vmrss / 1024.0;
    return 0;
}
