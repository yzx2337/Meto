#include <iostream>
#include <cstdlib>
#include <cstring>

int main() {
    const size_t memoryBlockSize = 100ll * 1024 * 1024 * 1024; // 1GB in bytes
    int * memoryBlock[101];

    for (int i = 0; i < 1; ++i) {
        std::cout << "Attempting to allocate " << memoryBlockSize << " bytes..." << std::endl;

        // Allocate memory
        memoryBlock[i] = (int *) malloc(memoryBlockSize);
        std::memset(memoryBlock[i],0,100ll*1024*1024*1024);
        // Check if memory allocation was successful
        if (memoryBlock[i] == nullptr) {
            std::cerr << "Memory allocation failed. in " << i << std::endl;
            break; // Exit loop on failure
        }

        // Do something with the allocated memory (if needed)
        // ...

        // For demonstration purposes, we will free the memory immediately after allocation.
        // In a real-world scenario, you would use the memory before freeing it.
        std::cout << "Memory allocated at address: " << memoryBlock[i] << std::endl;
        //free(memoryBlock); // Free the memory
    }
    while(1);

    std::cout << "Memory allocation loop completed." << std::endl;

    return 0;
}