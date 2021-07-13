#ifndef MEMORYUTIL_H
#define MEMORYUTIL_H

#include <sys/sysinfo.h>

/**
 * 获取剩余的内存空间大小，单位：字节
 */
unsigned long long get_free_memory_byte()
{
    struct sysinfo memInfo;

    sysinfo(&memInfo);
    return memInfo.freeram + memInfo.freeswap;
}
#endif // MEMORYUTIL_H
