
#ifndef LOGGER_H
#define LOGGER_H

#define INFO_LEVEL 1
#define DEBUG_LEVEL 2
#define WARN_LEVEL 3
#define ERROR_LEVEL 4
#define NO_DEBUG_LEVEL 5
#define LOG_LEVEL NO_DEBUG_LEVEL    // 设置的日志级别

// #if (LOG_LEVEL == NO_DEBUG_LEVEL)
//     #define ENABLE_DEBUG 0              // 是否开启 debug 模式，值只取 0、1 两种，0 表示不开启，1 表示开启（设置为 1 时，可用于执行一些调试代码）
// #else
//     #define ENABLE_DEBUG 1
// #endif

// INFO < DEBUG < WARN < ERROR < NO
// 1      2         3       4    5

#define WORKER_LOG(i, format, args...)           \
    if (get_worker_id() == i && LOG_LEVEL <= ERROR_LEVEL) \
        printf(format, ##args);

#define LOG_LINE(format, args...) (printf("[LINE]-[%s]-[%s]-[worker %d]-[thread %ld]-[%d]:" format, __FILE__, __FUNCTION__ ,  _my_rank, std::hash<std::thread::id>{}(std::this_thread::get_id()), __LINE__, ##args))
// #define LOG_LINE(...)

#if (LOG_LEVEL <= INFO_LEVEL)
    #define LOG_INFO(format, args...) (printf("[INFO]-[%s]-[%s]-[worker %d]-[thread %ld]-[%d]:" format, __FILE__, __FUNCTION__ ,  _my_rank, std::hash<std::thread::id>{}(std::this_thread::get_id()), __LINE__, ##args))
#else
    #define LOG_INFO(...)
#endif

// 调用 printf 输出信息，不包含任何额外信息
#if (LOG_LEVEL <= WARN_LEVEL)
    #define LOG_MSG(format, args...) (printf(format, ##args))
#else
    #define LOG_MSG(...)
#endif

#if (LOG_LEVEL <= DEBUG_LEVEL)
    #define LOG_DEBUG(format, args...) (printf("[DEBUG]-[%s]-[%s]-[worker %d]-[thread %ld]-[%d]:" format, __FILE__, __FUNCTION__ ,  _my_rank, std::hash<std::thread::id>{}(std::this_thread::get_id()), __LINE__, ##args))
#else
    #define LOG_DEBUG(...)
#endif

#if (LOG_LEVEL <= WARN_LEVEL)
    #define LOG_WARN(format, args...) (printf("[WARN]-[%s]-[%s]-[worker %d]-[thread %ld]-[%d]:" format, __FILE__, __FUNCTION__ ,  _my_rank, std::hash<std::thread::id>{}(std::this_thread::get_id()), __LINE__, ##args))
#else
    #define LOG_WARN(...)
#endif

#if (LOG_LEVEL <= ERROR_LEVEL)
    #define LOG_ERROR(format, args...) (printf("[ERROR]-[%s]-[%s]-[worker %d]-[thread %ld]-[%d]:" format, __FILE__, __FUNCTION__ ,  _my_rank, std::hash<std::thread::id>{}(std::this_thread::get_id()), __LINE__, ##args))
#else
    #define LOG_ERROR(...)
#endif

#endif // LOGGER_H
