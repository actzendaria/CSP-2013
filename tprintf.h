#ifndef TPRINTF_H
#define TPRINTF_H

#define tprintf(args...) do { \
        struct timeval tv;     \
        gettimeofday(&tv, 0); \
        printf("%ld:\t", tv.tv_sec * 1000 + tv.tv_usec / 1000);\
        printf(args);   \
        } while (0);

// change dprintf to nothing to clean debug messages
#define dprintf(args...) do { \
        /*do nothing*/ /*tprintf(args...)*/   \
        } while (0);

#endif
