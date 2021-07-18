/* File Name: gcc_pf_p3.h
 * Author:    Shimin Chen
 */

#ifndef _GCC_PREFETCH_P3
#define _GCC_PREFETCH_P3

#define prefetcht0(mem_var)	\
	__asm__ __volatile__ ("prefetcht0 %0": :"m"(mem_var))
#define prefetcht1(mem_var)	\
	__asm__ __volatile__ ("prefetcht1 %0": :"m"(mem_var))
//#define prefetcht2(mem_var)	
//	__asm__ __volatile__ ("prefetcht2 %0": :"m"(mem_var))
#define prefetchnta(mem_var) \
	__asm__ __volatile__ ("prefetchnta %0": :"m"(mem_var))

/*
   T0 (temporal data):
       prefetch data into all levels of the cache hierarchy.   
       Pentium III processor 1st- or 2nd-level cache.   
       Pentium 4 and Intel Xeon processors 2nd-level cache.

   T1 (temporal data with respect to first level cache) 
       prefetch data into level 2 cache and higher.   
       Pentium III processor 2nd-level cache.   
       Pentium 4 and Intel Xeon processors 2nd-level cache.

   T2 (temporal data with respect to second level cache) 
       prefetch data into level 2 cache and higher.   
       Pentium III processor 2nd-level cache.   
       Pentium 4 and Intel Xeon processors 2nd-level cache.

   NTA (non-temporal data with respect to all cache levels) 
        prefetch data into non-temporal cache structure and 
        into a location close to the processor, minimizing cache pollution.   
        Pentium III processor 1st-level cache   
        Pentium 4 and Intel Xeon processors 2nd-level cache
 */

#define pfld(mem_var)	   prefetcht0(mem_var)
#define pfst(mem_var)	   prefetcht0(mem_var)
#define pfldnta(mem_var)   prefetchnta(mem_var)
#define pfstnta(mem_var)   prefetchnta(mem_var)

#define ptouch(mem_var)	\
	__asm__ __volatile__ ("movl %0, %%eax": :"m"(mem_var):"%eax")

#endif /* _GCC_PREFETCH_P3 */
