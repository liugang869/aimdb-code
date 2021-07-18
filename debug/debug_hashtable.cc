#include "hashtable.h"
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <map>

#define CAPICITY (1000000)

using namespace std;

int test1()
{
    printf ("test 1...\n");
    unsigned int key = 0;
    char *tup = NULL;
    struct timeval start, end;
    unsigned long diff;

    HashTable *ht = new HashTable(1000000, 2, 0);
    srand(100);

    gettimeofday(&start, NULL);
    for (int i = 0; i < CAPICITY; i++) {
        key = rand();
        ht->add(key, tup);
        tup++;
    }
    gettimeofday(&end, NULL);

    diff =
        1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec -
        start.tv_usec;
    printf("\nhashtable by Chen the time-use is %ld us\n", diff);

    ht->utilization();
    //ht->show();
    return 0;
}

int test2()
{
    printf ("\ntest 2...\n");
    unsigned int key = 0;
    char *tup = (char *) 1;
    struct timeval start, end;
    unsigned long diff;

    multimap < int, char *>*mp = new multimap < int, char *>();
    srand(100);
    gettimeofday(&start, NULL);
    for (int i = 0; i < CAPICITY; i++) {
        key = rand() % CAPICITY;
        mp->insert(pair < int, char *>(key, tup));
        tup++;
    }
    gettimeofday(&end, NULL);

    diff =
        1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec -
        start.tv_usec;
    printf("\nhash stl multimap the time-use is %ld us\n", diff);

    //ht->show();
    return 0;

}

int test3()
{
    printf("\ntest3...\n");
    HashTable *ht = new HashTable(10, 2, 0);
    for(int64_t ii=1;ii<50;ii++)
        ht->add(ii, (char*)ii);
    for(int64_t ii=1;ii<50;ii++)
        ht->add(ii, (char*)ii);
    printf("\n");
    ht->show();
    return 0;
}

int main()
{
    printf("Hashcode_Ptr size: %lu\n",sizeof(Hashcode_Ptr));
    printf("HashCell size: %lu\n\n",sizeof(HashCell));
    srand(100);
    g_memory.init (1<<30,1<<3);
    test1();
    test2();
    test3();
    printf("\ntest pass!\n");
    g_memory.shut();
    return 0;
}
