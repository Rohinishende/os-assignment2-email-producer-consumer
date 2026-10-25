/*
 * RACE CONDITION DEMO (Email counter)
 * Two threads "send" emails and increment a shared counter.
 * Without mutex -> lost updates (wrong result).
 * With mutex    -> correct result.
 *
 * Compile : gcc race_condition_demo.c -o race -pthread
 * Run     : ./race
 */
#include <stdio.h>
#include <pthread.h>
#include <sched.h>

#define N 100000
volatile long total_sent = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

void *send_unsafe(void *a) {                 /* NO synchronization */
    for (int i = 0; i < N; i++) {
        long temp = total_sent;              /* 1. READ */
        if (i % 100 == 0) sched_yield();     /* context switch can happen here */
        total_sent = temp + 1;               /* 2. WRITE (may overwrite other thread's update) */
    }
    return NULL;
}

void *send_safe(void *a) {                   /* WITH mutex */
    for (int i = 0; i < N; i++) {
        pthread_mutex_lock(&lock);
        total_sent++;
        pthread_mutex_unlock(&lock);
    }
    return NULL;
}

int main(void) {
    pthread_t t1, t2;

    total_sent = 0;
    pthread_create(&t1, NULL, send_unsafe, NULL);
    pthread_create(&t2, NULL, send_unsafe, NULL);
    pthread_join(t1, NULL); pthread_join(t2, NULL);
    printf("WITHOUT mutex : expected %d, got %ld (race condition!)\n", 2 * N, total_sent);

    total_sent = 0;
    pthread_create(&t1, NULL, send_safe, NULL);
    pthread_create(&t2, NULL, send_safe, NULL);
    pthread_join(t1, NULL); pthread_join(t2, NULL);
    printf("WITH mutex    : expected %d, got %ld (correct)\n", 2 * N, total_sent);
    return 0;
}
