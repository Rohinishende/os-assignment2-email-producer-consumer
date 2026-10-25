/*
 * Producer-Consumer Problem : EMAIL SYSTEM
 * Producers -> users who COMPOSE emails and put them in Outbox (buffer)
 * Consumers -> mail servers who TAKE emails from Outbox and SEND them
 *
 * Semaphores:
 *   mutex : binary (=1)   -> mutual exclusion on buffer
 *   empty : counting (=N) -> empty slots (producer waits if buffer full)
 *   full  : counting (=0) -> filled slots (consumer waits if buffer empty)
 *
 * Compile : gcc email_producer_consumer.c -o email_pc -pthread
 * Run     : ./email_pc
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define BUFFER_SIZE         5
#define NUM_PRODUCERS       3
#define NUM_CONSUMERS       2
#define EMAILS_PER_PRODUCER 4

typedef struct {
    int  id;                 /* -1 = stop signal */
    char from[32];
    char to[32];
    char subject[64];
} Email;

Email buffer[BUFFER_SIZE];
int in = 0, out = 0;
int count = 0;
int next_id = 1;

sem_t mutex, empty, full;

const char *users[]    = {"alice@mail.com", "bob@mail.com", "carol@mail.com"};
const char *targets[]  = {"hr@company.com", "prof@college.edu", "friend@mail.com", "team@project.org"};
const char *subjects[] = {"Meeting Schedule", "Assignment Submission", "Leave Request", "Project Update", "Invitation"};

void *producer(void *arg) {
    int pid = *(int *)arg;
    for (int i = 0; i < EMAILS_PER_PRODUCER; i++) {
        Email e;
        snprintf(e.from, sizeof e.from, "%s", users[pid % 3]);
        snprintf(e.to, sizeof e.to, "%s", targets[rand() % 4]);
        snprintf(e.subject, sizeof e.subject, "%s", subjects[rand() % 5]);

        usleep((rand() % 300 + 100) * 1000);   /* typing time */

        sem_wait(&empty);                      /* wait for empty slot */
        sem_wait(&mutex);                      /* enter critical section */

        e.id = next_id++;
        buffer[in] = e;
        in = (in + 1) % BUFFER_SIZE;
        count++;
        printf("[Producer %d] Composed Email #%02d (%s -> %s | %s)  Outbox: %d/%d\n",
               pid, e.id, e.from, e.to, e.subject, count, BUFFER_SIZE);

        sem_post(&mutex);                      /* exit critical section */
        sem_post(&full);                       /* one more filled slot */
    }
    printf("[Producer %d] Finished composing.\n", pid);
    return NULL;
}

void *consumer(void *arg) {
    int cid = *(int *)arg;
    while (1) {
        sem_wait(&full);                       /* wait until outbox NOT empty */
        sem_wait(&mutex);                      /* enter critical section */

        Email e = buffer[out];
        out = (out + 1) % BUFFER_SIZE;
        if (e.id != -1) count--;

        if (e.id != -1)
            printf("    [Consumer %d] Sending Email #%02d (%s -> %s | %s)  Outbox: %d/%d\n",
                   cid, e.id, e.from, e.to, e.subject, count, BUFFER_SIZE);

        sem_post(&mutex);                      /* exit critical section */
        sem_post(&empty);                      /* one more empty slot */

        if (e.id == -1) break;
        usleep((rand() % 500 + 200) * 1000);   /* sending time */
    }
    printf("    [Consumer %d] Mail server shutting down.\n", cid);
    return NULL;
}

int main(void) {
    pthread_t prod[NUM_PRODUCERS], cons[NUM_CONSUMERS];
    int pid[NUM_PRODUCERS], cid[NUM_CONSUMERS];

    srand(42);
    sem_init(&mutex, 0, 1);
    sem_init(&empty, 0, BUFFER_SIZE);
    sem_init(&full, 0, 0);

    printf("=== Email Producer-Consumer Simulation ===\n");
    printf("Outbox size=%d, Producers=%d, Consumers=%d, Emails/producer=%d\n\n",
           BUFFER_SIZE, NUM_PRODUCERS, NUM_CONSUMERS, EMAILS_PER_PRODUCER);

    for (int i = 0; i < NUM_CONSUMERS; i++) { cid[i] = i + 1; pthread_create(&cons[i], NULL, consumer, &cid[i]); }
    for (int i = 0; i < NUM_PRODUCERS; i++) { pid[i] = i + 1; pthread_create(&prod[i], NULL, producer, &pid[i]); }

    for (int i = 0; i < NUM_PRODUCERS; i++) pthread_join(prod[i], NULL);

    /* one stop signal per consumer */
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        Email stop; memset(&stop, 0, sizeof stop); stop.id = -1;
        sem_wait(&empty); sem_wait(&mutex);
        buffer[in] = stop; in = (in + 1) % BUFFER_SIZE;
        sem_post(&mutex); sem_post(&full);
    }
    for (int i = 0; i < NUM_CONSUMERS; i++) pthread_join(cons[i], NULL);

    printf("\nAll %d emails sent successfully. Final outbox count = %d\n",
           NUM_PRODUCERS * EMAILS_PER_PRODUCER, count);

    sem_destroy(&mutex); sem_destroy(&empty); sem_destroy(&full);
    return 0;
}
