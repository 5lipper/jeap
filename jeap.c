#define _BSD_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <sched.h>

#include <pthread.h>

#ifdef __gnu_linux__
#include <jemalloc/jemalloc.h>
#endif

#ifdef __FREEBSD__
#include <malloc_np.h>
#endif

#define NWORKER 0x4

#ifdef DEBUG
volatile int log_level = 2;

#define debug(...) do { if (log_level <= 0) fprintf(stderr, __VA_ARGS__); } while (0)
#define info(...) do { if (log_level <= 1) fprintf(stderr, __VA_ARGS__); } while (0)
#define warning(...) do { if (log_level <= 2) fprintf(stderr, __VA_ARGS__); } while (0)

#else
#define debug(...) do {} while (0)
#define info(...) do {} while (0)
#define warning(...) do {} while (0)
#endif
		
#define MAX_LENGTH 0x1000
#define HTABLE_SIZE 0x1000

typedef enum {
		CMD_NONE = 0, // abstract command
		CMD_SET,
		CMD_ADD,
		CMD_REPLACE,
		CMD_APPEND,
		CMD_PREPEND,
		CMD_CAS,
		CMD_STORE, // abstract command
		CMD_GET,
		CMD_GETS = CMD_GET,
		CMD_DELETE,
		CMD_INCR,
		CMD_DECR,
		CMD_STAT,
		CMD_0OPS,
		CMD_MAX = 0x10, // abstract command
} cmd_t;

typedef struct job_s {
		int id;
		int flags;
		cmd_t cmd;
		int noreply;
		char *key;
		char *value;
		char *error;
		size_t length;
		time_t exptime;
		uint64_t casunique;

		int done;
		pthread_cond_t cond;
		pthread_mutex_t lock;
} job_t;

typedef struct item_s {
		struct item_s *next;
		struct item_s *prev;
		char *key;
		char *value;
		size_t length;
		time_t exptime;
		int dead;
		int flags;
		// uint64_t casunique;
} item_t;

typedef struct entry_s {
		struct item_s *next;
		struct item_s *prev;
		uint32_t hash;
		pthread_mutex_t lock;
} entry_t;

typedef void (*handler_t)(job_t *);
handler_t handlers[CMD_MAX];
entry_t htable[HTABLE_SIZE];

pthread_cond_t cond;
pthread_mutex_t lock;
volatile int worker_all;
volatile int job_done;
volatile int job_all;
volatile job_t *cur_job;
volatile int stopped;

pthread_t workers[NWORKER];

#ifdef DEBUG
volatile int trap_cnt = 0;
void trap(int stop) {
		// trap_cnt++;
		if (stop)
				__asm__  volatile ( "int $0x3" );
		else {
				trap_cnt++;
				// log_level = 0;
		}
}
#endif

void destroy_job(job_t *job) {
		debug("destroy %p\n", job);
		if (job->key) {
				free(job->key);
				job->key = NULL;
		}

		if (job->cmd < CMD_STORE && job->value) {
				free(job->value);
				job->value = NULL;
				job->length = 0;
		}

		free(job);
}

job_t *get_job(int wid) {
		job_t *ret = NULL;
		int waiting = 1;

		while (waiting == 1) {
				pthread_mutex_lock(&lock);

				if (stopped) {
						pthread_mutex_unlock(&lock);
						break;
				}

				worker_all++; // add a worker to list

				debug("worker #%d waiting\n", wid);
				pthread_cond_wait(&cond, &lock);

				if (cur_job){
						ret = *(job_t **)&cur_job;
						cur_job = NULL; // get a new job
						waiting = 0;
						debug("worker #%d get a new job\n", wid);
				} else if (stopped) {
						waiting = -1;
						info("worker #%d retired\n", wid);
				}
				worker_all--;

				pthread_mutex_unlock(&lock);
		}

		return ret;
}

void put_job(job_t *job) {
		int waiting;

		while (1) {
				pthread_mutex_lock(&lock);
				if (worker_all > 0 && cur_job == NULL && job_all - job_done < NWORKER) {
						cur_job = job;
						job->id = ++job_all;
						pthread_mutex_init(&job->lock, NULL);
						pthread_cond_init(&job->cond, NULL);
						waiting = 0;
						info("put a new job #%d (%p)\n", job->id, job);
						// pthread_cond_signal(&cond);
						pthread_cond_broadcast(&cond);
				} else {
						waiting = 1;
						// debug("pending job (%p)\n", job);
				}
				pthread_mutex_unlock(&lock);
				// sched_yield();
				if (waiting) {
						// sleep(1);
						// sched_yield();
				} else {
						break;
				}
		}

}

void done_job(job_t *job) {
		pthread_mutex_lock(&job->lock);
		job->done = 1;
		pthread_cond_signal(&job->cond);
		pthread_mutex_unlock(&job->lock);
		sched_yield();

		pthread_mutex_lock(&lock);
		job_done++;
		pthread_mutex_unlock(&lock);
}

void worker(int wid) {
		job_t *job = NULL;

		while ((job = get_job(wid)) != NULL) {
				handler_t h = handlers[job->cmd];

				info("worker #%d get job #%d (cmd = %d)\n", wid, job->id, job->cmd);

				if (h == NULL) {
						job->error = "NOT_IMPLEMENT";
				} else {
						h(job);
				}
				
				done_job(job);

				if (job->noreply) {
#ifdef DEBUG
						if (job->error && 0)
								warning("%s\n", job->error);

#endif
						destroy_job(job);
				}
		}

}

int recvn(int fd, char *buf, int n) {
		int i;
		for (i = 0; i < n; ) {
				int r = read(fd, &buf[i], n - i);
				if (r == -1) {
						break;
				}
				i += r;
		}
		return i;
}

int recvline(int fd, char *buf, int n) {
		int i;
		for (i = 0; i < n; i++) {
				int r = read(fd, &buf[i], 1);
				if (r == -1) {
						break;
				} else if (r == 1 && (buf[i] == '\n'))
						break;
		}
		buf[i] = '\0';
		if (i > 1 && buf[i - 1] == '\r')
				buf[--i] = '\0';
		return i;
}

void sendn(int fd, char *buf, int n) {
		write(fd, buf, n);
		write(fd, "\r\n", 2);
}

void sendline(int fd, char *buf) {
		write(fd, buf, strlen(buf));
		write(fd, "\r\n", 2);
}


uint32_t hash(char *s) {
		uint32_t c = 0;
		if (!s)
				return 0;
		for (uint8_t *a = (uint8_t *)s; *a; a++)
				c = c * 31 + *a;
		return c % HTABLE_SIZE;
}

// safe version
char *_strdup(const char *s) {
		if (s == NULL)
				return NULL;
		return strdup(s);
}

void *_memcpy(void *dest, const void *src, size_t n) {
		if (n == 0 || src == NULL)
				return dest;
		return memcpy(dest, src, n);
}

int _strcmp(const char *s1, const char *s2) {
		if (s1 == NULL || s2 == NULL)
				return s1 == s2 ? 0 : 1;
		return strcmp(s1, s2);
}

void _free(void *ptr) {
		if (ptr != NULL)
				free(ptr);
}

void fatal(char *s) {
		sendline(1, s);
		exit(1);
}

void _unlink(item_t *i) {
		item_t *p = i->prev;
		item_t *n = i->next;
		if (p->next != i || n->prev != i)
				fatal("SERVER_ERROR corrupted item");
		p->next = n;
		n->prev = p;
		_free(i);
}

item_t *lookup(entry_t *e, char *k) {
		// must hold entry lock
		item_t *ret = NULL;
		item_t *next = NULL;
		time_t t = time(NULL);
		for (item_t *i = e->next; i != (item_t *)e; i = next) {
				if (i->prev->next != i || i->next->prev != i) {
						fatal("SERVER_ERROR corrupted item");
				}
				next = i->next;
				if (i->exptime && i->exptime < t) {
						// expired
						_unlink(i);
						continue;
				}
				if (!_strcmp(i->key, k)) {
						ret = i;
						break;
				}
		}
		return ret;
}

char *insert(entry_t *e, item_t *i) {
		// must hold entry lock
		// linklist check
		item_t *h = e->next;
		if (h->prev != (item_t *)e)
				fatal("SERVER_ERROR corrupted entry");
		e->next = i;
		h->prev = i;
		i->next = h;
		i->prev = (item_t *)e;
}

void handle_set(job_t *job) {
		uint32_t h = hash(job->key);
		item_t *i;

		pthread_mutex_lock(&htable[h].lock);

		i = lookup(&htable[h], job->key);

		if (i == NULL) { // create a new one
				i = malloc(sizeof(item_t));
				memset(i, 0, sizeof(item_t));
				i->value = job->value;
				job->value = NULL;
				i->length = job->length;
				i->exptime = job->exptime;
				i->dead = 0;
				i->flags = job->flags;
				i->key = _strdup(job->key);
				insert(&htable[h], i);
		} else if (job->length == i->length) {
				// shortcut
				_memcpy(i->value, job->value, i->length);
				i->exptime = job->exptime;
				i->dead = 0;
				i->flags = job->flags;
		} else { // update one item
				_free(i->value);
				i->value = job->value;
				job->value = NULL;
				i->length = job->length;
				i->exptime = job->exptime;
				i->dead = 0;
				i->flags = job->flags;
		}

		pthread_mutex_unlock(&htable[h].lock);
}

void handle_add(job_t *job) {
		uint32_t h = hash(job->key);
		item_t *i;
		pthread_mutex_lock(&htable[h].lock);
		i = lookup(&htable[h], job->key);
		if (i == NULL) {
				i = malloc(sizeof(item_t));
				memset(i, 0, sizeof(item_t));
				i->value = job->value;
				job->value = NULL;
				i->length = job->length;
				i->exptime = job->exptime;
				i->dead = 0;
				i->flags = job->flags;
				i->key = _strdup(job->key);
				insert(&htable[h], i);
		} else {
				job->error = "EXISTS";
		}
		pthread_mutex_unlock(&htable[h].lock);
}

void handle_replace(job_t *job) {
		uint32_t h = hash(job->key);
		item_t *i;
		pthread_mutex_lock(&htable[h].lock);
		i = lookup(&htable[h], job->key);
		if (i == NULL || i->dead) {
				job->error = "NOT_FOUND";
		} else {
				_free(i->value);
				i->value = job->value;
				job->value = NULL;
				i->length = job->length;
		}
		pthread_mutex_unlock(&htable[h].lock);
}

void handle_append(job_t *job) {
		uint32_t h = hash(job->key);
		item_t *i;
		pthread_mutex_lock(&htable[h].lock);
		i = lookup(&htable[h], job->key);
		if (i == NULL || i->dead) {
				job->error = "NOT_FOUND";
		} else if (job->length != 0) {
				char *value = malloc(job->length + i->length);
				if (i->value == NULL)
						job->error = "NOT_STORED";
				else {
						_memcpy(value, i->value, i->length);
						_memcpy(&value[i->length], job->value, job->length);
						_free(i->value);
						i->value = value;
						i->length += job->length;
				}
		}
		pthread_mutex_unlock(&htable[h].lock);
}

void handle_prepend(job_t *job) {
		uint32_t h = hash(job->key);
		item_t *i;
		pthread_mutex_lock(&htable[h].lock);
		i = lookup(&htable[h], job->key);
		if (i == NULL || i->dead) {
				job->error = "NOT_FOUND";
		} else if (job->length != 0) {
				char *value = malloc(job->length + i->length);
				if (i->value == NULL)
						job->error = "NOT_STORED";
				else {
						_memcpy(value, job->value, job->length);
						_memcpy(&value[job->length], i->value, i->length);
						_free(i->value);
						i->value = value;
						i->length += job->length;
				}
		}
		pthread_mutex_unlock(&htable[h].lock);
}

void handle_get(job_t *job) {
		uint32_t h = hash(job->key);
		item_t *i;

		pthread_mutex_lock(&htable[h].lock);

		i = lookup(&htable[h], job->key);
		if (i == NULL || i->dead) {
				job->error = "NOT_FOUND";
		} else {
				job->value = i->value;
				job->length = i->length;
				job->flags = i->flags;
		}

		pthread_mutex_unlock(&htable[h].lock);
}

void handle_delete(job_t *job) {
		uint32_t h = hash(job->key);
		item_t *i;

		pthread_mutex_lock(&htable[h].lock);

		i = lookup(&htable[h], job->key);
		if (i == NULL || (i->dead && job->exptime > i->exptime)) {
				job->error = "NOT_FOUND";
		} else {
				i->dead = 1;
				i->exptime = job->exptime;
		}

		pthread_mutex_unlock(&htable[h].lock);
}

void handle_stat(job_t *job) {
		job->error = "jeapcached v0.095";
}

void handle_0ops(job_t *job) {
#ifdef DEBUG
		printf("malloc(%lu) = %p\r\n", sizeof(item_t), malloc(sizeof(item_t)));
#else
		job->error = "NO_BACKDOOR";
#endif
}

void init() {
		long i;
		debug("init\n");

		handlers[CMD_SET] = handle_set;
		handlers[CMD_ADD] = handle_add;
		handlers[CMD_REPLACE] = handle_replace;
		handlers[CMD_APPEND] = handle_append;
		handlers[CMD_PREPEND] = handle_prepend;
		handlers[CMD_GET] = handle_get;
		handlers[CMD_DELETE] = handle_delete;
		handlers[CMD_STAT] = handle_stat;
		handlers[CMD_0OPS] = handle_0ops;

		pthread_cond_init(&cond, NULL);
		pthread_mutex_init(&lock, NULL);

		stopped = 0;
		cur_job = NULL;
		job_done = 0;
		job_all = 0;
		worker_all = 0;

		for (i = 0; i < NWORKER; i++) {
				pthread_create(&workers[i], NULL, (void *)worker, (void *)i);
		}

		memset(htable, 0, sizeof(htable));
		for (i = 0; i < HTABLE_SIZE; i++) {
				htable[i].prev = htable[i].next = (item_t *)&htable[i];
				pthread_mutex_init(&htable[i].lock, NULL);
		}
}

void fini() {
		int i;
		debug("fini\n");
		for (i = 0; i < NWORKER; i++)
				pthread_join(workers[i], NULL);

		pthread_cond_destroy(&cond);
		pthread_mutex_destroy(&lock);

		// TODO destroy more locks
}

void stop() {
		int waiting = 1;
		debug("stop\n");
		do {
				pthread_mutex_lock(&lock);
				if (worker_all) {
					stopped = 1;
					pthread_cond_broadcast(&cond);
				} else {
					waiting = 0;
				}
				pthread_mutex_unlock(&lock);
		} while (waiting);
		// hide error messages of main thread :)
		exit(0);
}

void test_job() {
		int i = 0;

		getchar();

		for (i = 0; i < 10; i++) {
				job_t *job = malloc(sizeof(job_t));
				job->cmd = i * i;
				put_job(job);
		}
}

job_t *parse_args(char *buf) {
		char command[0x10];
		char key[0x100];
		uint64_t args[5];
		int scanned;

		job_t *job = (job_t *)malloc(sizeof(job_t));
		memset(job, 0, sizeof(job_t));

		if (sscanf(buf, "%10s %250s%n", command, key, &scanned) == 2) {
				job->key = _strdup(key);

				if (!strcasecmp(command, "get") || !strcasecmp(command, "gets")) {
						job->cmd = CMD_GET;
				} else if (!strcasecmp(command, "delete")) {
						job->cmd = CMD_DELETE;
						int argc = sscanf(&buf[scanned], "%ld %d", &job->exptime, &job->noreply);
						if (job->exptime == 0)
								job->exptime = 1;
						else
								job->exptime += time(NULL);
				} else if (!strcasecmp(command, "stat")) {
						job->cmd = CMD_STAT;
				} else if (!strcasecmp(command, "incr") || !strcasecmp(command, "decr")) {
						int argc = sscanf(&buf[scanned], "%ld %d", (uint64_t *)&job->value, &job->noreply);
						if (command[0] == 'i' || command[0] == 'I')
								job->cmd = CMD_INCR;
						else
								job->cmd = CMD_DECR;

				} else if (!strcasecmp(command, "set")
						|| !strcasecmp(command, "add")
						|| !strcasecmp(command, "replace")
						|| !strcasecmp(command, "append")
						|| !strcasecmp(command, "prepend")
						|| !strcasecmp(command, "cas")) {
					int argc = sscanf(&buf[scanned], "%ld %ld %ld %ld %ld", &args[0], &args[1], &args[2], &args[3], &args[4]);

					switch (command[0]) {
							case 's':
							case 'S':
									job->cmd = CMD_SET;
									break;
							case 'r':
							case 'R':
									job->cmd = CMD_REPLACE;
									break;
							case 'p':
							case 'P':
									job->cmd = CMD_PREPEND;
									break;
							case 'c':
							case 'C':
									job->cmd = CMD_CAS;
									break;
							default:
									if (command[1] == 'd' || command[1] == 'D')
											job->cmd = CMD_ADD;
									else
											job->cmd = CMD_APPEND;
					}

					if (argc == 1) {
							job->length = args[0];
					} else if (argc == 2) {
							job->flags = args[0];
							job->length = args[1];
					} else if (argc >= 3) {
							job->flags = args[0];
							job->exptime = args[1];
							job->length = args[2];
							if (argc == 4)
									job->noreply = args[3];
							else { // argc == 5
									job->casunique = args[3];
									job->noreply = args[4];
							}
					} else { // argc == 0
							job->error = "NOT_STORED";
					}
				} else if (!strcasecmp(command, "0ops")) {
						job->cmd = CMD_0OPS;
				} else {
						job->error = "ERROR";
				}

				// sanity checks
				if (!job->cmd) {
						job->error = "ERROR";
				} else if(job->length > MAX_LENGTH) {
						job->error = "NOT_STORED";
				}

		} else {
				job->error = "ERROR";
		}

		return job;
}

void handler() {
		char buf[0x200];
		job_t *job;

		while (recvline(0, buf, 0x100)) {
				job = parse_args(buf);
				if (!job->error) {
						debug("got a job: cmd = %d\n", job->cmd);
						// read length for a store command
						if (job->cmd < CMD_STORE) {
								if ((job->value = malloc(job->length)) == NULL) {
										job->error = "NOT_STORED";
										goto error;
								}
								debug("reading %ld bytes => %p\n", job->length, job->value);
								recvn(0, job->value, job->length);
						}

						if (handlers[job->cmd] == NULL) {
								job->error = "NOT_IMPLEMENT";
								goto error;
						}

						if (job->key == NULL) {
								job->error = "NOT_STORED";
								goto error;
						}

						put_job(job);
						
						// wait until the job is done
						if (!job->noreply) {
								pthread_mutex_lock(&job->lock);
								if (!job->done)
									pthread_cond_wait(&job->cond, &job->lock);
								pthread_mutex_unlock(&job->lock);
						}

						if (!job->noreply && !job->error) {
								// handle some normal result
								if (job->cmd < CMD_STORE)
										sendline(1, "STORED");
								else if (job->cmd == CMD_GET) {
										snprintf(buf, sizeof(buf), "VALUE %s %u %lu", job->key, job->flags, job->length);
										sendline(1, buf);
										sendn(1, job->value, job->length);
								} else if (job->cmd == CMD_DELETE) {
										sendline(1, "DELETED");
								}
						}
				}

error:
				if (!job->noreply) {
						if (job->error)
								sendline(1, job->error);

						destroy_job(job);
				}
		}
}

int main(int argc, char* argv[]) {
#ifdef DEBUG
		if (getenv("NODEBUG"))
				fclose(stderr);
#endif
		if (argc > 1) {
				alarm(atoi(argv[1]));
		}
		init();
		handler();
		stop();
		fini();
		return 0;
}


