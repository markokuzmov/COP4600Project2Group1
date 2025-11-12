#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_WORKER_THREADS 19 // including main => total <= 20

int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

typedef struct {
    unsigned char *data;   // compressed data buffer (exact size)
    int in_size;           // original bytes read
    int out_size;          // compressed size
    int ready;             // flag set by worker when result is ready
} result_t;

typedef struct {
    char *directory_name;
    char **files;
    int nfiles;
    int next_idx; // next job index to dispatch
    pthread_mutex_t idx_mutex;

    result_t *results;
    pthread_mutex_t res_mutex;
    pthread_cond_t res_cv;
} work_ctx_t;

static void *worker_thread(void *arg) {
	work_ctx_t *ctx = (work_ctx_t *)arg;
	// Allocate per-thread buffers and initialize zlib once
	unsigned char *buffer_in = (unsigned char *)malloc(BUFFER_SIZE);
	unsigned char *buffer_out = (unsigned char *)malloc(BUFFER_SIZE);
	assert(buffer_in != NULL && buffer_out != NULL);

	// Reusable path buffer (max likely path length)
	size_t path_buf_size = 512;
	char *full_path = (char *)malloc(path_buf_size);
	assert(full_path != NULL);

	z_stream strm;
	memset(&strm, 0, sizeof(strm));
	int zrc = deflateInit(&strm, 9);  // level 9: max compression (required for exact match)
	assert(zrc == Z_OK);
	for(;;) {
        // fetch next job index
        pthread_mutex_lock(&ctx->idx_mutex);
        int idx = ctx->next_idx;
        if (idx >= ctx->nfiles) {
            pthread_mutex_unlock(&ctx->idx_mutex);
            break; // no more work
        }
        ctx->next_idx++;
        pthread_mutex_unlock(&ctx->idx_mutex);

        // build full path (reuse buffer)
        int len = (int)strlen(ctx->directory_name) + (int)strlen(ctx->files[idx]) + 2;
        if ((size_t)len > path_buf_size) {
            path_buf_size = len + 128;
            full_path = (char *)realloc(full_path, path_buf_size);
            assert(full_path != NULL);
        }
        strcpy(full_path, ctx->directory_name);
        strcat(full_path, "/");
        strcat(full_path, ctx->files[idx]);

        // load file (up to BUFFER_SIZE)
		FILE *f_in = fopen(full_path, "rb");
        assert(f_in != NULL);
        int nbytes = (int)fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
        fclose(f_in);

        // compress
		int ret = deflateReset(&strm);
		assert(ret == Z_OK);
        strm.avail_in = (uInt)nbytes;
        strm.next_in = buffer_in;
        strm.avail_out = BUFFER_SIZE;
        strm.next_out = buffer_out;

        ret = deflate(&strm, Z_FINISH);
        assert(ret == Z_STREAM_END);
        int nbytes_zipped = (int)(BUFFER_SIZE - strm.avail_out);

        // shrink to exact size for storage
        unsigned char *exact = (unsigned char *)malloc(nbytes_zipped);
        assert(exact != NULL);
        memcpy(exact, buffer_out, nbytes_zipped);

        // publish result (use signal - only one waiter per idx)
        pthread_mutex_lock(&ctx->res_mutex);
        ctx->results[idx].data = exact;
        ctx->results[idx].in_size = nbytes;
        ctx->results[idx].out_size = nbytes_zipped;
        ctx->results[idx].ready = 1;
        pthread_cond_signal(&ctx->res_cv);
        pthread_mutex_unlock(&ctx->res_mutex);
    }
	deflateEnd(&strm);
	free(buffer_in);
	free(buffer_out);
	free(full_path);
    return NULL;
}

void compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return;
	}

	// create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		int len = (int)strlen(dir->d_name);
		if (len >= 4 && dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
			files = realloc(files, (nfiles+1)*sizeof(char *));
			assert(files != NULL);
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);
			nfiles++;
		}
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);

	// Prepare threading context
	work_ctx_t ctx;
	ctx.directory_name = directory_name;
	ctx.files = files;
	ctx.nfiles = nfiles;
	ctx.next_idx = 0;
	ctx.results = (result_t *)calloc(nfiles > 0 ? nfiles : 1, sizeof(result_t));
	assert(ctx.results != NULL);
	pthread_mutex_init(&ctx.idx_mutex, NULL);
	pthread_mutex_init(&ctx.res_mutex, NULL);
	pthread_cond_init(&ctx.res_cv, NULL);

	int worker_count = nfiles;
	if (worker_count > MAX_WORKER_THREADS) worker_count = MAX_WORKER_THREADS;
	int cores = 4;
	if (worker_count > cores) worker_count = cores;
	if (worker_count < 1) worker_count = 1; // still run writer path cleanly

	pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * worker_count);
	assert(threads != NULL);
	for (int t = 0; t < worker_count; ++t) {
		int rc = pthread_create(&threads[t], NULL, worker_thread, &ctx);
		assert(rc == 0);
	}

	// Writer: create a single zipped package with all text files in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "wb");
	assert(f_out != NULL);
	// Use larger buffer for output to reduce write syscalls
	setvbuf(f_out, NULL, _IOFBF, 256 * 1024);  // 256KB buffer
	for (int i = 0; i < nfiles; ++i) {
		pthread_mutex_lock(&ctx.res_mutex);
		while (!ctx.results[i].ready) {
			pthread_cond_wait(&ctx.res_cv, &ctx.res_mutex);
		}
		int nbytes_zipped = ctx.results[i].out_size;
		int in_size = ctx.results[i].in_size;
		unsigned char *data = ctx.results[i].data;
		pthread_mutex_unlock(&ctx.res_mutex);

		// dump zipped file
		fwrite(&nbytes_zipped, sizeof(int), 1, f_out);
		fwrite(data, sizeof(unsigned char), nbytes_zipped, f_out);
		total_in += in_size;
		total_out += nbytes_zipped;

		free(data);
	}
	fclose(f_out);

	if (total_in > 0)
		printf("Compression rate: %.2lf%%\n", 100.0*(total_in - total_out) / total_in);
	else
		printf("Compression rate: 0.00%%\n");

	// join workers and cleanup
	for (int t = 0; t < worker_count; ++t) {
		pthread_join(threads[t], NULL);
	}
	free(threads);
	pthread_mutex_destroy(&ctx.idx_mutex);
	pthread_mutex_destroy(&ctx.res_mutex);
	pthread_cond_destroy(&ctx.res_cv);
	free(ctx.results);

	// release list of files
	for(int i=0; i < nfiles; i++)
		free(files[i]);
	free(files);

	// do not modify the main function after this point!
    return;
}
