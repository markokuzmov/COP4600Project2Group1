// ------------------------------------------------------------
// Project 2 - Multi-Threaded Compression Tool
// COP4600-003 - Group 1
// Marko Kuzmov, Anthony Lozbin, Jacob Moran, Jordan Nguyen
// ------------------------------------------------------------

// ---------- Libraries ----------
#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

// ---------- Macro Definitions ----------
#define BUFFER_SIZE 1048576 // 1MB buffer size.
#define MAX_WORKER_THREADS 19 // Limit for worker threads.

// ---------- Struct Definitions ----------

// Struct holding compressing data for a single file and the status of the compression for that file.
typedef struct {
    unsigned char *data;   // compressed data buffer (exact size)
    int in_size;           // Original File Size
    int out_size;          // Compressed File Size
    int ready;             // Flag to indicate completion of compression.
} result_t;

// Struct to hold shared context for all worker threads.
typedef struct {
    char *directory_name; 		// Holds the directory being processed.
    char **files;		  		// List of file names.
    int nfiles;			  		// Total number of files in the directory.
    int next_idx;         		// Next file index to be compressed.
    pthread_mutex_t idx_mutex;  // Lock to protect next_idx.

    result_t *results;			// Array of compression results.
    pthread_mutex_t res_mutex;  // Lock to protect access to the compressed results (above).
    pthread_cond_t res_cv;      // Condition variable to indicate result readiness.
} work_ctx_t;

// ---------- Function Definitions ----------

// String comparison wrapper function (used in qsort).
int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

// Routine each worker thread will run.
static void *worker_thread(void *arg) {

	// Obtaining the shared context between worker threads.
	work_ctx_t *ctx = (work_ctx_t *)arg; 

	// Allocate per-thread buffers for compression.
	unsigned char *buffer_in = (unsigned char *)malloc(BUFFER_SIZE);
	unsigned char *buffer_out = (unsigned char *)malloc(BUFFER_SIZE);
	assert(buffer_in != NULL && buffer_out != NULL);

	// Allocating reusable buffer for constructing full file paths.
	size_t path_buf_size = 512;
	char *full_path = (char *)malloc(path_buf_size);
	assert(full_path != NULL);

	// Initializing zlib stream for compression.
	z_stream strm;
	memset(&strm, 0, sizeof(strm));
	int zrc = deflateInit(&strm, 9);
	assert(zrc == Z_OK);

	while(1) {
        // Acquiring idx_mutex lock and obtaining the next file index to process.
        pthread_mutex_lock(&ctx->idx_mutex);
        int idx = ctx->next_idx;
		// Checking if there are any files left to compress.
        if (idx >= ctx->nfiles) {
			// If no more files left to compress, release lock and end loop.
            pthread_mutex_unlock(&ctx->idx_mutex);
            break;
        }
		// Incrementing next index and releasing lock.
        ctx->next_idx++;
        pthread_mutex_unlock(&ctx->idx_mutex);

        // Building full path to file (reusing buffer).
        int len = (int)strlen(ctx->directory_name) + (int)strlen(ctx->files[idx]) + 2;
		// Verifying buffer size is large enough; if not, enlarge buffer.
        if ((size_t)len > path_buf_size) {
            path_buf_size = len + 128;
            full_path = (char *)realloc(full_path, path_buf_size);
            assert(full_path != NULL);
        }
        strcpy(full_path, ctx->directory_name);
        strcat(full_path, "/");
        strcat(full_path, ctx->files[idx]);

        // Obtaining the file (in binary mode) up to 1MB in size.
		FILE *f_in = fopen(full_path, "rb");
        assert(f_in != NULL);
        int nbytes = (int)fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
        fclose(f_in);

        // Resetting zlib state before compressing a new file.
		int ret = deflateReset(&strm);
		assert(ret == Z_OK);

		// Setting up zlib input and output buffer.
        strm.avail_in = (uInt)nbytes;
        strm.next_in = buffer_in;
        strm.avail_out = BUFFER_SIZE;
        strm.next_out = buffer_out;

		// Performing compression.
        ret = deflate(&strm, Z_FINISH);
        assert(ret == Z_STREAM_END);
        int nbytes_zipped = (int)(BUFFER_SIZE - strm.avail_out);

        // Copying compressed data to an exact-sized buffer.
        unsigned char *exact = (unsigned char *)malloc(nbytes_zipped);
        assert(exact != NULL);
        memcpy(exact, buffer_out, nbytes_zipped);

        // Saving compressed result to shared struct, signaling to main, and releasing the lock.
        pthread_mutex_lock(&ctx->res_mutex);
        ctx->results[idx].data = exact;
        ctx->results[idx].in_size = nbytes;
        ctx->results[idx].out_size = nbytes_zipped;
        ctx->results[idx].ready = 1;
        pthread_cond_signal(&ctx->res_cv);
        pthread_mutex_unlock(&ctx->res_mutex);
    }
	
	// Cleaning up zlib and buffer memory.
	deflateEnd(&strm);
	free(buffer_in);
	free(buffer_out);
	free(full_path);

    return NULL;
}

// Function to compress all text files found in the directory.
void compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;

	// Opening the directory and verifying it was successful.
	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return;
	}

	// Collecting all text files from the directory.
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

	// Sorting files lexicographically using qsort and cmp wrapper function.
	qsort(files, nfiles, sizeof(char *), cmp);

	// Initializing shared context for the worker threads.
	work_ctx_t ctx;
	ctx.directory_name = directory_name;
	ctx.files = files;
	ctx.nfiles = nfiles;
	ctx.next_idx = 0;
	ctx.results = (result_t *)calloc(nfiles > 0 ? nfiles : 1, sizeof(result_t));
	assert(ctx.results != NULL);

	// Initializing locks and condition variables.
	pthread_mutex_init(&ctx.idx_mutex, NULL);
	pthread_mutex_init(&ctx.res_mutex, NULL);
	pthread_cond_init(&ctx.res_cv, NULL);

	// Determining (and limiting) worker thread count.
	int worker_count = nfiles;
	// Verifying worker threads do not exceed 19 (<= 20 threads total).
	if (worker_count > MAX_WORKER_THREADS) worker_count = MAX_WORKER_THREADS;
	if (worker_count < 1) worker_count = 1;

	// Creating worker threads.
	pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * worker_count);
	assert(threads != NULL);
	for (int t = 0; t < worker_count; ++t) {
		int rc = pthread_create(&threads[t], NULL, worker_thread, &ctx);
		assert(rc == 0);
	}

	// Opening output file (tzip file).
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "wb");
	assert(f_out != NULL);

	// Use larger buffer to reduce I/O overhead (256KB).
	setvbuf(f_out, NULL, _IOFBF, 256 * 1024);

	// Writer thread waits for compressed results in order and writes to the output file.
	for (int i = 0; i < nfiles; i++) {
		// Obtaining lock and verifying writer condition.
		pthread_mutex_lock(&ctx.res_mutex);
		while (!ctx.results[i].ready) {
			// If results not ready, writer sleeps and releases lock.
			pthread_cond_wait(&ctx.res_cv, &ctx.res_mutex);
		}
		int nbytes_zipped = ctx.results[i].out_size;
		int in_size = ctx.results[i].in_size;
		unsigned char *data = ctx.results[i].data;
		pthread_mutex_unlock(&ctx.res_mutex);

		// Write compressed block size and data.
		fwrite(&nbytes_zipped, sizeof(int), 1, f_out);
		fwrite(data, sizeof(unsigned char), nbytes_zipped, f_out);
		total_in += in_size;
		total_out += nbytes_zipped;

		// Freeing per-file compressed buffer.
		free(data);
	}
	fclose(f_out);

	// Printing out compression rate (actual percentage on success, 0 on failure).
	if (total_in > 0)
		printf("Compression rate: %.2lf%%\n", 100.0*(total_in - total_out) / total_in);
	else
		printf("Compression rate: 0.00%%\n");

	// Joining worker threads.
	for (int t = 0; t < worker_count; ++t) {
		pthread_join(threads[t], NULL);
	}

	// Freeing thread memory and destroying locks and condition variables.
	free(threads);
	pthread_mutex_destroy(&ctx.idx_mutex);
	pthread_mutex_destroy(&ctx.res_mutex);
	pthread_cond_destroy(&ctx.res_cv);
	free(ctx.results);

	// Freeing file list.
	for(int i=0; i < nfiles; i++)
		free(files[i]);
	free(files);

    return;
}
