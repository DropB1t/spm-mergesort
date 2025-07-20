#if !defined(DEFINES_HPP)
#define DEFINES_HPP

#include <string>
#include <mpi.h>
#include <iostream>
#include <cstdint>

enum class ExecutionPolicy {
    Sequential,
    Parallel,
    OMP,
    FastFlow,
    MPI_FF
};

using enum ExecutionPolicy;

constexpr std::string_view ep_to_string(ExecutionPolicy c) {
    switch (c) {
        case Sequential:    return "Sequential";
        case Parallel:      return "Parallel";
        case OMP:           return "OMP";
        case FastFlow:      return "FastFlow";
        case MPI_FF:        return "MPI_FF";
    }
    return "Unknown";
}

#define CHECK_ERROR(err) do {								\
	if (err != MPI_SUCCESS) {								\
		char errstr[MPI_MAX_ERROR_STRING];					\
		int errlen=0;										\
		MPI_Error_string(err,errstr,&errlen);				\
		std::cerr											\
			<< "MPI error code " << err						\
			<< " (" << std::string(errstr,errlen) << ")"	\
			<< " line: " << __LINE__ << "\n";				\
		MPI_Abort(MPI_COMM_WORLD, err);						\
		std::abort();										\
	}														\
} while(0)

/* RECORD PARAMETERS */
constexpr std::string INPUT_FILE = "records.dat";
constexpr std::string OUTPUT_FILE = "sorted.txt";

uint32_t PAYLOAD_MAX = 1024; // Maximum payload size in bytes
uint32_t PAYLOAD_MIN = 8; // Minimum payload size in bytes

/* EXECUTION PARAMETERS */
ExecutionPolicy g_policy = MPI_FF; // Execution policy
size_t th_workers = 8; // Number of threads for parallel (shared) processing
size_t max_chunk_size = 250000; // Maximum chunk size of records

/* MPI PARAMETERS */
int cluster_size; // Number of MPI processes in the cluster
constexpr int WR_TAG = 1;
constexpr int ACK_TAG = 2;
constexpr int COLLECT_TAG = 3;
constexpr int EOS_TAG = 4;
double t_start, t_start_emitter, t_end;

#endif