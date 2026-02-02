#if !defined(DEFINES_HPP)
#define DEFINES_HPP

#include <mpi.h>
#include <string>
#include <stdexcept>
#include <cstdint>

enum class ExecutionPolicy {
    Sequential,
    Parallel,
    OMP,
    FastFlow,
    MPI_FF
};

using enum ExecutionPolicy;

constexpr std::string ep_to_string(ExecutionPolicy c) {
    switch (c) {
        case Sequential:    return "Sequential";
        case Parallel:      return "Parallel";
        case OMP:           return "OMP";
        case FastFlow:      return "FastFlow";
        case MPI_FF:        return "MPI_FF";
    }
    return "Unknown";
}

constexpr ExecutionPolicy string_to_ep(std::string_view str) {
    if (str == "Sequential") return Sequential;
    if (str == "Parallel")   return Parallel;
    if (str == "OMP")        return OMP;
    if (str == "FastFlow")   return FastFlow;
    if (str == "MPI_FF")     return MPI_FF;

    throw std::invalid_argument("Unknown ExecutionPolicy: " + std::string(str));
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

inline uint32_t PAYLOAD_MAX = 1024;    // Maximum payload size in bytes
inline uint32_t PAYLOAD_MIN = 8;       // Minimum payload size in bytes

/* EXECUTION PARAMETERS */
inline ExecutionPolicy g_policy;       // Execution policy
inline size_t th_workers;              // Number of threads for parallel (shared) processing
inline size_t max_chunk_size;          // Maximum chunk size of records

inline int g_num_processes;
inline long g_record_count;
inline std::string csv_file;

/* MPI PARAMETERS */
inline int cluster_size;               // Number of MPI processes in the cluster
constexpr int WR_TAG = 1;
constexpr int ACK_TAG = 2;
constexpr int COLLECT_TAG = 3;
constexpr int EOS_TAG = 4;
inline double t_start, t_start_emitting, t_end, t_elapsed;

#endif
