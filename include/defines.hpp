#if !defined(DEFINES_HPP)
#define DEFINES_HPP

#include <string>

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

/* RECORD PARAMETERS */
constexpr std::string INPUT_FILE = "records.dat";
constexpr std::string OUTPUT_FILE = "sorted.txt";

size_t g_num_records = 1000; // Number of records
int PAYLOAD_MAX = 1024; // Maximum payload size in bytes
int PAYLOAD_MIN = 8; // Minimum payload size in bytes

/* EXECUTION PARAMETERS */
ExecutionPolicy g_policy = FastFlow;
size_t g_th_workers = 4; // Number of threads for parallel processing

size_t max_chunk_size = 10000; // Maximum chunk size of records

#endif