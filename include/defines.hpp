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
constexpr std::string OUTPUT_FILE = "sorted.dat";

constexpr size_t N = 1000; // Number of records
constexpr int PAYLOAD_MAX = 1024; // Maximum payload size in bytes
constexpr int PAYLOAD_MIN = 8; // Minimum payload size in bytes

/* EXECUTION PARAMETERS */
constexpr ExecutionPolicy POLICY = OMP;
constexpr size_t T = 4; // Number of threads for parallel processing
constexpr size_t MAX_CHUNK_SIZE = 500000; // Maximum chunk size of records

#endif