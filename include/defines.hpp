#if !defined(DEFINES_HPP)
#define DEFINES_HPP

#include <string>

enum ExecutionPolicy {
    Sequential = 1,
    Parallel,
    FastFlow,
    OMP,
    MPI_FF,
    MPI_OMP
};

/* RECORD PARAMETERS */
constexpr std::string INPUT_FILE = "records.dat";
constexpr std::string OUTPUT_FILE = "sorted.dat";

constexpr size_t N = 1000; // Number of records
constexpr int PAYLOAD_MAX = 1024; // Maximum payload size in bytes
constexpr int PAYLOAD_MIN = 8; // Minimum payload size in bytes

/* EXECUTION PARAMETERS */
constexpr ExecutionPolicy POLICY = ExecutionPolicy::Parallel;
constexpr size_t T = 4; // Number of threads for parallel processing
constexpr size_t MAX_CHUNK_SIZE = 50'000; // Maximum chunk size of records

#endif