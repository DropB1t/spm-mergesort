# Out-of-Core Parallel and Distributed Mergesort

A high-performance parallel and distributed mergesort implementation for sorting large binary files that exceed available main memory. This project explores multiple parallelization strategies including OpenMP, FastFlow, and MPI+FastFlow hybrid models.

[![C++](https://img.shields.io/badge/C++-20-blue.svg)](https://isocpp.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Overview

This project implements an **out-of-core mergesort** algorithm capable of sorting files containing variable-length binary records. The solution leverages:

- **Memory-mapped I/O** for efficient file access
- **Lightweight indexing** to avoid loading full records into memory
- **Multi-threaded parallelism** using OpenMP and FastFlow
- **Distributed computing** via MPI for multi-node execution
- **Hybrid MPI+FastFlow** model combining distributed and shared-memory parallelism

### Key Features

- ✅ Sorts files up to 32GB+ with minimal memory footprint
- ✅ Multiple execution policies: Sequential, OpenMP, FastFlow, MPI+FastFlow
- ✅ K-way merge algorithm with min-heap optimization
- ✅ Asynchronous communication with double buffering (MPI)
- ✅ SIMD vectorization support via C++20 execution policies

## Architecture

### Core Algorithm

The implementation follows a multi-phase approach:

1. **Indexing Phase**: Memory-map the input file and build a lightweight index (`RecordTask`) without loading full payloads
2. **Chunking Phase**: Divide the index into manageable chunks for parallel processing
3. **Parallel Sorting**: Sort chunks in parallel using configurable execution policies
4. **Merging Phase**: Apply k-way merge with min-heap to combine sorted chunks
5. **Output Phase**: Write sorted records to output file (optional)

### Data Structures

```cpp
// On-disk binary format
struct Record {
    uint64_t key;        // 8-byte sorting key
    uint32_t len;        // Payload length
    char payload[];      // Variable-length data
};

// Lightweight in-memory proxy (24 bytes)
struct RecordTask {
    uint64_t key;        // Sorting key
    uint32_t len;        // Payload length
    size_t foffset;      // File offset to original record
};

// Work range for parallel processing
struct WorkRange {
    size_t start_idx;
    size_t end_idx;
    const RecordTask* range_ptr;
    size_t ff_id;        // FastFlow worker ID
};
```

### Memory Efficiency

For a 200GB file with `PAYLOAD_MAX = 1024` bytes:
- Average record size: ~516 bytes
- Estimated record count: ~416M records
- In-memory index size: ~16GB RAM (using `RecordTask` proxies)

## Execution Policies

### 1. Sequential
Basic single-threaded implementation for baseline comparison.

### 2. OpenMP
Data-parallel approach using OpenMP directives:
- Divides work into ranges, one per thread
- Uses `#pragma omp parallel for` with static scheduling
- SIMD vectorization with `std::execution::unseq`
- Parallel divide-and-conquer merge strategy

### 3. FastFlow
Structured parallel pipeline with multiple stages:

```
Emitter → [Worker Farm] → [Merge Network] → Collector
```

- **RecordTaskSplitter**: Distributes work ranges to workers
- **RecordSortWorker**: Farm of parallel sorting workers
- **RecordTaskMerger**: Multi-level binary tree of 2-way mergers
- **RecordCollector**: Routes tasks between sorting and merge layers

### 4. MPI+FastFlow (Distributed)
Hybrid model for multi-node execution:

```
Rank 0 (Emitter) → Ranks 1..P-2 (Workers) → Rank P-1 (Collector)
```

**Key Optimizations:**
- Asynchronous communication (`MPI_Isend`, `MPI_Irecv`)
- Double buffering for overlapping computation and communication
- Each worker runs internal FastFlow pipeline
- Final k-way merge at collector

## Building the Project

### Prerequisites

```bash
# Required dependencies
- C++20 compatible compiler (g++ 10+)
- OpenMPI 4.0+
- OpenMP support
- FastFlow library
- R (optional, for performance plots)
```

### Compilation

```bash
# Set FastFlow path (if not in ~/fastflow)
export FF_ROOT=/path/to/fastflow

# Build all targets
make all

# Build with debug symbols
make DEBUG=1 all

# Build specific targets
make mergesort      # Main sorting program
make record_gen     # Test data generator

# Clean build artifacts
make clean          # Remove binaries
make cleanall       # Remove binaries and test files
```

### Installation

```bash
# Clone the repository
git clone https://github.com/DropB1t/spm-mergesort.git
cd spm-mergesort

# Build the project
make all
```

## Usage

### Generate Test Data

```bash
./record_gen <num_records> [payload_max] [output_file] [seed]

# Examples:
./record_gen 1000000                    # 1M records, default settings
./record_gen 50000000 1024 data.dat     # 50M records, max payload 1KB
./record_gen 10000000 512 test.dat 42   # Custom seed for reproducibility
```

**Parameters:**
- `num_records`: Number of records to generate (required)
- `payload_max`: Maximum payload size in bytes (default: 1024, min: 8)
- `output_file`: Output filename (default: records.dat)
- `seed`: Random seed (default: current timestamp)

### Run Mergesort

```bash
./mergesort <policy> <num_processes> <num_threads> <chunk_size> <record_count> [csv_file]

# Examples:
./mergesort OMP 1 8 1000000 10000000              # OpenMP with 8 threads
./mergesort FastFlow 1 16 5000000 50000000        # FastFlow with 16 workers

# MPI+FastFlow (requires mpirun)
mpirun -n 4 ./mergesort MPI_FF 4 8 1000000 10000000
```

**Parameters:**
- `policy`: Execution policy (Sequential, OMP, FastFlow, MPI_FF)
- `num_processes`: Number of MPI processes (use 1 for non-MPI policies)
- `num_threads`: Number of threads/workers per process
- `chunk_size`: Records per chunk
- `record_count`: Total records in input file
- `csv_file`: Optional CSV file for benchmark results

### Run Benchmarks

```bash
# Run comprehensive benchmark suite
./ms_benchmark.sh

# Custom options
./ms_benchmark.sh --executable ./mergesort --runs 5 --time 01:00:00

# Available options:
--executable PATH    Path to benchmark executable
--time TIME         SLURM time limit (default: 00:30:00)
--runs NUMBER       Number of runs per configuration (default: 3)
--help             Show help message
```

The benchmark suite tests:
- Multiple execution policies
- Various thread/process counts
- Different chunk sizes
- Multiple data sizes (up to 50M records / 25GB)

## Performance Characteristics

### K-way Merge with Min-Heap

The k-way merge optimization provides:
- **Time Complexity**: O(log k) for selecting minimum element
- **Reduced Merge Passes**: Merges k runs simultaneously vs. traditional 2-way merge
- **Cache Efficiency**: Better memory access patterns

### Memory-Mapped I/O

Using `mmap()` with `POSIX_MADV_SEQUENTIAL`:
- Delegates memory management to kernel
- Proactive prefetching of sequential pages
- Aggressive release of processed pages
- Reduces page faults and improves throughput

## Project Structure

```
spm-mergesort/
├── mergesort.cpp              # Main sorting implementation
├── record_gen.cpp             # Test data generator
├── ms_benchmark.sh            # Benchmark suite script
├── plot.r                     # R visualization script
├── Makefile                   # Build configuration
├── README.md                  # This file
└── include/
    ├── defines.hpp            # Global constants and enums
    ├── record.hpp             # Record data structures
    ├── timer.hpp              # Benchmark timing utilities
    └── utils.hpp              # Helper functions
```

## Configuration

Key parameters in `include/defines.hpp`:

```cpp
constexpr size_t PAYLOAD_MIN = 8;        // Minimum payload size
constexpr size_t PAYLOAD_MAX = 1024;     // Maximum payload size
constexpr size_t max_chunk_size = 1000000; // Default chunk size
std::string INPUT_FILE = "records.dat";   // Input filename
std::string OUTPUT_FILE = "sorted.dat";   // Output filename
```

## Benchmarking

Results are saved in structured directories:

```
benchmark_results/run_<timestamp>/    # CSV files with measurements
performance_plots/run_<timestamp>/    # Generated graphs (if R available)
benchmark_logs/run_<timestamp>/       # Individual job logs
```

Each benchmark run generates:
- CSV file with detailed metrics (execution time, speedup, efficiency)
- Performance graphs (if R is installed)
- Summary report with configuration and statistics

## Implementation Highlights

### Asynchronous MPI Communication

```cpp
// Double buffering for overlapping computation and communication
std::vector<char> recv_buf[2];
std::vector<char> send_buf[2];
MPI_Request recv_reqs[2], send_reqs[2];

// Non-blocking receives
MPI_Irecv(recv_buf[0].data(), size, MPI_BYTE, source, tag, comm, &recv_reqs[0]);
MPI_Irecv(recv_buf[1].data(), size, MPI_BYTE, source, tag, comm, &recv_reqs[1]);
```

### FastFlow Pipeline

```cpp
RecordSortingPipeline(work_ranges, num_workers, record_tasks) {
    auto sorting_farm = build_sorting_farm(num_workers);
    pipeline->add_stage(sorting_farm);
    
    for (auto level : merger_levels) {
        auto merger_farm = build_merger_farm(level, num_mergers, record_tasks);
        pipeline->add_stage(merger_farm);
    }
    
    pipeline->add_stage(build_last_merger(final_level, record_tasks));
}
```

### SIMD-Optimized Sorting

```cpp
std::sort(std::execution::unseq, span.begin(), span.end(),
    [](const RecordTask& a, const RecordTask& b) {
        return std::tie(a.key, a.foffset) < std::tie(b.key, b.foffset);
    });
```

## Future Work

Potential optimizations and extensions:

- **Decentralized Emitter**: Use MPI-IO for workers to read input directly
- **Parallel Collector**: Multi-level tree-based merge network with multiple processes
- **GPU Acceleration**: Offload sorting to CUDA/OpenCL
- **Compression**: Add payload compression for reduced I/O
- **Fault Tolerance**: Checkpointing for long-running distributed jobs

## Author

**Yuriy Rymarchuk**  
MSc in Computer Science and Networking - Parallel and Distributed Systems Course  
University of Pisa

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- GitHub repository of [FastFlow](https://github.com/fastflow/fastflow) library
