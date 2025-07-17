#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstddef>
#include <stdexcept>
#include <cstring>
#include <iostream>
#include <chrono>
#include <vector>
#include <span>
#include <algorithm>
#include <execution>
#include <thread>
#include <future>
#include <queue>

#include <mpi.h>
#include <omp.h>

#include "defines.hpp"
#include "record.hpp"
#include "utils.hpp"

class RecordChunker {
private:
    char* mmap_data;
    std::vector<RecordTask> record_tasks;
    size_t num_threads;
    size_t chunk_size;
    ExecutionPolicy policy;

public:
    struct WorkRange {
        size_t start_idx;
        size_t end_idx;
        char* mmap_base;
        const std::vector<RecordTask>* tasks;
        
        std::span<RecordTask> get_task_span() const {
            return std::span<RecordTask>(const_cast<RecordTask*>(tasks->data() + start_idx), end_idx - start_idx);
        }
        
        size_t size() const { return end_idx - start_idx; }
    };

    RecordChunker(char* _mapped_data, size_t file_size, size_t _num_threads = T, size_t _chunk_size = MAX_CHUNK_SIZE , ExecutionPolicy _policy = POLICY)
        : mmap_data(_mapped_data), num_threads(_num_threads), chunk_size(_chunk_size), policy(_policy)
    {
        build_record_index(file_size); // Parse file and build RecordTask index

        if (policy == ExecutionPolicy::OMP || policy == ExecutionPolicy::MPI_OMP) {
            num_threads = omp_get_max_threads();
        }
        
        // Compute dynamic chunk size based on number of threads
        if (num_threads > 1) {
            chunk_size = std::max(record_tasks.size() / num_threads, static_cast<size_t>(1));
        } else {
            chunk_size =std::max(record_tasks.size()/static_cast<size_t>(2), static_cast<size_t>(1)); // Fallback to single-threaded size
        }
        std::cout << "[INFO] Chunk size set to " << chunk_size << " numbers of records" << std::endl;
        std::cout << "[INFO] Using " << num_threads << " threads for processing" << std::endl;
    }

private:
    void build_record_index(size_t file_size) {
        size_t offset = 0;
        
        while (offset + RECORD_HEADER_SIZE <= file_size) {
            Record* rec = reinterpret_cast<Record*>(mmap_data + offset);
            
            // Bounds check
            if (offset + RECORD_HEADER_SIZE + rec->len > file_size) {
                throw std::runtime_error( "[ERROR] Incomplete record at offset " + std::to_string(offset) + "\n");
            }

            // Validate record length
            if (rec->len < PAYLOAD_MIN || rec->len > PAYLOAD_MAX) {
                throw std::runtime_error( "[ERROR] Invalid record length at offset " + std::to_string(offset) + ": " + std::to_string(rec->len) + "\n");
            }

            record_tasks.emplace_back(RecordTask{
                rec->key,
                rec->len,
                offset
            });
            
            offset += RECORD_HEADER_SIZE + rec->len;
        }
    }

public:
    size_t record_count() const { return record_tasks.size(); }
    
    // Create work ranges for a chunk of RecordTasks
    std::vector<WorkRange> create_work_ranges(size_t chunk_start, size_t chunk_end) {
        std::vector<WorkRange> ranges;
        size_t chunk_tasks = chunk_end - chunk_start;
        size_t tasks_per_thread = chunk_tasks / num_threads;
        
        if (tasks_per_thread == 0) {
            ranges.push_back({chunk_start, chunk_end, mmap_data, &record_tasks});
            return ranges;
        }
        
        for (size_t i = 0; i < num_threads; ++i) {
            size_t range_start = chunk_start + (i * tasks_per_thread);
            size_t range_end = (i == num_threads - 1) ? 
                chunk_end : chunk_start + ((i + 1) * tasks_per_thread);
            
            ranges.push_back({range_start, range_end, mmap_data, &record_tasks});
        }
        
        return ranges;
    }

    // Process records chunk by chunk
    template<typename Processor>
    void process_chunked(Processor processor) {
        for (size_t chunk_start = 0; chunk_start < record_tasks.size(); chunk_start += chunk_size) {
            size_t chunk_end = std::min(chunk_start + chunk_size, record_tasks.size());
            
            auto work_ranges = create_work_ranges(chunk_start, chunk_end);
            
            std::vector<std::future<void>> futures;
            for (auto& range : work_ranges) {
                futures.push_back(std::async(std::launch::async, [&processor, range]() {
                    processor(range);
                }));
            }
            
            for (auto& future : futures) {
                future.wait();
            }
        }
    }

    // Sequential sort version
    template<typename Compare = std::less<uint64_t>>
    void seq_sort(Compare comp = Compare{}) {
        std::sort(record_tasks.begin(), record_tasks.end(), 
                 [comp](const RecordTask& a, const RecordTask& b) {
                     return comp(a.key, b.key);
                 });
    }

    template<typename Compare = std::less<uint64_t>>
    void par_sort(std::vector<WorkRange>& work_ranges, Compare comp = Compare{}) {
        std::vector<std::future<void>> futures;
        for (auto& range : work_ranges) {
            futures.push_back(std::async(std::launch::async, [this, comp, range]() {
                auto wspan = range.get_task_span();
                // Sort the work range using stable sort and unsequenced execution policy (vectorized)
                std::stable_sort(std::execution::unseq, wspan.begin(), wspan.end(),
                                [comp](const RecordTask& a, const RecordTask& b) {
                                    return comp(a.key, b.key);
                                });
            }));
        }
        // Wait for all work ranges in this chunk to complete
        for (auto& future : futures) future.wait();
    }

   template<typename Compare = std::less<uint64_t>>
    void omp_sort(std::vector<WorkRange>& work_ranges, Compare comp = Compare{}) {
#pragma omp parallel for schedule(static) num_threads(num_threads)
        for (auto& range : work_ranges) {
                std::printf("Sorting th: %d\n", omp_get_thread_num());
                auto wspan = range.get_task_span();
                // Sort the work range using stable sort and unsequenced execution policy (vectorized)
                std::stable_sort(std::execution::unseq, wspan.begin(), wspan.end(),
                                [comp](const RecordTask& a, const RecordTask& b) {
                                    return comp(a.key, b.key);
                                });
        }
    } 
    
    void shm_chunked_sort() {

        // Track chunk boundaries for proper merging of chunks ( k_way_merge_chunks )
        std::vector<std::pair<size_t, size_t>> chunk_boundaries;
        
        for (size_t chunk_start = 0; chunk_start < record_tasks.size(); chunk_start += chunk_size) {
            size_t chunk_end = std::min(chunk_start + chunk_size, record_tasks.size());
            chunk_boundaries.emplace_back(chunk_start, chunk_end);
            
            auto work_ranges = create_work_ranges(chunk_start, chunk_end);
            
            switch (policy) {
                case ExecutionPolicy::Parallel:
                    par_sort(work_ranges);
                    break;
                case ExecutionPolicy::OMP:
                    omp_sort(work_ranges);
                    break;
                default:
                    throw std::runtime_error("The execution policy is set up incorrectly");
                    break;
            }
            
            merge_work_ranges_in_chunk(chunk_start, chunk_end);
        }
        
        k_way_merge_chunks(chunk_boundaries);
    }

private:

    // Merge sorted work ranges within a single chunk
    void merge_work_ranges_in_chunk(size_t chunk_start, size_t chunk_end) {
        if (chunk_end - chunk_start <= 1) return; // Single element or empty
        
        auto work_ranges = create_work_ranges(chunk_start, chunk_end);
        if (work_ranges.size() <= 1) return; // Only one work range
        
        std::vector<RecordTask> temp(chunk_end - chunk_start);
        
        // Use k-way merge for work ranges within this chunk
        k_way_merge_ranges(work_ranges, temp);
        
        // Copy back to original vector
        std::copy(temp.begin(), temp.end(), record_tasks.begin() + chunk_start);
    }

    // K-way merge implementation for work ranges
    void k_way_merge_ranges(const std::vector<WorkRange>& ranges, std::vector<RecordTask>& output) {
        if (ranges.empty()) return;

        // Iterator for each range
        struct RangeIterator {
            const RecordTask* current;
            const RecordTask* end;
            //size_t range_id;
            
            bool is_valid() const { return current < end; }
            
            bool operator>(const RangeIterator& other) const {
                if (!is_valid()) return false;
                if (!other.is_valid()) return true;
                return std::tie(current->key, current->foffset) > std::tie(other.current->key, other.current->foffset); // Invert for min-heap
            }
        };
        
        std::priority_queue<RangeIterator, std::vector<RangeIterator>, std::greater<RangeIterator>> pq;
        
        for (size_t i = 0; i < ranges.size(); ++i) {
            const auto& range = ranges[i];
            if (range.size() > 0) {
                const RecordTask* start = range.tasks->data() + range.start_idx;
                pq.emplace(start, start + range.size());
            }
        }
        
        size_t output_idx = 0;
        
        while (!pq.empty() && output_idx < output.size()) {
            auto min_iter = pq.top();
            pq.pop();
            
            output[output_idx++] = *min_iter.current;
            ++min_iter.current;
            if (min_iter.is_valid()) {
                pq.emplace(min_iter);
            }
        }
    }

    // K-way merge for chunks
    void k_way_merge_chunks(const std::vector<std::pair<size_t, size_t>>& chunk_boundaries) {
        if (chunk_boundaries.size() <= 1) return;
        
        std::vector<RecordTask> temp(record_tasks.size());
        
        // Iterator for each chunk
        struct ChunkIterator {
            const RecordTask* current;
            const RecordTask* end;
            //size_t chunk_id;

            bool is_valid() const { return current < end; }
            
            bool operator>(const ChunkIterator& other) const {
                if (!is_valid()) return false;
                if (!other.is_valid()) return true;
                return std::tie(current->key, current->foffset) > std::tie(other.current->key, other.current->foffset); // Invert for min-heap
            }
        };
        
        std::priority_queue<ChunkIterator, std::vector<ChunkIterator>, std::greater<ChunkIterator>> pq;
        
        // Initialize iterators for each chunk
        for (size_t i = 0; i < chunk_boundaries.size(); ++i) {
            const auto& [start, end] = chunk_boundaries[i];
            if (end > start) {
                const RecordTask* chunk_start = record_tasks.data() + start;
                const RecordTask* chunk_end = record_tasks.data() + end;
                pq.emplace(chunk_start, chunk_end);
            }
        }
        
        size_t output_idx = 0;
        
        while (!pq.empty() && output_idx < temp.size()) {
            auto min_iter = pq.top();
            pq.pop();
            
            temp[output_idx++] = *min_iter.current;
            
            ++min_iter.current;
            if (min_iter.is_valid()) {
                pq.emplace(min_iter);
            }
        }
        
        record_tasks = std::move(temp);
    }

public:
    // Get sorted record tasks (after sorting)
    const std::vector<RecordTask>& get_sorted_tasks() const {
        return record_tasks;
    }

    // Write sorted records to new file
    void write_sorted_file(const char* output_filename) {
        int fd = open(output_filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd == -1) {
            throw std::runtime_error("Cannot create output file");
        }
        
        for (const auto& task : record_tasks) {
            Record* original = task.get_record(mmap_data);
            
            // Write the entire record (header + payload)
            ssize_t bytes_written = write(fd, original, task.rec_size());
            if (bytes_written != static_cast<ssize_t>(task.rec_size())) {
                close(fd);
                throw std::runtime_error("Write error");
            }
        }
        
        close(fd);
    }

    // Verify that records are sorted (for debugging)
    bool verify_sorted() const {
        for (size_t i = 1; i < record_tasks.size(); ++i) {
            if (record_tasks[i-1].key > record_tasks[i].key) {
                std::cerr << "Sort verification failed at index " << i 
                         << ": " << record_tasks[i-1].key << " > " << record_tasks[i].key << "\n";
                return false;
            }
        }
        return true;
    }
};


class MMapFile {
private:
    int fd;
    char* mapped_data;
    size_t file_size;

public:
    MMapFile(const char* filename) : fd(-1), mapped_data(nullptr) {
        fd = open(filename, O_RDONLY);
        if (fd == -1) {
            throw std::runtime_error("Cannot open file");
        }
        
        struct stat st;
        if (fstat(fd, &st) == -1) {
            close(fd);
            throw std::runtime_error("Cannot stat file");
        }
        
        file_size = st.st_size;
        
        mapped_data = static_cast<char*>(mmap(nullptr, file_size, 
                                            PROT_READ, MAP_SHARED, fd, 0));
        
        if (mapped_data == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("Cannot mmap file");
        }

        // Madvise sequential access for better performance
        if (posix_madvise(mapped_data, file_size, POSIX_MADV_SEQUENTIAL) != 0) {
            munmap(mapped_data, file_size);
            close(fd);
            throw std::runtime_error("Cannot set madvise on mmaped file");
        }
    }
    
    ~MMapFile() {
        if (mapped_data != nullptr) {
            munmap(mapped_data, file_size);
        }
        if (fd != -1) {
            close(fd);
        }
    }
    
    MMapFile(const MMapFile&) = delete;
    MMapFile& operator=(const MMapFile&) = delete;
    
    MMapFile(MMapFile&& other) noexcept 
        : fd(other.fd), mapped_data(other.mapped_data), file_size(other.file_size) {
        other.fd = -1;
        other.mapped_data = nullptr;
    }
    
    char* data() { return mapped_data; }
    size_t size() const { return file_size; }
    
    RecordChunker create_chunker() {
        return RecordChunker(mapped_data, file_size);
    }
};

int main() {
    try {
        MMapFile file(INPUT_FILE.c_str());

        auto chunker = file.create_chunker();
        std::cout << "[INFO] Found " << chunker.record_count() << " variable-length records\n";
        
        auto start = std::chrono::high_resolution_clock::now();
        chunker.shm_chunked_sort();
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        std::cout << "[RESULT] Sorted records in " << duration.count() << " ms\n";

#if defined(DEBUG)
        // Verify sorting
        if (chunker.verify_sorted()) {
            std::cout << "[DEBUG] Sort verification: PASSED\n";
        } else {
            std::cout << "[DEBUG] Sort verification: FAILED\n";
        }
#endif
        
        // Write sorted file
        chunker.write_sorted_file(OUTPUT_FILE.c_str());
        utils::print_records_to_txt(OUTPUT_FILE, "sorted.txt");

        /*
        // Example: Process payloads
        std::atomic<size_t> total_payload_bytes{0};
        chunker.process_chunked([&total_payload_bytes](RecordChunker::WorkRange range) {
            size_t local_bytes = 0;
            for (const auto& task : range.get_task_span()) {
                local_bytes += task.len;
            }
            total_payload_bytes += local_bytes;
        });
        std::cout << "[DEBUG] Total payload bytes: " << total_payload_bytes << "\n";
        */
        
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}