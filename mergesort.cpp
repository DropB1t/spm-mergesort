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

#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
using namespace ff;

#include "defines.hpp"
#include "record.hpp"
#include "utils.hpp"

class RecordTaskSplitter : public ff_node_t<WorkRange> {
private:
    std::vector<WorkRange> *work_ranges;

public:
    RecordTaskSplitter(std::vector<WorkRange> *ranges) 
        : work_ranges(ranges) {}

    WorkRange* svc(WorkRange* task) override {
        for (auto& range : *work_ranges) {
            this->ff_send_out(&range);
        }
        return EOS;
    }
};

class RecordSortWorker : public ff::ff_node_t<WorkRange> {
private:
    ssize_t worker_id;

public:
    RecordSortWorker() : worker_id(-1) {}
    
    int svc_init() override {
        worker_id = this->get_my_id();
        return 0;
    }

    WorkRange* svc(WorkRange* task) override {
        // Sort the work range
        auto work_span = task->get_task_span();
        std::sort(std::execution::unseq, work_span.begin(), work_span.end(),
                  [](const RecordTask& a, const RecordTask& b) {
                      return std::tie(a.key, a.foffset) < std::tie(b.key, b.foffset);
                  });

        task->ff_id = worker_id;
        return task;
    }
};

void k_way_merge_ranges(std::span<RecordTask>& span1, std::span<RecordTask>& span2, std::vector<RecordTask> &output) {
    struct RangeIterator {
        const RecordTask* current;
        const RecordTask* end;
        
        bool is_valid() const { return current < end; }
        
        bool operator>(const RangeIterator& other) const {
            if (!is_valid()) return false;
            if (!other.is_valid()) return true;
            return std::tie(current->key, current->foffset) > std::tie(other.current->key, other.current->foffset); // Invert for min-heap
        }
    };

    std::priority_queue<RangeIterator, std::vector<RangeIterator>, std::greater<RangeIterator>> pq;
    pq.emplace(span1.data(), span1.data() + span1.size());
    pq.emplace(span2.data(), span2.data() + span2.size());

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

class RecordTaskMerger : public ff::ff_node_t<WorkRange> {
private:
    int level;
    size_t merger_id;
    WorkRange* last_task = nullptr;
    bool is_last_level;
    std::vector<RecordTask>* record_tasks;
    
public:
    RecordTaskMerger(int l, bool is_last = false, std::vector<RecordTask>* tasks = nullptr) 
        : level(l), is_last_level(is_last), record_tasks(tasks) {}
    
    int svc_init() override {
        merger_id = this->get_my_id();
        return 0;
    }
    
    WorkRange* svc(WorkRange* task) override {
        if (last_task == nullptr) {
            last_task = task;
            return GO_ON; // Wait for pair
        } else {
            // Merge two tasks
            auto merged_task = merge_work_ranges(last_task, task);
            
            // Clean up consumed tasks
            //delete last_task;
            //delete task;
            last_task = nullptr;

            if (is_last_level) {
                last_task = merged_task; // Keep the last merged task for final output
                return GO_ON;
            }
            
            return merged_task; // Pass to next level
        }
    }

    void eosnotify(ssize_t id) override {
        if (!is_last_level && last_task) {
            // Forward unpaired task to next level
            this->ff_send_out(last_task);
        }
    }

private:
    WorkRange* merge_work_ranges(WorkRange* task1, WorkRange* task2) {
        auto span1 = task1->get_task_span();
        auto span2 = task2->get_task_span();

        // Create new storage for merged data
        size_t total_size = span1.size() + span2.size();
        std::vector<RecordTask> tmp(total_size);

        k_way_merge_ranges(span1, span2, tmp);

        /* std::merge(span1.begin(), span1.end(),
                   span2.begin(), span2.end(),
                   tmp.begin(),
                   [](const RecordTask& a, const RecordTask& b) {
                       return std::tie(a.key, a.foffset) < std::tie(b.key, b.foffset);
                   }); */

        auto start_idx = std::min(task1->start_idx, task2->start_idx);
        auto end_idx = std::max(task1->end_idx, task2->end_idx);
        std::copy(tmp.begin(), tmp.end(), record_tasks->data() + start_idx);

        // Create new WorkRange pointing to merged data
        auto merged_range = new WorkRange{
            start_idx,
            end_idx,
            record_tasks,
            merger_id
        };
        
        return merged_range;
    }
};

class RecordCollector : public ff::ff_monode_t<WorkRange> {
private:
    int level;
    int next_merge_size;
    
public:
    RecordCollector(int l) : level(l), next_merge_size(0) {}

    int svc_init() override {
        next_merge_size = this->get_num_outchannels();
        return 0;
    }

    WorkRange* svc(WorkRange* task) override {
        if (!task) return task;
        
        // Route tasks to mergers based on ff_id
        int target_merger = (task->ff_id >= 0) ? (task->ff_id / 2) : 0;
        if (target_merger >= next_merge_size) {
            target_merger = next_merge_size - 1;
        }
        
        this->ff_send_out_to(task, target_merger);
        return GO_ON;
    }
};

class RecordSortingPipeline {
private:
    std::unique_ptr<ff_pipeline> pipeline;
    std::vector<WorkRange> *work_ranges_storage; // Own the work ranges

public:
    RecordSortingPipeline(std::vector<WorkRange> *work_ranges, 
                          int num_workers, 
                          char* mmap_data, 
                          std::vector<RecordTask>* record_tasks)
    {
        pipeline = std::make_unique<ff_pipeline>();
        work_ranges_storage = work_ranges;

        // Stage 1: Sorting farm
        auto sorting_farm = build_sorting_farm(num_workers);
        pipeline->add_stage(sorting_farm);

        // Stage 2: Merging stages (binary tree)
        auto merger_levels = calculate_merger_levels(num_workers);

        for (size_t i = 0; i < merger_levels.size() - 1; i++) {
            int level = i + 1;
            int num_mergers = merger_levels[i];
            auto merger_farm = build_merger_farm(level, num_mergers, record_tasks);
            pipeline->add_stage(merger_farm);
        }
        
        pipeline->add_stage(build_last_merger(merger_levels.back(), record_tasks));
    }
    
    int run_and_wait_end() {
        return pipeline->run_and_wait_end();;
    }
    
private:
    std::vector<int> calculate_merger_levels(int num_workers) {
        std::vector<int> levels;
        int current_level_size = num_workers;
        int level = 1;
        
        while (current_level_size > 1) {
            int next_level_size = (current_level_size + 1) / 2;
            levels.push_back(next_level_size);
            /* std::cout << "[INFO] Level " << level << ": " << next_level_size 
                      << " mergers (reducing from " << current_level_size << ")" << std::endl; */
            current_level_size = next_level_size;
            level++;
        }
        
        return levels;
    }

    ff::ff_farm build_sorting_farm(int num_workers) {
        auto splitter = new RecordTaskSplitter(work_ranges_storage);

        std::vector<ff::ff_node*> workers;
        for (int i = 0; i < num_workers; i++) {
            workers.push_back(new RecordSortWorker());
        }

        auto farm = ff::ff_farm(workers);
        farm.add_emitter(splitter);
        farm.remove_collector();
        return farm;
    }

    ff::ff_farm build_merger_farm(int level, int num_mergers, std::vector<RecordTask>* record_tasks) {
        auto collector = new RecordCollector(level);
        std::vector<ff::ff_node*> mergers;
        
        for (int i = 0; i < num_mergers; i++) {
            mergers.push_back(new RecordTaskMerger(level, false, record_tasks));
        }

        auto farm = ff::ff_farm(mergers);
        farm.add_emitter(collector);
        farm.remove_collector(); // No collector needed
        return farm;
    }

    ff::ff_farm build_last_merger(int level, std::vector<RecordTask>* record_tasks) {
        std::vector<ff::ff_node*> mergers;
        mergers.push_back(new RecordTaskMerger(level, true, record_tasks));
        auto farm = ff::ff_farm(mergers);
        return farm;
    }
};

class RecordProcessor {
private:
    char* mmap_data;
    std::vector<RecordTask> record_tasks;
    size_t num_threads;
    size_t chunk_size;
    ExecutionPolicy policy;

public:

    RecordProcessor(char* _mapped_data, size_t file_size, size_t _num_threads = g_th_workers, size_t _chunk_size = max_chunk_size , ExecutionPolicy _policy = g_policy)
        : mmap_data(_mapped_data), num_threads(_num_threads), chunk_size(_chunk_size), policy(_policy)
    {
        build_record_index(file_size); // Parse file and build RecordTask index
        std::cout << "[INFO] Found " << this->record_count() << " variable-length records\n";
        std::cout << "[INFO] Chunk size set to " << chunk_size << " numbers of records" << std::endl;
        std::cout << "[INFO] Using " << ep_to_string(policy) << " policy" << std::endl;
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
            ranges.push_back({chunk_start, chunk_end, &record_tasks});
            return ranges;
        }
        
        for (size_t i = 0; i < num_threads; ++i) {
            size_t range_start = chunk_start + (i * tasks_per_thread);
            size_t range_end = (i == num_threads - 1) ? 
                chunk_end : chunk_start + ((i + 1) * tasks_per_thread);
            
            ranges.push_back({range_start, range_end, &record_tasks});
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

    // Sequential sort version (sorts whole record_tasks vector)
    template<typename Compare = std::less<uint64_t>>
    void seq_sort(Compare comp = Compare{}) {
        std::stable_sort(record_tasks.begin(), record_tasks.end(), 
                 [comp](const RecordTask& a, const RecordTask& b) {
                     return comp(a.key, b.key);
                 });
    }

    void par_sort(std::vector<WorkRange>& work_ranges) {
        std::vector<std::future<void>> futures;
        for (auto& range : work_ranges) {
            futures.push_back(std::async(std::launch::async, [this, range]() {
                auto wspan = range.get_task_span();
                // Sort the work range using execution policy (vectorized)
                std::sort(std::execution::unseq, wspan.begin(), wspan.end(),
                                [](const RecordTask& a, const RecordTask& b) {
                                    return std::tie(a.key, a.foffset) < std::tie(b.key, b.foffset);
                                });
            }));
        }
        for (auto& future : futures) future.wait();
    }

    void omp_sort(std::vector<WorkRange>& work_ranges) {
#pragma omp parallel for schedule(static) num_threads(num_threads)
        for (auto& range : work_ranges) {
                auto wspan = range.get_task_span();
                std::sort(std::execution::unseq, wspan.begin(), wspan.end(),
                                [](const RecordTask& a, const RecordTask& b) {
                                    return std::tie(a.key, a.foffset) < std::tie(b.key, b.foffset);
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
                case Sequential:
                    seq_sort();
                    return;
                case Parallel:
                    par_sort(work_ranges);
                    break;
                case OMP:
                    omp_sort(work_ranges);
                    break;
                default:
                    throw std::runtime_error("The execution policy is set up incorrectly");
                    break;
            }
            merge_work_ranges_in_chunk(work_ranges, chunk_start, chunk_end);
        }
        switch (policy) {
            case OMP: {
                double start = omp_get_wtime();
                omp_dac_merge_chunks(chunk_boundaries);
                double elapsed = omp_get_wtime() - start;
                std::cout << "[TIMES] dac omp chunk merge times " << elapsed*1000.0 << " ms\n";
                break;
            }
            default: {
                auto start = std::chrono::high_resolution_clock::now();
                k_way_merge_chunks(chunk_boundaries);
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                std::cout << "[TIMES] k way chunk merge times " << duration.count() << " ms\n";
                break;
            }
        }
    }

    // FastFlow-based chunked sort
    void ff_chunked_sort() {
        std::vector<std::pair<size_t, size_t>> chunk_boundaries;
        // Print record task size
        for (size_t chunk_start = 0; chunk_start < record_tasks.size(); chunk_start += chunk_size) {
            size_t chunk_end = std::min(chunk_start + chunk_size, record_tasks.size());
            chunk_boundaries.emplace_back(chunk_start, chunk_end);
            
            auto work_ranges = create_work_ranges(chunk_start, chunk_end);
            RecordSortingPipeline pipeline(&work_ranges, num_threads, mmap_data, &record_tasks);
            
            if (pipeline.run_and_wait_end() < 0) {
                throw std::runtime_error("FastFlow pipeline execution failed!");
            }
        }
        k_way_merge_chunks(chunk_boundaries);
    }

private:

    // Merge sorted work ranges within a single chunk
    inline void merge_work_ranges_in_chunk(std::vector<WorkRange> &work_ranges, size_t chunk_start, size_t  chunk_end) {
        if (work_ranges.size() <= 1) return; // Only one work range
        std::vector<RecordTask> temp(chunk_end - chunk_start);
        k_way_merge_ranges(work_ranges, temp);
        std::copy(temp.begin(), temp.end(), record_tasks.begin() + chunk_start); // Copy back to original vector
    }

    // K-way merge implementation for work ranges within a chunk
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

    // Divide and conquer merge for many chunks
    void omp_dac_merge_chunks(const std::vector<std::pair<size_t, size_t>>& chunk_boundaries) {
        std::vector<std::pair<size_t, size_t>> current_chunks = chunk_boundaries;
        std::vector<RecordTask> temp(record_tasks.size());
        std::vector<RecordTask>* input = &record_tasks;
        std::vector<RecordTask>* output = &temp;
        
        while (current_chunks.size() > 1) {
            std::vector<std::pair<size_t, size_t>> next_chunks;
           
            
            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < current_chunks.size(); i += 2) {
                if (i + 1 < current_chunks.size()) {
                    // Merge two adjacent chunks
                    size_t start1 = current_chunks[i].first;
                    size_t end1 = current_chunks[i].second;
                    size_t start2 = current_chunks[i + 1].first;
                    size_t end2 = current_chunks[i + 1].second;
                    
                    size_t merged_start = start1;
                    size_t merged_size = (end1 - start1) + (end2 - start2);
                    
                    std::merge(input->begin() + start1, input->begin() + end1,
                            input->begin() + start2, input->begin() + end2,
                            output->begin() + merged_start,
                            [](const RecordTask& a, const RecordTask& b) {
                                return std::tie(a.key, a.foffset) < std::tie(b.key, b.foffset);
                            });
                    
                    #pragma omp critical
                    {
                        next_chunks.emplace_back(merged_start, merged_start + merged_size);
                    }
                } else {
                    // Odd chunk, copy to output
                    size_t start = current_chunks[i].first;
                    size_t end = current_chunks[i].second;
                    std::copy(input->begin() + start, input->begin() + end,
                            output->begin() + start);
                    
                    #pragma omp critical
                    {
                        next_chunks.emplace_back(start, end);
                    }
                }
            }
            
            current_chunks = std::move(next_chunks);
            std::swap(input, output);
        }
        
        // Ensure result is in record_tasks
        if (input != &record_tasks) {
            record_tasks = std::move(*input);
        }
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
};

int main() {
    try {
        MMapFile record_file(INPUT_FILE.c_str());

        auto rproc = std::make_unique<RecordProcessor>(record_file.data(),record_file.size());
        
        auto start = std::chrono::high_resolution_clock::now();
        if (g_policy == FastFlow) {
            rproc->ff_chunked_sort();
        } else {
            rproc->shm_chunked_sort();
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "[RESULT] Sorted records in " << duration.count() << " ms\n";
        utils::print_records_to_txt(rproc->get_sorted_tasks(), OUTPUT_FILE);

#if defined(DEBUG)
        // Verify sorting
        if (rproc->verify_sorted()) {
            std::cout << "[DEBUG] Sort verification: PASSED\n";
        } else {
            std::cout << "[DEBUG] Sort verification: FAILED\n";
        }
        std::atomic<size_t> total_payload_bytes{0};
        rproc->process_chunked([&total_payload_bytes](WorkRange range) {
            size_t local_bytes = 0;
            for (const auto& task : range.get_task_span()) {
                local_bytes += task.len;
            }
            total_payload_bytes += local_bytes;
        });
        std::cout << "[DEBUG] Total payload bytes: " << total_payload_bytes << "\n";
#endif
        
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}