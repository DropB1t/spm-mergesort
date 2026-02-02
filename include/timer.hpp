#include <chrono>
#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <filesystem>

class BenchmarkTimer {
private:
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = Clock::time_point;
    using Duration = std::chrono::nanoseconds;

    struct Measurement {
        std::string execution_policy;
        int num_processes;
        int num_threads;
        int chunk_size;
        long long record_count;
        double completion_time_ms;

        Measurement(const std::string& policy, int processes, int threads,
                   int chunk, long long records, double time_ms)
            : execution_policy(policy), num_processes(processes), num_threads(threads),
              chunk_size(chunk), record_count(records), completion_time_ms(time_ms) {}
    };

    std::vector<Measurement> measurements;
    std::string output_file;
    TimePoint start_time;
    bool is_running{false};

    // Current test parameters
    std::string current_policy;
    int current_processes{1};
    int current_threads{1};
    int current_chunk_size{100000};
    long current_record_count{5000000};

public:
    explicit BenchmarkTimer(const std::string& filename = "ms_test.csv") : output_file(filename) {
        // Create/overwrite CSV file with header if not present or empty
        if (!std::filesystem::exists(output_file) || std::filesystem::is_empty(output_file)) {
            std::ofstream file(output_file, std::ios::trunc);
            if (!file.is_open()) {
                throw std::runtime_error("Cannot open output file: " + output_file);
            }
            file << "execution_policy,num_processes,num_threads,chunk_size,record_count,completion_time_ms\n";
            file.close();
        }
        std::cout << "BenchmarkTimer initialized. Output file: " << output_file << std::endl;
    }

    // Set parameters for the next measurement
    void setTestParameters(const std::string& execution_policy, int num_processes,
                          int num_threads, int chunk_size, long record_count) {
        current_policy = execution_policy;
        current_processes = num_processes;
        current_threads = num_threads;
        current_chunk_size = chunk_size;
        current_record_count = record_count;

        std::cout << "Set parameters: " << execution_policy << " | Processes: " << num_processes
                  << " | Threads: " << num_threads << " | Chunk: " << chunk_size
                  << " | Records: " << record_count << std::endl;
    }

    void start() {
        if (is_running) {
            throw std::runtime_error("Timer is already running");
        }
        start_time = Clock::now();
        is_running = true;
        std::cout << "Timer started for " << current_policy << " execution..." << std::endl;
    }

    void stop() {
        if (!is_running) {
            throw std::runtime_error("Timer is not running");
        }

        auto end_time = Clock::now();
        auto duration = std::chrono::duration_cast<Duration>(end_time - start_time);
        double completion_time_ms = static_cast<double>(duration.count()) / 1'000'000.0;

        measurements.emplace_back(current_policy, current_processes, current_threads,
                                current_chunk_size, current_record_count,
                                completion_time_ms);

        is_running = false;

        /* std::cout << "Timer stopped. Completion time: " << std::fixed << std::setprecision(2)
                  << completion_time_ms << " ms" << std::endl; */

        //writeLastMeasurement();
    }

    void writeLastMeasurement() {
        if (measurements.empty()) return;

        std::ofstream file(output_file, std::ios::app);
        if (!file.is_open()) {
            std::cerr << "Warning: Cannot open output file for writing" << std::endl;
            return;
        }

        const auto& measurement = measurements.back();

        file << measurement.execution_policy << ","
             << measurement.num_processes << ","
             << measurement.num_threads << ","
             << measurement.chunk_size << ","
             << measurement.record_count << ","
             << measurement.completion_time_ms << "\n";

        file.close();
        std::cout << "Measurement written to " << output_file << std::endl;
    }

    void writeAllMeasurements() {
        std::ofstream file(output_file, std::ios::trunc);
        if (!file.is_open()) {
            std::cerr << "Error: Cannot open output file for writing" << std::endl;
            return;
        }

        // Write header
        file << "execution_policy,num_processes,num_threads,chunk_size,record_count,"
             << "completion_time_ms\n";

        for (const auto& measurement : measurements) {
            file << measurement.execution_policy << ","
                 << measurement.num_processes << ","
                 << measurement.num_threads << ","
                 << measurement.chunk_size << ","
                 << measurement.record_count << ","
                 << measurement.completion_time_ms << "\n";
        }

        file.close();
        std::cout << "All " << measurements.size() << " measurements written to " << output_file << std::endl;
    }

    double getLastCompletionTimeMs() const {
        if (measurements.empty()) return 0.0;
        return measurements.back().completion_time_ms;
    }

    void setLastCompletionTimeMs(double time_ms) {
        if (measurements.empty()) return;
        measurements.back().completion_time_ms = time_ms;
        std::cout << "Set new CompletionTime to " << time_ms << "ms\n";
    }

    void printStats() const {
        if (measurements.empty()) {
            std::cout << "No measurements recorded.\n";
            return;
        }

        const auto& last = measurements.back();

        std::cout << "\n=== Last Measurement Summary ===\n"
                  << "Execution Policy: " << last.execution_policy << "\n"
                  << "Processes: " << last.num_processes << "\n"
                  << "Threads: " << last.num_threads << "\n"
                  << "Chunk Size: " << last.chunk_size << "\n"
                  << "Record Count: " << last.record_count << "\n"
                  << "Completion Time: " << last.completion_time_ms << " ms\n"
                  << "================================\n" << std::endl;
    }

    size_t getMeasurementCount() const {
        return measurements.size();
    }

    void clear() {
        measurements.clear();
        std::cout << "All measurements cleared." << std::endl;
    }

    std::string getOutputFile() const {
        return output_file;
    }

    ~BenchmarkTimer() {
        /* if (!measurements.empty()) {
            std::cout << "Timer destructor: Final write of all measurements..." << std::endl;
            writeAllMeasurements();
        } */
    }
};
