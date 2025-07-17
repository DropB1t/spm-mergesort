#include <iostream>
#include <fstream>
#include <random>
#include <vector>
#include <string>
#include <chrono>
#include <span>
#include <format>
#include <ranges>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>

#include "defines.hpp"
#include "record.hpp"
#include "utils.hpp"

// Configuration structure
struct Config {
    size_t record_count = N;
    std::uint32_t payload_max = PAYLOAD_MAX;
    std::filesystem::path output_file = INPUT_FILE;
    std::uint64_t seed = std::chrono::steady_clock::now().time_since_epoch().count();
    
    // Buffer size for efficient large file I/O (32MB buffer)
    static constexpr size_t buffer_size = 32 * 1024 * 1024;
};

// Command line parsing functions
namespace cli {
    auto parse_config(std::span<char*> args) -> std::optional<Config> {
        if (args.size() < 2) {
            return std::nullopt;
        }
        
        Config config{};
        
        // Parse required N parameter
        try {
            config.record_count = std::stoull(args[1]);
            if (config.record_count == 0) {
                std::cerr << "Error: N must be a positive integer" << std::endl;
                return std::nullopt;
            }
        } catch (const std::exception&) {
            std::cerr << "Error: Invalid number format for N" << std::endl;
            return std::nullopt;
        }
        
        // Parse optional PAYLOAD_MAX
        if (args.size() >= 3) {
            try {
                config.payload_max = std::stoul(args[2]);
                if (config.payload_max < 8) {
                    std::cerr << "Error: PAYLOAD_MAX must be at least 8" << std::endl;
                    return std::nullopt;
                }
            } catch (const std::exception&) {
                std::cerr << "Error: Invalid number format for PAYLOAD_MAX" << std::endl;
                return std::nullopt;
            }
        }
        
        // Parse optional filenames
        if (args.size() >= 4) {
            config.output_file = args[3];
        }

        // Parse optional seed
        if (args.size() >= 5) {
            try {
                config.seed = std::stoull(args[4]);
            } catch (const std::exception&) {
                std::cerr << "Error: Invalid number format for seed" << std::endl;
                return std::nullopt;
            }
        }
        
        return config;
    }

    auto make_reporter(size_t total) -> std::function<void(size_t)> {
        auto milestone = std::max<size_t>(1, total / 100);
        return [total, milestone](size_t current) {
            if (total > 1000 && (current + 1) % milestone == 0) {
                double percentage = 100.0 * (current + 1) / total;
                std::cout << std::format("Progress: {}/{} records ({:.1f}%)", current + 1, total, percentage) << std::endl;
            }
        };
    }
    
    void print_usage(std::string_view program_name) {
        std::cout << std::format("Usage: {} <N> [PAYLOAD_MAX] [output_file]", program_name) << std::endl;
        std::cout << "  N            - Number of records to generate" << std::endl;
        std::cout << "  PAYLOAD_MAX  - Maximum payload size (default: 1024, min: 8)" << std::endl;
        std::cout << "  output_file  - Output filename (default: records.dat)" << std::endl;
        std::cout << "  seed         - Random seed (default: current time)" << std::endl;
        std::cout << std::endl;
        std::cout << "  Optimized for large files up to 32GB with buffered I/O." << std::endl;
    }
}
// Functional random number generators
namespace rng {
    using Generator = std::mt19937_64;
    using KeyDist = std::uniform_int_distribution<std::uint64_t>;
    using LenDist = std::uniform_int_distribution<std::uint32_t>;
    using ByteDist = std::uniform_int_distribution<std::uint8_t>;
    
    auto make_generator(std::uint64_t seed) -> Generator {
        return Generator{seed};
    }
    
    auto make_key_generator(Generator& gen) -> std::function<std::uint64_t()> {
        static KeyDist dist{0, 10};
        return [&gen]() { return dist(gen); };
    }
    
    auto make_length_generator(Generator& gen, std::uint32_t min_len, std::uint32_t max_len) 
        -> std::function<std::uint32_t()> {
        auto dist = LenDist{min_len, max_len};
        return [&gen, dist]() mutable { return dist(gen); };
    }
    
    auto make_byte_generator(Generator& gen) -> std::function<std::uint8_t()> {
        static ByteDist dist{0, 255};
        return [&gen]() { return dist(gen); };
    }

    auto generate_payload(std::uint32_t length, 
                         const std::function<std::uint8_t()>& byte_gen) -> std::vector<std::uint8_t> {
        std::vector<std::uint8_t> payload;
        payload.reserve(length);
        
        std::ranges::generate_n(std::back_inserter(payload), length, byte_gen);
        return payload;
    }
}

// Efficient buffered file writer for large files
namespace file_io {
    struct BufferedWriter {
        std::ofstream file;
        std::vector<std::uint8_t> buffer;
        size_t buffer_pos = 0;
        size_t total_written = 0;
        
        explicit BufferedWriter(const std::filesystem::path& path, size_t buffer_size) 
            : file(path, std::ios::binary | std::ios::out), buffer(buffer_size) {
            if (!file) {
                throw std::runtime_error(std::format("Cannot open file: {}", path.string()));
            }
        }
        
        ~BufferedWriter() {
            flush();
        }
        
        void flush() {
            if (buffer_pos > 0) {
                file.write(reinterpret_cast<const char*>(buffer.data()), buffer_pos);
                if (!file) {
                    throw std::runtime_error("Write operation failed during flush");
                }
                total_written += buffer_pos;
                buffer_pos = 0;
            }
        }
        
        void write_bytes(std::span<const std::uint8_t> data) {
            size_t remaining = data.size();
            size_t offset = 0;
            
            while (remaining > 0) {
                size_t available = buffer.size() - buffer_pos;
                size_t to_copy = std::min(remaining, available);
                
                std::ranges::copy_n(data.begin() + offset, to_copy, 
                                  buffer.begin() + buffer_pos);
                
                buffer_pos += to_copy;
                offset += to_copy;
                remaining -= to_copy;
                
                if (buffer_pos == buffer.size()) {
                    flush();
                }
            }
        }
        
        void write_pod(const auto& pod_data) {
            auto bytes = std::span{reinterpret_cast<const std::uint8_t*>(&pod_data), sizeof(pod_data)};
            write_bytes(bytes);
        }
        
        size_t bytes_written() const {
            return total_written + buffer_pos;
        }
    };
    
    auto create_writer(const std::filesystem::path& path, size_t buffer_size) 
        -> std::unique_ptr<BufferedWriter> {
        return std::make_unique<BufferedWriter>(path, buffer_size);
    }
}

// Generation pipeline using functional composition
namespace pipeline {
    
    void generate_records(const Config& config) {
        std::cout << std::format("Generating {} records with payload sizes 8-{} bytes", 
                    config.record_count, config.payload_max) << std::endl;
        std::cout << std::format("Output file: {}", config.output_file.string()) << std::endl;
 
        // Create functional generators
        auto gen = rng::make_generator(config.seed);
        auto key_gen = rng::make_key_generator(gen);
        auto len_gen = rng::make_length_generator(gen, PAYLOAD_MIN, config.payload_max);
        auto byte_gen = rng::make_byte_generator(gen);
        
        // Create buffered writer for efficient large file I/O
        auto writer = file_io::create_writer(config.output_file, Config::buffer_size);
        auto report_progress = cli::make_reporter(config.record_count);
        
        for (size_t index = 0; index < config.record_count; ++index) {
            // Generate record
            auto len = len_gen();
            Record *rec = (Record*)malloc(sizeof(Record) + len);
            
            rec->len = len;
            rec->key = key_gen();
            auto payload_vec = rng::generate_payload(rec->len, byte_gen);
            std::memcpy(rec->payload, payload_vec.data(), rec->len);

#if defined(DEBUG)
            // Print debug info
            std::string hex_payload;
            hex_payload.reserve(rec->len * 2);
            for (auto byte : payload_vec) {
                hex_payload += std::format("{:02x}", byte);
            }
            std::cout << std::format("Generating record {}: key={}, len={}\n payload: {}",
                        index + 1, rec->key, rec->len, hex_payload) << std::endl;
#endif

            writer->write_bytes(std::span<const std::uint8_t>(
                reinterpret_cast<const std::uint8_t*>(rec), RECORD_HEADER_SIZE + rec->len));
            report_progress(index); // Report progress
            free(rec); // Free the record memory
        }

        auto final_size = writer->bytes_written();
        std::cout << std::format("Successfully generated {} records", config.record_count) << std::endl;
        std::cout << std::format("Actual file size: {} bytes ({:.2f} GB)", 
                    final_size, final_size / (1024.0 * 1024.0 * 1024.0)) << std::endl;
    }
}

int main(int argc, char* argv[]) {
    try {
        auto config = cli::parse_config(std::span{argv, static_cast<size_t>(argc)});
        
        if (!config) {
            cli::print_usage(argv[0]);
            return 1;
        }
        
        pipeline::generate_records(*config);
        utils::print_records_to_txt(config->output_file.string(), "generated.txt");

    } catch (const std::exception& e) {
        std::cerr << std::format("Error: {}", e.what()) << std::endl;
        return 1;
    }
    return 0;
}