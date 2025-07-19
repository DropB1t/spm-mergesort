#if !defined(UTILS_HPP)
#define UTILS_HPP

#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <string>
#include <format>
#include <vector>
#include <span>
#include <queue>

#include "defines.hpp"
#include "record.hpp"

namespace utils {

// Print a file of records in text format: <key> <len> <payload-hex> ( useful for correctness tests )
inline void print_records_to_txt(const std::string& bin_filename, const std::string& txt_filename) {
    std::ifstream in(bin_filename, std::ios::binary);
    std::ofstream out(txt_filename);
    if (!in) throw std::runtime_error("Cannot open input file: " + bin_filename);
    if (!out) throw std::runtime_error("Cannot open output file: " + txt_filename);

    std::cout << "Converting binary records from " << bin_filename 
              << " to text format representation inside " << txt_filename << std::endl;

    while (true) {
        if (in.peek() == EOF) {
            std::cout << "End of file reached, the conversion is complete." << std::endl;
            break;
        }
        
        Record cur;
        in.read(reinterpret_cast<char*>(&cur), sizeof(Record));
        if (in.gcount() != sizeof(Record)) {
            throw std::runtime_error("Failed to read record header");
        }
        if (cur.len == 0) {
            throw std::runtime_error("Header length is zero, stopping");
        }
        if (cur.len < PAYLOAD_MIN || cur.len > PAYLOAD_MAX) {
            throw std::runtime_error("Invalid record length: " + std::to_string(cur.len));
        }

        out << cur.key << " " << cur.len << "\n";

        in.seekg(cur.len, std::ios::cur); // Skip the payload
    }

    std::cout << "Wrote " << txt_filename << " for inspection." << std::endl;
    in.close();
    out.close();
}

inline void print_records_to_txt(const std::vector<RecordTask>& tasks, const std::string& txt_filename) {
    std::ofstream out(txt_filename);
    if (!out) throw std::runtime_error("Cannot open output file: " + txt_filename);

    for (const auto& task : tasks) {
        out << task.key << " " << task.len << "\n";
    }

    out.close();
    std::cout << "Wrote " << txt_filename << " for inspection." << std::endl;
}

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

} // namespace utils

#endif // UTILS_HPP