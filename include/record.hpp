#if !defined(RECORD_HPP)
#define RECORD_HPP

#include <cstdint>
#include <cstddef>
#include <span>

struct Record {
    uint64_t key;     // 8-byte sorting value
    uint32_t len;     // payload length
    uint8_t padding[4]; // padding to align to 16 bytes
    char payload[];
};

constexpr size_t RECORD_HEADER_SIZE = sizeof(Record);
constexpr size_t PAYLOAD_OFFSET = offsetof(Record, payload);
constexpr size_t RECORD_PAD_SIZE = (RECORD_HEADER_SIZE - PAYLOAD_OFFSET);

// Fixed-size metadata for processing
struct RecordTask {
    uint64_t key;
    uint32_t len;
    size_t foffset;   // File offset to original record

    size_t rec_size() const {
        return RECORD_HEADER_SIZE + len;
    }

    // Get pointer to original record in mapped memory
    Record* get_record(char* mmap_base) const {
        return reinterpret_cast<Record*>(mmap_base + foffset);
    }

    // Get payload view
    std::span<char> get_payload(char* mmap_base) const {
        Record* rec = get_record(mmap_base);
        return std::span<char>(rec->payload, len);
    }
};

constexpr size_t record_task_size = sizeof(RecordTask);

struct WorkRange {
    size_t start_idx;
    size_t end_idx;
    const RecordTask* range_ptr;
    size_t ff_id = -1; // FastFlow worker ID

    std::span<RecordTask> get_task_span() const {
        return std::span<RecordTask>(const_cast<RecordTask*>(range_ptr + start_idx), end_idx - start_idx);
    }

    size_t size() const { return end_idx - start_idx; }
};

#endif // RECORD_HPP
