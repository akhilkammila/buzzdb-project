#include <iostream>
#include <algorithm>
#include <fstream>
#include <chrono>
#include <map>
#include <vector>
#include <list>
#include <unordered_map>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <thread>
#include <queue>
#include <optional>
#include <random>
#include <mutex>
#include <shared_mutex>
#include <cassert>
#include <cstring> 
#include <exception>
#include <atomic>
#include <iomanip>

#define UNUSED(p)  ((void)(p))

// Helper function to format microseconds
std::string format_duration(std::chrono::microseconds duration) {
    std::stringstream ss;
    ss << std::fixed << std::setprecision(3) << (duration.count() / 1000.0) << " ms";
    return ss.str();
}

// RAII timer class that prints timing info immediately
class ScopedTimer {
private:
    std::string operation_name;
    std::chrono::high_resolution_clock::time_point start;
    
public:
    ScopedTimer(const std::string& op_name) 
        : operation_name(op_name), 
          start(std::chrono::high_resolution_clock::now()) {
    }
    
    ~ScopedTimer() {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "[TIMING] " << operation_name << ": " << format_duration(duration) << "\n";
    }
};

enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
    FieldType type;
    std::unique_ptr<char[]> data;
    size_t data_length;

public:
    Field(int i) : type(INT) { 
        ScopedTimer timer("Field Constructor (INT)");
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    Field(float f) : type(FLOAT) { 
        ScopedTimer timer("Field Constructor (FLOAT)");
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    Field(const std::string& s) : type(STRING) {
        ScopedTimer timer("Field Constructor (STRING)");
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    Field& operator=(const Field& other) {
        ScopedTimer timer("Field Assignment");
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
        return *this;
    }

    Field(Field&& other) {
        ScopedTimer timer("Field Move Constructor");
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
    }

    FieldType getType() const { return type; }
    
    int asInt() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    
    float asFloat() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    
    std::string asString() const { 
        return std::string(data.get());
    }

    std::string serialize() {
        ScopedTimer timer("Field Serialize");
        std::stringstream buffer;
        buffer << type << ' ' << data_length << ' ';
        if (type == STRING) {
            buffer << data.get() << ' ';
        } else if (type == INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        ScopedTimer timer("Field Serialize to File");
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Field> deserialize(std::istream& in) {
        ScopedTimer timer("Field Deserialize");
        int type; in >> type;
        size_t length; in >> length;
        if (type == STRING) {
            std::string val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == INT) {
            int val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == FLOAT) {
            float val; in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    void print() const {
        switch(getType()){
            case INT: std::cout << asInt(); break;
            case FLOAT: std::cout << asFloat(); break;
            case STRING: std::cout << asString(); break;
        }
    }
};

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        ScopedTimer timer("Tuple AddField");
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        ScopedTimer timer("Tuple GetSize");
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    std::string serialize() {
        ScopedTimer timer("Tuple Serialize");
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        ScopedTimer timer("Tuple Serialize to File");
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        ScopedTimer timer("Tuple Deserialize");
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount; in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            tuple->addField(Field::deserialize(in));
        }
        return tuple;
    }

    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

static constexpr size_t PAGE_SIZE = 4096;
static constexpr size_t MAX_SLOTS = 512;
static constexpr size_t MAX_PAGES = 1000;
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max();

struct Slot {
    bool empty = true;
    uint16_t offset = INVALID_VALUE;
    uint16_t length = INVALID_VALUE;
};

class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage() {
        ScopedTimer timer("SlottedPage Constructor");
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    bool addTuple(std::unique_ptr<Tuple> tuple) {
        ScopedTimer timer("SlottedPage AddTuple");
        
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());        
        size_t slot_itr = 0;
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == true && 
                slot_array[slot_itr].length >= tuple_size) {
                break;
            }
        }
        
        if (slot_itr == MAX_SLOTS) {
            return false;
        }

        slot_array[slot_itr].empty = false;
        size_t offset = INVALID_VALUE;
        
        if (slot_array[slot_itr].offset == INVALID_VALUE) {
            if(slot_itr != 0) {
                auto prev_slot_offset = slot_array[slot_itr - 1].offset;
                auto prev_slot_length = slot_array[slot_itr - 1].length;
                offset = prev_slot_offset + prev_slot_length;
            } else {
                offset = metadata_size;
            }
            slot_array[slot_itr].offset = offset;
        } else {
            offset = slot_array[slot_itr].offset;
        }

        if(offset + tuple_size >= PAGE_SIZE) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            return false;
        }

        assert(offset != INVALID_VALUE);
        assert(offset >= metadata_size);
        assert(offset + tuple_size < PAGE_SIZE);

        if (slot_array[slot_itr].length == INVALID_VALUE) {
            slot_array[slot_itr].length = tuple_size;
        }

        std::memcpy(page_data.get() + offset, serializedTuple.c_str(), tuple_size);
        return true;
    }

    void deleteTuple(size_t index) {
        ScopedTimer timer("SlottedPage DeleteTuple");
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if(slot_itr == index && slot_array[slot_itr].empty == false) {
                slot_array[slot_itr].empty = true;
                break;
            }
        }
    }

    void print() const {
        ScopedTimer timer("SlottedPage Print");
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == false) {
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loadedTuple->print();
            }
        }
        std::cout << "\n";
    }
};

const std::string database_filename = "buzzdb.dat";

class StorageManager {
public:    
    std::fstream fileStream;
    size_t num_pages = 0;
    std::mutex io_mutex;

public:
    StorageManager() {
        ScopedTimer timer("StorageManager Constructor");
        fileStream.open(database_filename, std::ios::in | std::ios::out);
        if (!fileStream) {
            fileStream.clear();
            fileStream.open(database_filename, std::ios::out);
        }
        fileStream.close(); 
        fileStream.open(database_filename, std::ios::in | std::ios::out); 

        fileStream.seekg(0, std::ios::end);
        num_pages = fileStream.tellg() / PAGE_SIZE;
        
        if(num_pages == 0) {
            extend();
        }
    }

    ~StorageManager() {
        if (fileStream.is_open()) {
            fileStream.close();
        }
    }

    std::unique_ptr<SlottedPage> load(uint16_t page_id) {
        ScopedTimer timer("StorageManager Load Page");
        std::lock_guard<std::mutex> io_guard(io_mutex);
        assert(page_id < num_pages);
        
        fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();
        if(fileStream.read(page->page_data.get(), PAGE_SIZE)) {
        } else {
            std::cerr << "Error: Unable to read data from the file :: page id " << page_id << " \n";
            exit(-1);
        }
        return page;
    }

    void flush(uint16_t page_id, const std::unique_ptr<SlottedPage>& page) {
        ScopedTimer timer("StorageManager Flush Page");
        std::lock_guard<std::mutex> io_guard(io_mutex);
        size_t page_offset = page_id * PAGE_SIZE;        
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page->page_data.get(), PAGE_SIZE);        
        fileStream.flush();
    }

    void extend() {
        ScopedTimer timer("StorageManager Extend");
        std::lock_guard<std::mutex> io_guard(io_mutex);
        std::cout << "Extending database file \n";

        auto empty_slotted_page = std::make_unique<SlottedPage>();
        fileStream.seekp(0, std::ios::end);
        fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
        fileStream.flush();
        num_pages += 1;
    }

    void extend(uint64_t till_page_id) {
        ScopedTimer timer("StorageManager Extend To");
        std::lock_guard<std::mutex> io_guard(io_mutex); 
        uint64_t write_size = std::max(static_cast<uint64_t>(0), till_page_id + 1 - num_pages) * PAGE_SIZE;
        if(write_size > 0) {
            std::cout << "Extending database file till page id : " << till_page_id << " \n";
            char* buffer = new char[write_size];
            std::memset(buffer, 0, write_size);

            fileStream.seekp(0, std::ios::end);
            fileStream.write(buffer, write_size);
            fileStream.flush();
            delete[] buffer;
            
            num_pages = till_page_id + 1;
        }
    }
};

using PageID = uint64_t;
using FrameID = uint64_t;
constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferFrame {
private:
    friend class BufferManager;
    
    PageID page_id;
    bool dirty;
    bool exclusive;
    bool valid;
    int users;
    std::shared_mutex shared_lock;

public:
    std::unique_ptr<SlottedPage> page;

    BufferFrame() {
        ScopedTimer timer("BufferFrame Constructor");
        dirty = false;
        exclusive = false;
        valid = false;
        users = 0;
        page_id = 0;
    }
};

class buffer_full_error : public std::exception {
    public:
        const char* what() const noexcept override { 
            return "buffer is full"; 
        }
};

class BufferManager {
private:
    StorageManager storage_manager;
    std::vector<std::unique_ptr<BufferFrame>> buffer_pool;
    
    std::mutex mutex;
    std::vector<PageID> fifo;
    std::vector<PageID> lru;
    std::map<PageID, FrameID> pageMap;
    const uint64_t MAXINT = 0xFFFFFFFFFFFFFFFF;

public:
    uint64_t capacity_;
    uint64_t page_size_;

    BufferManager() {
        ScopedTimer timer("BufferManager Constructor");
        capacity_ = MAX_PAGES_IN_MEMORY;
        for(uint64_t i = 0; i < capacity_; i++) {
            buffer_pool.push_back(std::make_unique<BufferFrame>());
        }
        storage_manager.extend(MAX_PAGES);
    }

    void flushPage(FrameID frame_id) {
        ScopedTimer timer("BufferManager FlushPage");
        PageID p = buffer_pool[frame_id]->page_id;
        storage_manager.flush(p, buffer_pool[frame_id]->page);
    }

    ~BufferManager() {
        ScopedTimer timer("BufferManager Destructor");
        for (uint64_t i = 0; i < capacity_; i++) {
            buffer_pool[i]->shared_lock.lock();
            if (buffer_pool[i]->dirty) flushPage(i);
            buffer_pool[i]->shared_lock.unlock();
        }
    }

    BufferFrame& fix_page(PageID page_id, bool exclusive) {
        ScopedTimer timer("BufferManager FixPage");

        std::lock_guard<std::mutex> l(mutex);

        if (pageMap.find(page_id) != pageMap.end()) {
            FrameID f = pageMap[page_id];
            if (exclusive) buffer_pool[f]->shared_lock.lock();
            else buffer_pool[f]->shared_lock.lock_shared();
            buffer_pool[f]->exclusive = exclusive;
            buffer_pool[f]->users++;
            return *buffer_pool[f];
        }
        
        auto location = std::find(fifo.begin(), fifo.end(), page_id);
        auto location2 = std::find(lru.begin(), lru.end(), page_id);
        if (location != fifo.end()) {
            fifo.erase(location);
            lru.push_back(page_id);
            return *buffer_pool[pageMap[page_id]];
        }
        else if (location2 != lru.end()) {
            lru.erase(location2);
            lru.push_back(page_id);
            return *buffer_pool[pageMap[page_id]];
        }

        if (capacity_ == fifo.size() + lru.size()) {
            PageID evicted = MAXINT;
            for(uint64_t i = 0; i < fifo.size(); i++) {
                if (evicted != MAXINT) continue;
                FrameID f = pageMap[fifo[i]];
                if (buffer_pool[f]->users > 0) continue;
                evicted = fifo[i];
                fifo.erase(fifo.begin()+i);
            }
            for(uint64_t i = 0; i < lru.size(); i++) {
                if (evicted != MAXINT) continue;
                FrameID f = pageMap[lru[i]];
                if (buffer_pool[f]->users > 0) continue;
                evicted = lru[i];
                lru.erase(lru.begin()+i);
            }
            if (evicted == MAXINT) throw buffer_full_error{};
            FrameID f = pageMap[evicted];
            buffer_pool[f]->shared_lock.lock();
            if (buffer_pool[f]->dirty) flushPage(f);
            buffer_pool[f]->valid = false;
            pageMap.erase(evicted);
            buffer_pool[f]->shared_lock.unlock();
        }

        fifo.push_back(page_id);
        for(uint64_t i = 0; i < buffer_pool.size(); i++) {
            if (buffer_pool[i]->valid) continue;

            if (exclusive) buffer_pool[i]->shared_lock.lock();
            else buffer_pool[i]->shared_lock.lock_shared();

            buffer_pool[i]->valid = true;
            buffer_pool[i]->dirty = false;
            buffer_pool[i]->users = 1;
            buffer_pool[i]->page_id = page_id;
            buffer_pool[i]->exclusive = exclusive;
            buffer_pool[i]->page = storage_manager.load(page_id);
            pageMap[page_id] = i;
            return *buffer_pool[i];
        }
        throw buffer_full_error{};
    }

    void unfix_page(BufferFrame& page, bool is_dirty) {
        ScopedTimer timer("BufferManager UnfixPage");
        page.dirty = is_dirty;
        page.users -= 1;
        if (page.exclusive) page.shared_lock.unlock();
        else page.shared_lock.unlock_shared();
    }

    void extend() {
        ScopedTimer timer("BufferManager Extend");
        storage_manager.extend();
    }
    
    size_t getNumPages() {
        ScopedTimer timer("BufferManager GetNumPages");
        return storage_manager.num_pages;
    }

    std::vector<PageID> get_fifo_list() const {
        return fifo;
    }

    std::vector<PageID> get_lru_list() const {
        return lru;
    }
};

class BuzzDB {
public:
    BufferManager buffer_manager;

public:
    BuzzDB() {
        ScopedTimer timer("BuzzDB Constructor");
    }
};

int main(int argc, char* argv[]) {
    bool execute_all = false;
    std::string selected_test = "-1";

    if(argc < 2) {
        execute_all = true;
    } else {
        selected_test = argv[1];
    }

    std::cout << "\n=== BuzzDB Performance Tests ===\n\n";

    if(execute_all || selected_test == "1") {
        std::cout << "\n[TEST 1] Single Page Operations\n";
        {
            ScopedTimer timer("Test 1 - Single Page Operations");
            BuzzDB db;
            std::vector<uint64_t> expected_values(PAGE_SIZE / sizeof(uint64_t), 123);
            {
                auto& page_frame = db.buffer_manager.fix_page(0, true);
                auto& page = page_frame.page;
                std::memcpy(page->page_data.get(), expected_values.data(), PAGE_SIZE);
                db.buffer_manager.unfix_page(page_frame, true);
            }

            {
                std::vector<uint64_t> values(PAGE_SIZE / sizeof(uint64_t));
                auto& page_frame = db.buffer_manager.fix_page(0, false);
                auto& page = page_frame.page;
                std::memcpy(values.data(), page->page_data.get(), PAGE_SIZE);
                db.buffer_manager.unfix_page(page_frame, true);
                assert(expected_values == values);
            }
        }
    }

if(execute_all || selected_test == "2") {
        std::cout << "\n[TEST 2] Persistent Restart Test\n";
        {
            ScopedTimer timer("Test 2 - Persistent Restart");
            auto db = std::make_unique<BuzzDB>();
            
            {
                ScopedTimer timer("Test 2 - Initial Data Writing");
                for (uint16_t segment = 0; segment < 3; ++segment) {
                    for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
                        uint64_t page_id = segment*200 + segment_page;
                        auto& page_frame = db->buffer_manager.fix_page(page_id, true);
                        auto& page = page_frame.page;
                        
                        uint64_t& value = *reinterpret_cast<uint64_t*>(page->page_data.get());
                        value = segment * 200 + segment_page;
                        db->buffer_manager.unfix_page(page_frame, true);
                    }
                }
            }

            {
                ScopedTimer timer("Test 2 - Database Restart and Verification");
                db = std::make_unique<BuzzDB>();
                for (uint16_t segment = 0; segment < 3; ++segment) {
                    for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
                        uint64_t page_id = segment*200 + segment_page;
                        auto& page_frame = db->buffer_manager.fix_page(page_id, false);
                        auto& page = page_frame.page;
                        
                        uint64_t value = *reinterpret_cast<uint64_t*>(page->page_data.get());
                        db->buffer_manager.unfix_page(page_frame, false);
                        assert(segment * 200 + segment_page == value);
                    }
                }
            }
        }
    }
    
    if(execute_all || selected_test == "3") {
        std::cout << "\n[TEST 3] FIFO Eviction Test\n";
        {
            ScopedTimer timer("Test 3 - FIFO Eviction");
            BuzzDB db;
            
            {
                ScopedTimer timer("Test 3 - Initial Page Loading");
                for (uint64_t i = 1; i < 11; ++i) {
                    auto& page = db.buffer_manager.fix_page(i, false);
                    db.buffer_manager.unfix_page(page, false);
                }
            }
            
            std::vector<uint64_t> expected_fifo{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            assert(expected_fifo == db.buffer_manager.get_fifo_list());
            assert(db.buffer_manager.get_lru_list().empty());
            
            {
                ScopedTimer timer("Test 3 - Eviction Trigger");
                auto& page = db.buffer_manager.fix_page(11, false);
                db.buffer_manager.unfix_page(page, false);
            }

            expected_fifo = std::vector<u_int64_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
            assert(expected_fifo == db.buffer_manager.get_fifo_list());
            assert(db.buffer_manager.get_lru_list().empty());
        }
    }
    
    if(execute_all || selected_test == "4") {
        std::cout << "\n[TEST 4] Buffer Full Test\n";
        {
            ScopedTimer timer("Test 4 - Buffer Full");
            BuzzDB db;
            std::vector<BufferFrame*> pages;
            pages.reserve(10);
            
            {
                ScopedTimer timer("Test 4 - Fill Buffer");
                for (uint64_t i = 1; i < 11; ++i) {
                    auto& page = db.buffer_manager.fix_page(i, false);
                    pages.push_back(&page);
                }
            }

            {
                ScopedTimer timer("Test 4 - Overflow Attempt");
                bool exception_thrown = false;
                try {
                    db.buffer_manager.fix_page(11, false);
                } catch (const buffer_full_error& ex) {
                    exception_thrown = true;
                }
                assert(exception_thrown);
            }
            
            {
                ScopedTimer timer("Test 4 - Cleanup");
                for (auto* page : pages) {
                    db.buffer_manager.unfix_page(*page, false);
                }
            }
        }
    }

    std::cout << "\n=== All Tests Completed ===\n";
    return 0;
}