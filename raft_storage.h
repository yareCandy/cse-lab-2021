#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>
#include <stdlib.h>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void persist_to_storage(int term, int vote_for, std::vector<log_entry<command>> &log);
    void persist_log_entry(log_entry<command>& entry);
    void persist_snapshot(int last_include_index, int last_include_term, std::vector<char> &snapshot);
    void persist_metadata(int term, int vote_for);
    void recovery_from_storage(int &term, int &vote_for, std::vector<log_entry<command>> &log);
    void recovery_from_snapshot(int &last_include_index, int &last_include_term, std::vector<char> &snapshot);

    static const int BUFFER_SIZE = 10000;
    static const int INT_SIZE = 4;
private:
    std::mutex mtx_meta;
    std::mutex mtx_data;
    std::mutex mtx_snapshot;
    std::mutex mtx_length;
    std::string data_path;
    std::string metadata_path;
    std::string snapshot_path;
    std::string length_path;

    char buf[BUFFER_SIZE];
    char len[INT_SIZE];
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    data_path = dir + "/storage";
    metadata_path = dir + "/medadata";
    snapshot_path = dir + "/snapshot";
    length_path = dir + "/length";
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
}

template<typename command>
void raft_storage<command>::persist_to_storage(int term, int vote_for, std::vector<log_entry<command>> &log) {
    // std::cout << "Persist to storage" << std::endl;
    std::unique_lock<std::mutex> lk(mtx_meta);
    std::ofstream metadata(metadata_path);
    metadata << term << "\n";
    metadata << vote_for << "\n";
    metadata.close();
    lk.unlock();

    std::unique_lock<std::mutex> lk2(mtx_data);
    std::unique_lock<std::mutex> lk3(mtx_length);
    std::ofstream storage(data_path, std::ios::binary);
    std::ofstream length(length_path, std::ios::binary);
    for(const auto &entry: log) {
        int size = entry.cmd.size();
        raft_command::int_to_charA(len, size);
        length.write(len, INT_SIZE);

        raft_command::int_to_charA(buf, entry.term);
        raft_command::int_to_charA(buf+INT_SIZE, entry.index);
        entry.cmd.serialize(buf+INT_SIZE+INT_SIZE, size);
        storage.write(buf, size+INT_SIZE+INT_SIZE);
    }
    storage.close();
    length.close();
    lk2.unlock();
    lk3.unlock();
}

template<typename command>
void raft_storage<command>::persist_log_entry(log_entry<command>& entry) {
    //std::cout << "Persist log entry" << std::endl;

    int size = entry.cmd.size();
    std::unique_lock<std::mutex> lk(mtx_data);
    std::unique_lock<std::mutex> lk2(mtx_length);
    std::ofstream storage(data_path, std::ios::app|std::ios::binary);
    std::ofstream length(length_path, std::ios::app|std::ios::binary);
    raft_command::int_to_charA(len, size);
    length.write(len, INT_SIZE);

    raft_command::int_to_charA(buf, entry.term);
    raft_command::int_to_charA(buf+INT_SIZE, entry.index);
    entry.cmd.serialize(buf+INT_SIZE+INT_SIZE, size);
    storage.write(buf, size+INT_SIZE+INT_SIZE);
    storage.close();
    length.close();
}

template<typename command>
void raft_storage<command>::persist_metadata(int term, int vote_for) {
    std::unique_lock<std::mutex> lk(mtx_meta);
    std::ofstream metadata(metadata_path);
    metadata << term << "\n";
    metadata << vote_for << "\n";
    metadata.close();
}


template<typename command>
void raft_storage<command>::persist_snapshot(int last_include_index, 
        int last_include_term, std::vector<char> &snapshot) {
    std::unique_lock<std::mutex> lk2(mtx_snapshot);
    std::ofstream storage(snapshot_path, std::ios::binary);
    if(!storage) {
        lk2.unlock();
        return;
    }
    raft_command::int_to_charA(buf, last_include_index);
    raft_command::int_to_charA(buf+INT_SIZE, last_include_term);
    storage.write(buf, 2*INT_SIZE);
    storage.write(snapshot.data(), snapshot.size());
    storage.close();
}

template<typename command>
void raft_storage<command>::recovery_from_storage(int &term, int &vote_for, std::vector<log_entry<command>> &log) {
    //std::cout << "Restore from storage" << std::endl;
 
    std::unique_lock<std::mutex> lk(mtx_meta);
    std::ifstream metadata(metadata_path);
    if(!metadata) {
        lk.unlock();
        return;
    }
    metadata >> term;
    metadata >> vote_for;
    metadata.close();
    lk.unlock();

    std::unique_lock<std::mutex> lk2(mtx_meta);
    std::unique_lock<std::mutex> lk3(mtx_length);
    std::ifstream storage(data_path, std::ios::binary);
    std::ifstream length(length_path, std::ios::binary);

    if(!storage || !length) { return; }
    std::vector<log_entry<command>> newLog;
    while(length.read(len, INT_SIZE)) {
        int size = -1;
        raft_command::charA_to_int(len, size);
        // std::cout << "cmd size: " << size << std::endl;
        if(size < 0) break;

        log_entry<command> entry;
        storage.read(buf, size+INT_SIZE+INT_SIZE);
        raft_command::charA_to_int(buf, entry.term);
        raft_command::charA_to_int(buf+INT_SIZE, entry.index);
        entry.cmd.deserialize(buf+INT_SIZE+INT_SIZE, size);
        // std::cout << "\tterm: "<< entry.term << " index: " << entry.index << " size: " << size+INT_SIZE+INT_SIZE <<  " value: " << entry.cmd.value << std::endl;
        newLog.push_back(std::move(entry));
    }
    if(newLog.size()) log.swap(newLog);
    storage.close();
    length.close();
}

template<typename command>
void raft_storage<command>::recovery_from_snapshot(int &last_include_index, 
        int &last_include_term, std::vector<char> &snapshot) {
    std::unique_lock<std::mutex> lk2(mtx_snapshot);
    std::ifstream storage(snapshot_path, std::ios::binary);
    if(!storage) {
        lk2.unlock();
        return;
    }
    storage.read(buf, BUFFER_SIZE);
    raft_command::charA_to_int(buf, last_include_index);
    raft_command::charA_to_int(buf+INT_SIZE, last_include_term);
    std::vector<char> tmp;
    for(int i = 2*INT_SIZE; i < BUFFER_SIZE; ++i) {
        tmp.push_back(buf[i]);
        if(buf[i] == 0) break;
    }
    snapshot.swap(tmp);
    storage.close();
}

#endif // raft_storage_h