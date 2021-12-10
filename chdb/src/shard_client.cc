#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    r = 0;
    std::cout << "Put shard " << shard_id << " tx_id: " << var.tx_id << " key: " << var.key << " value: " << var.value << std::endl;
    // get primary data
    auto& data = get_store();
    // get old value
    int old_v = 0;
    if(data.count(var.key)) {
        old_v = data[var.key].value;
    }
    // write to redo log
    chdb_log_entry entry(chdb_protocol::Put, var.tx_id, var.key, var.value, old_v);
    redo_log[var.tx_id].emplace_back(std::move(entry));

    r = 1; // succ
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    r = 0;
    auto& data = get_store(); // primary
    std::cout << "Get shard " << shard_id << " tx_id: " << var.tx_id << std::endl;
    for(auto &it: data) {
        std::cout << "\tkey: " << it.first << " value: " << it.second.value << std::endl; 
    }

    // return the newest value(maybe in redo_log)
    if(data.count(var.key)) {
        r = data[var.key].value;
    }

    if(redo_log.count(var.tx_id)) {
        auto& log = redo_log[var.tx_id];
        // lookup from back to front
        for(int i = (int)log.size()-1; i >= 0; --i) {
            auto& entry = log[i];
            if(entry.operation == chdb_protocol::Put && entry.key == var.key) {
                r = entry.new_v;
                break;
            }
        }
    }
    std::cout << "Get shard " << shard_id << " tx_id: " << var.tx_id << " key: " << var.key << " value: " << r << std::endl;
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    if(!active && redo_log.count(var.tx_id)) {
        r = 0;
        return 0;
    }
    r = 1;
    auto& data = get_store(); // primary
    // update the data in redo_log to store
    if(redo_log.count(var.tx_id)) {
        auto &log = redo_log[var.tx_id];
        for(auto &entry: log) {
            value_entry value;
            value.value = entry.new_v;
            for(int j = 0; j < replica_num; ++j) {
                store[j][entry.key] = value;
            }
        }
        redo_log.erase(var.tx_id);
        r = 1;
        
        std::cout << "Commit shard " << shard_id << " tx_id: " << var.tx_id << std::endl;
        for(auto &it: data) {
            std::cout << "\tkey: " << it.first << " value: " << it.second.value << std::endl; 
        }
    }
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    r = 0;
    std::cout << "Rollback shard " << shard_id << " tx_id: " << var.tx_id << std::endl;
    redo_log.erase(var.tx_id);
    r = 1;
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    r = 0; // prepare_not_ok;
    // if active or (not active but tx is readonly) ==> prepare_ok
    if(active || redo_log.count(var.tx_id) == 0) {
        r = 1;  // prepare_ok
    }
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    r = 0; // prepare_not_ok;
    // if active or (not active but tx is readonly) ==> prepare_ok
    if(active || redo_log.count(var.tx_id) == 0) {
        std::cout << "Prepare shard " << shard_id << " tx_id: " << var.tx_id << std::endl;
        r = 1;  // prepare_ok
    }
    return 0;
}

shard_client::~shard_client() {
    delete node;
}