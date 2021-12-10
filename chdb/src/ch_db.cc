#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::prepare(unsigned int query_key, unsigned int proc, const chdb_protocol::prepare_var &var, int &r) {
    int nums = shard_num();
    int base_port = this->node->port();
    std::vector<int> res(nums, 0);
    for(int i = 1; i < nums; ++i) {
        int tmp = 0;
        std::cout << "Prepare base port: " << base_port+i << std::endl;
        this->node->template call(base_port + i, proc, var, tmp);
        res[i] = tmp;
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(wait_timeout));
    for(int i = 1; i < nums; ++i) {
        if(res[i] == 0) { r = 0; return 0; }
    }
    r = 1;
    return 0;
}

int view_server::check_prepare(unsigned int query_key, unsigned int proc, const chdb_protocol::check_prepare_state_var &var, int &r) {
    int nums = shard_num();
    int base_port = this->node->port();
    std::vector<int> res(nums, 0);
    for(int i = 1; i < nums; ++i) {
        int tmp = 0;
        this->node->template call(base_port + i, proc, var, tmp);
        res[i] = tmp;
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(wait_timeout));
    for(int i = 1; i < nums; ++i) {
        if(res[i] == 0) { r = 0; return 0; }
    }
    r = 1;
    return 0;
}

int view_server::commit(unsigned int query_key, unsigned int proc, const chdb_protocol::commit_var &var, int &r) {
    int nums = shard_num();
    int base_port = this->node->port();
    std::vector<int> res(nums, 0);
    for(int i = 1; i < nums; ++i) {
        int tmp = 0;
        this->node->template call(base_port + i, proc, var, tmp);
        res[i] = tmp;
    }
    for(int i = 1; i < nums; ++i) {
        while(res[i] == 0) {
            this->node->template call(base_port + i, proc, var, res[i]);
        }
    }
    r = 1;
    return 0;
}

int view_server::rollback(unsigned int query_key, unsigned int proc, const chdb_protocol::rollback_var &var, int &r) {
    int nums = shard_num();
    int base_port = this->node->port();
    std::vector<int> res(nums, 0);
    for(int i = 1; i < nums; ++i) {
        int tmp = 0;
        this->node->template call(base_port + i, proc, var, tmp);
        res[i] = tmp;
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(wait_timeout));
    for(int i = 1; i < nums; ++i) {
        while(res[i] == 0) {
            this->node->template call(base_port + i, proc, var, res[i]);
        }
    }
    r = 1;
    return 0;
}


view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}