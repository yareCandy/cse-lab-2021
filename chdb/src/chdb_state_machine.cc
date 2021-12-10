#include "chdb_state_machine.h"

chdb_command::chdb_command() {
    // TODO: Your code here
    cmd_tp = command_type::CMD_NONE;
    key = -1;
    value = -1;
    tx_id = -1;
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id) {
    // TODO: Your code here
    res = std::make_shared<result>();
    res->start = std::chrono::system_clock::now();
    res->key = key;
    res->tx_id = tx_id;
    res->tp = tp;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) {
    // TODO: Your code here
}


void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    std::cout << "serialize size: " << size << std::endl;
    if(size != this->size()) return;

    raft_command::int_to_charA(buf, cmd_tp);
    raft_command::int_to_charA(buf+4, key);
    raft_command::int_to_charA(buf+8, value);
    raft_command::int_to_charA(buf+12, tx_id);

    return;
}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    std::cout << "deserialize size: " << size << std::endl;
    if(size != this->size()) return;
    int tmp = 0;
    raft_command::charA_to_int(buf, tmp);
    raft_command::charA_to_int(buf+4, key);
    raft_command::charA_to_int(buf+8, value);
    raft_command::charA_to_int(buf+12, tx_id);

    cmd_tp = (enum chdb_command::command_type) tmp;

    return;
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    m << (int)cmd.cmd_tp;
    m << cmd.key;
    m << cmd.value;
    m << cmd.tx_id;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int type = 0;
    u >> type;
    u >> cmd.key;
    u >> cmd.value;
    u >> cmd.tx_id;
    cmd.cmd_tp = (enum chdb_command::command_type) type;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command &kv_cmd = dynamic_cast<chdb_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    switch(kv_cmd.cmd_tp) {
        case chdb_command::CMD_NONE: 
            kv_cmd.res->value = kv_cmd.value;
            break;
        case chdb_command::CMD_GET:
            kv_cmd.res->value = commands[kv_cmd.key];
            std::cout << "Get key: " << kv_cmd.key << " res value: " << kv_cmd.res->value << std::endl;
            break;
        case chdb_command::CMD_PUT:
            kv_cmd.res->value = commands[kv_cmd.key];
            commands[kv_cmd.key] = kv_cmd.value;
            std::cout << "Put key: " << kv_cmd.key << " res value: " << kv_cmd.res->value << std::endl;
            break;
        default:
            break;
    }

    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}