#include "raft_state_machine.h"
#include <sstream>


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return sizeof(command_type) + key.size() + value.size() + 2;
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    if(size != this->size()) return;
    buf[0] = (cmd_tp >> 24) & 0xff;
    buf[1] = (cmd_tp >> 16) & 0xff;
    buf[2] = (cmd_tp >> 8) & 0xff;
    buf[3] = cmd_tp & 0xff;
    int lenk = key.size(), lenv = value.size();
    int i = 4;
    for(; i < lenk+4; ++i) {
        buf[i] = key[i-4];
    }
    buf[i++] = 0;
    for(; i <lenv+lenk+5; ++i) {
        buf[i] = value[i-lenk-5];
    }
    buf[i] = 0;

    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    int tmp_tp = 0;
    tmp_tp = (buf[0] & 0xff) << 24;
    tmp_tp |= (buf[1] & 0xff) << 16;
    tmp_tp |= (buf[2] & 0xff) << 8;
    tmp_tp |= buf[3] & 0xff;
    cmd_tp = (enum command_type) tmp_tp;

    int i = 4, j = 4;

    while(j < size && buf[j] != 0) ++j;
    key = std::string(buf+i);
    value = std::string(buf+j+1);
    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    m << (int)cmd.cmd_tp;
    m << cmd.key;
    m << cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int tmp = 0;
    u >> tmp;
    u >> cmd.key;
    u >> cmd.value;
    cmd.cmd_tp = (enum kv_command::command_type) tmp;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    switch(kv_cmd.cmd_tp) {
        case kv_command::CMD_NONE: 
            kv_cmd.res->succ = true;
            kv_cmd.res->value = kv_cmd.value;
            break;
        case kv_command::CMD_GET:
            kv_cmd.res->succ = commands.count(kv_cmd.key);
            kv_cmd.res->value = commands[kv_cmd.key];
            std::cout << "Get key: " << kv_cmd.key << " succ: " << kv_cmd.res->succ << " res value: " << kv_cmd.res->value << std::endl;
            break;
        case kv_command::CMD_PUT:
            if(commands.count(kv_cmd.key)) {
                kv_cmd.res->succ = false;
                kv_cmd.res->value = commands[kv_cmd.key];
            } else {
                commands[kv_cmd.key] = kv_cmd.value;
                kv_cmd.res->succ = true;
                kv_cmd.res->value = kv_cmd.value;
            }
            std::cout << "Put key: " << kv_cmd.key << " succ: " << kv_cmd.res->succ << " res value: " << kv_cmd.res->value << std::endl;
            break;
        case kv_command::CMD_DEL:
            kv_cmd.res->succ = commands.count(kv_cmd.key);
            kv_cmd.res->value = commands[kv_cmd.key];
            commands.erase(kv_cmd.key);
            std::cout << "Delete key: " << kv_cmd.key << " succ: " << kv_cmd.res->succ << " res value: " << kv_cmd.res->value << std::endl;
            break;
        default:
            // kv_cmd.res->succ = false;
            break;
    }

    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    std::vector<char> res;
    std::stringstream ss;
    ss << (int)commands.size();
    for(auto &pair: commands) {
        ss << " " << pair.first << " " << pair.second;
    }
    std::string str = ss.str();
    std::cout << "machine: " << str << std::endl;
    res.assign(str.begin(), str.end());
    res.push_back(0);

    return res;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    std::string str;
    std::map<std::string, std::string> tmp;
    str.assign(snapshot.begin(), snapshot.end());
    std::cout << "machine: " << str << std::endl;
    std::stringstream ss(str);
    int size = 0;
    ss >> size;
    for(int i = 0; i < size; ++i) {
        std::string t_k, t_v;
        ss >> t_k;
        ss >> t_v;
        tmp[t_k] = t_v;
    }
    commands.swap(tmp);

    return;    
}