#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <random>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:

    // Persistent state on all servers
	int vote_for;
    int got_votes;
	std::vector<log_entry<command>> log;

    // Volatile state on all servers.
	int commited_index;
	int applied_index;

    // Volatile state on leaders.
	std::vector<int> nextIndex;
	std::vector<int> matchIndex;

    // clock at last rpc
    std::chrono::time_point<std::chrono::system_clock> last_received_RPC_time;
    int heartbeat_timeout;
    int follower_election_timeout;
    int candidate_election_timeout;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    int get_last_log_term() {
	    return log.back().term;
    }

    int get_last_log_index() {
        return log.back().index;
    }

    void read_from_snapshot() {
        // std::unique_lock<std::mutex> lk(mtx);
        int last_include_index = 0, last_include_term = 0;
        std::vector<char> snapshot;
        storage->recovery_from_snapshot(last_include_index, last_include_term, snapshot);
        if(snapshot.size()) {
            commited_index = last_include_index;
            applied_index = last_include_index;
            state->apply_snapshot(snapshot);
            update_log(last_include_index, last_include_term);
        }
    }

    void update_log(int last_include_index, int last_include_term) {
        std::vector<log_entry<command>> newLog(1);
        // cmd 怎么说
        newLog[0].index = last_include_index;
        newLog[0].term = last_include_term;

        for(int i = log.size()-1; i >= 0; --i) {
            if(log[i].index == last_include_index && log[i].term == last_include_term) {
                newLog.insert(newLog.end(), log.begin()+i+1, log.end());
                break;
            }
        }
        log.swap(newLog);
    }

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    vote_for = -1;
    got_votes = 0;
    heartbeat_timeout = 100 + random()%20;
    follower_election_timeout = 300 + random()%200;
    candidate_election_timeout = 1000;

    // initializethe last_received_RPC_time
    auto value = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch();
    last_received_RPC_time = std::chrono::time_point<std::chrono::system_clock>(std::chrono::milliseconds(value.count()-250));

    // initialize the index
    commited_index = 0;
    applied_index = 0;
    
    // initialize log
    log.resize(1);
    log[0].term = 0;
    log[0].index = 0;

}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    
    RAFT_LOG("start");

	// initialize from state persisted before a crash
    storage->recovery_from_storage(current_term, vote_for, log);
    storage->persist_to_storage(current_term, vote_for, log);
    RAFT_LOG("start_recovry log:");
    for(const auto &entry: log) {
        RAFT_LOG("\tentry %d term: %d", entry.index, entry.term);
    }
    if(log.size() > 1) {
        last_received_RPC_time = std::chrono::system_clock::now();
    }

	read_from_snapshot();
    // save_snapshot();

    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    std::unique_lock<std::mutex> lk(mtx);
    if(role != leader) return false;

    log_entry<command> entry;
    entry.index = get_last_log_index() + 1;
    entry.term = current_term;
    entry.cmd = cmd;
    RAFT_LOG("new command storage log %d term: %d cmd value: %d", entry.index, entry.term);
    log.emplace_back(std::move(entry));
    storage->persist_to_storage(current_term, vote_for, log);

    term = current_term;
    index = get_last_log_index();
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    std::unique_lock<std::mutex> lk(mtx);
    int base_index = log[0].index;
    int last_index = get_last_log_index();
    if(applied_index < base_index || applied_index > last_index) 
        return false;
    
    RAFT_LOG("save snapshot applied_index: %d term: %d", applied_index, log[applied_index-base_index].term);
    update_log(applied_index, log[applied_index-base_index].term);
    int last_include_index = log[0].index;
    int last_include_term = log[0].term;
    std::vector<char> snapshot = state->snapshot();
    storage->persist_snapshot(last_include_index, last_include_term, snapshot);
    RAFT_LOG("save snapshot log[0].index: %d log[0].term: %d", last_include_index, last_include_term);
    storage->persist_to_storage(current_term, vote_for, log);

    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args arg, request_vote_reply& reply) {
    // Your code here:
    RAFT_LOG("Received vote request from %d term %d lastLogIndex %d lastLogTerm %d", arg.candidateId, arg.term, arg.lastLogIndex, arg.lastLogTerm);
    std::unique_lock<std::mutex> lk(mtx);
    int base_index = log[0].index;
    int last_log_index = get_last_log_index();
    int term = log[last_log_index-base_index].term;

    if(arg.term < current_term) {
		reply.term = current_term;
		reply.voteGranted = false;
		return 0;
	}

	if(arg.term > current_term) {
		role = follower;
		current_term = arg.term;
		vote_for = -1;
        storage->persist_metadata(current_term, vote_for);
	}

	reply.term = current_term;
    reply.voteGranted = false;
    int index = get_last_log_index();
	if((vote_for == -1 || vote_for == arg.candidateId) && 
            (arg.lastLogTerm > term || (arg.lastLogTerm == term && arg.lastLogIndex >= index))) {
		vote_for = arg.candidateId;
		reply.voteGranted = true;
        storage->persist_metadata(current_term, vote_for);
        last_received_RPC_time = std::chrono::system_clock::now();
	}
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lk(mtx);
    if(reply.voteGranted) {
        ++ got_votes;
        RAFT_LOG("got a vote from %d", target);
        if(got_votes > num_nodes()/2) {
            role = leader;
            std::vector<int> tmp1(num_nodes(), commited_index+1);
            nextIndex.swap(tmp1);
            std::vector<int> tmp2(num_nodes(), 0);
            matchIndex.swap(tmp2);
            RAFT_LOG("commited_index: %d applied_index: %d", commited_index, applied_index);
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    RAFT_LOG("Received heartbeat from %d in term %d and log index is %d log term is %d log size %d", arg.leaderId, arg.term, arg.prevLogIndex, arg.prevLogTerm, (int)arg.entries.size());
    std::unique_lock<std::mutex> lk(mtx);

    reply.success = false;
	if(arg.term < current_term) { 
        reply.term = current_term; 
        reply.nextTry = get_last_log_index() + 1;
        return 0;
    }

	if(arg.term > current_term) {
		role = follower;
		current_term = arg.term;
		vote_for = -1;
        storage->persist_metadata(current_term, vote_for);
	}

	// update timer
	last_received_RPC_time = std::chrono::system_clock::now();
	reply.term = current_term;
	if(arg.prevLogIndex > get_last_log_index()) { reply.nextTry = get_last_log_index()+1; return 0; }

	int baseIndex = log[0].index;
    // RAFT_LOG("base index %d last log index %d log log term %d", baseIndex, get_last_log_index(), get_last_log_term());
	if(arg.prevLogIndex >= baseIndex && arg.prevLogTerm != log[arg.prevLogIndex-baseIndex].term) {
		int term = log[arg.prevLogIndex-baseIndex].term;
		for(int j = arg.prevLogIndex; j >= baseIndex; --j) {
			if(log[j-baseIndex].term != term) { 
                reply.nextTry = j+1; 
                break; 
            }
		}
        RAFT_LOG("term %d failed next try %d", term, reply.nextTry);
	} else if(arg.prevLogIndex >= baseIndex-1) {
        log.resize(arg.prevLogIndex-baseIndex+1);
        log.insert(log.end(), arg.entries.begin(), arg.entries.end());
        storage->persist_to_storage(current_term, vote_for, log);
		reply.success = true;
		reply.nextTry = arg.prevLogIndex + arg.entries.size();
		if(commited_index < arg.leaderCommit) {
			commited_index = std::min(arg.leaderCommit, get_last_log_index());
		}
	}
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lk(mtx);

    if(reply.term > current_term) {
		current_term = reply.term;
		role = follower;
		vote_for = -1;
		storage->persist_metadata(current_term, vote_for);
        last_received_RPC_time = std::chrono::system_clock::now();
        return;
	}

    if(reply.success) {
		if(arg.entries.size() > 0) {
			nextIndex[target] = arg.entries.back().index + 1;
			matchIndex[target] = nextIndex[target] - 1;
		}
	} else {
		nextIndex[target] = std::min(reply.nextTry, get_last_log_index());
	}
    RAFT_LOG("Received reply from %d result %d and nextIndex is %d", target, reply.success, nextIndex[target]);

    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> lk(mtx);
    
    if(args.term < current_term) {
		reply.term = current_term;
		return 0;
	}

	if(args.term > current_term) {
		role = follower;
		current_term = args.term;
		vote_for = -1;
		storage->persist_metadata(current_term, vote_for);
	}

    last_received_RPC_time = std::chrono::system_clock::now();
	reply.term = current_term;

	if(args.lastIncludedIndex > commited_index) {
		update_log(args.lastIncludedIndex, args.lastIncludedTerm);
		applied_index = args.lastIncludedIndex;
		commited_index = args.lastIncludedIndex;

        // 应用到状态机
        state->apply_snapshot(args.snapshot);
		storage->persist_snapshot(args.lastIncludedIndex, args.lastIncludedTerm, args.snapshot);
	}
    RAFT_LOG("commited_index: %d applied_index: %d", commited_index, applied_index);

    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& args, const install_snapshot_reply& reply) {
    // Your code here:

    std::unique_lock<std::mutex> lk(mtx);
    if(args.term != current_term) { return; }

	if(reply.term > current_term) {
		current_term = reply.term;
		role = follower;
		vote_for = -1;
        storage->persist_metadata(current_term, vote_for);
		return;
	}

	nextIndex[target] = args.lastIncludedIndex + 1;
	matchIndex[target] = args.lastIncludedIndex;
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        std::unique_lock<std::mutex> lk(mtx);
        auto current_time = std::chrono::system_clock::now();
        auto du = std::chrono::duration_cast<std::chrono::milliseconds>(
                current_time-last_received_RPC_time);
        if(role != leader && du.count() > follower_election_timeout) {
            RAFT_LOG("start election");
            role = candidate;
            vote_for = my_id;
            ++ current_term;
            got_votes = 1;
            storage->persist_metadata(current_term, vote_for);
            last_received_RPC_time = std::chrono::system_clock::now();
            // send request vote
            request_vote_args arg;
            arg.term = current_term;
            arg.candidateId = my_id;
            arg.lastLogIndex = get_last_log_index();
            arg.lastLogTerm = get_last_log_term();
            lk.unlock();
            for(int i = 0; i < num_nodes(); ++i) {
                if(i == my_id) continue;
                thread_pool->addObjJob(this, &raft::send_request_vote, i, arg);
            }
            while(true) {
                if (is_stopped()) return;
                lk.lock();
                if(role != candidate) { lk.unlock(); break; }
                auto cur = std::chrono::system_clock::now();
                auto du = std::chrono::duration_cast<std::chrono::milliseconds>(cur-last_received_RPC_time);
                if(du.count() > candidate_election_timeout) { lk.unlock(); break; }
                lk.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if(role == leader) {
                RAFT_LOG("election success");
            } else {
                RAFT_LOG("election failed");
            }
        } else {
            lk.unlock();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        std::unique_lock<std::mutex> lk(mtx);
        if(role == leader) {
            int baseIndex = log[0].index;
            for(int j = get_last_log_index(); j > commited_index; --j) {
                int count = 1;
                for(int k = 0; k < num_nodes(); ++k) {
                    if(k != my_id && matchIndex[k] >= j) ++count;
                }
                if(count > num_nodes()/2) { 
                    commited_index = j; 
                    // send heart beat to follower
                    // append_entries_args<command> arg;
                    // arg.term = current_term;
                    // arg.leaderId = my_id;
                    // arg.leaderCommit = commited_index;
                    // for(int i = 0; i < num_nodes(); ++i) {
                    //     if(i == my_id) continue;
                    //     arg.prevLogIndex = nextIndex[i] - 1;
                    //     if(arg.prevLogIndex >= baseIndex) {
                    //         arg.prevLogTerm = log[arg.prevLogIndex-baseIndex].term;
                    //     }
                    //     if(nextIndex[i] <= get_last_log_index()) {
                    //         RAFT_LOG("make follower update commit index log.size: %d nextIndex[%d]: %d baseIndex: %d\n", (int)log.size(), i, nextIndex[i], baseIndex);
                    //         std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    //         arg.entries.insert(arg.entries.begin(), log.begin()+(nextIndex[i]-baseIndex), log.end());
                    //     }
                    //     thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                    //     std::vector<log_entry<command>> tmp;
                    //     arg.entries.swap(tmp);
                    // }
                    break; 
                }
            }
        }
        lk.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        std::unique_lock<std::mutex> lk(mtx);
        int baseIndex = log[0].index;
        if(commited_index > applied_index) {
            RAFT_LOG("Apply log index %d to state machine", applied_index+1);
            state->apply_log(log[applied_index-baseIndex+1].cmd);
            ++applied_index;
            storage->persist_metadata(current_term, vote_for);
        }
        lk.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        std::unique_lock<std::mutex> lk(mtx);
        if(role == leader) {
            int baseIndex = log[0].index;
            for(int i = 0; i < num_nodes(); ++i) {
                if(i == my_id) continue;
                if(nextIndex[i] > baseIndex) {  // send log
                    append_entries_args<command> arg;
                    arg.term = current_term;
                    arg.leaderId = my_id;
                    arg.prevLogIndex = nextIndex[i] - 1;
                    if(arg.prevLogIndex >= baseIndex) {
                        arg.prevLogTerm = log[arg.prevLogIndex-baseIndex].term;
                    }
                    if(nextIndex[i] <= get_last_log_index()) {
                        arg.entries.insert(arg.entries.begin(), log.begin()+(nextIndex[i]-baseIndex), log.end());
                    }
                    arg.leaderCommit = commited_index;
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                } else {    // send snapshot
                    RAFT_LOG("send snapshot to %d", i);
                    std::vector<char> snapshot;
                    int last_include_index = 0;
                    int last_include_term = 0;
                    storage->recovery_from_snapshot(last_include_index, last_include_term, snapshot);
                    install_snapshot_args arg;
                    arg.term = current_term;
                    arg.leaderId = my_id;
                    arg.lastIncludedIndex = log[0].index;
                    arg.lastIncludedTerm = log[0].term;
                    arg.snapshot = snapshot;
                    thread_pool->addObjJob(this, &raft::send_install_snapshot, i, arg);
                }
            }
            lk.unlock();
        } else {
            lk.unlock();
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_timeout)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/



#endif // raft_h