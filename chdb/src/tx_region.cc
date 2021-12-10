#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    int r;
    this->db->vserver->execute(key, // query key
                               chdb_protocol::Put,
                               chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val},
                               r);
    return r;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    int r = -1;
    this->db->vserver->execute(key, // query key
                               chdb_protocol::Get,
                               chdb_protocol::operation_var{.tx_id = tx_id, .key = key},
                               r);
    return r;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    int r = 0;
    this->db->vserver->prepare(1,
            chdb_protocol::Prepare,
            chdb_protocol::prepare_var{.tx_id = tx_id},
            r);
    return chdb_protocol::prepare_not_ok + r;
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    int r = 0;
    this->db->vserver->commit(1,
            chdb_protocol::Commit,
            chdb_protocol::commit_var{.tx_id = tx_id},
            r);
    return r;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    int r = 0;
    this->db->vserver->rollback(1,
            chdb_protocol::Rollback,
            chdb_protocol::rollback_var{.tx_id = tx_id},
            r);
    printf("tx[%d] abort\n", tx_id);
    return 0;
}
