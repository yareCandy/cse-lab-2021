#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"
#include "marshall.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab2: Your definition here.
		mr_tasktype type;
		int index;
		int mapers;
		string filename;
	};

	struct AskTaskRequest {
		// Lab2: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab2: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab2: Your definition here.
	};

};

inline marshall& operator<<(marshall& m, const mr_protocol::AskTaskResponse& response) {
	m << response.type;
	m << response.index;
	m << response.mapers;
	m << response.filename;
	return m;
}

inline unmarshall& operator>>(unmarshall& m, mr_protocol::AskTaskResponse& response) {
    int type;
	m >> type;
	m >> response.index;
	m >> response.mapers;
	m >> response.filename;
    response.type = (mr_tasktype)type;
	return m;
}

#endif

