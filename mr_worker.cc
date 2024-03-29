#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include "lang/verify.h"

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

inline bool ischar(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
}

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
    // Copy your code from mr_sequential.cc here.
    vector<KeyVal> res;
    int n = content.size();
    int i = 0, j = 0;
    while(true) {
        while(i < n && !ischar(content[i])) ++i;
        if(i == n) break;
        string key;
        KeyVal tmp;
        j = i+1;
        while(j < n && ischar(content[j])) ++j;
        tmp.key = content.substr(i, j-i);
        tmp.val = "1";
        res.emplace_back(tmp);
        i = j+1;
    }
    return res;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector <string> &values)
{
    // Copy your code from mr_sequential.cc here.
    int res = 0;
    for(auto &s: values) 
        res += stoi(s);

    return to_string(res);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const string &filenames);
	void doReduce(int index);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int mapers;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const string &filename)
{
	// Lab2: Your code goes here.
	string content;
	getline(ifstream(filename), content, '\0');
	vector<KeyVal> KVS = mapf(filename, content);
	vector<string> filecontent(REDUCER_COUNT);
	char out[100];
	for(auto &p: KVS) {
		int belogs = std::hash<std::string>()(p.key) % REDUCER_COUNT;
		filecontent[belogs] += p.key + " " + p.val + " ";
	}
	for(int i = 0; i < REDUCER_COUNT; ++i) {
		sprintf(out, "%s/mr_%d_%d.tmp", basedir.c_str(), index, i);
		// string path = basedir + "/mr_" + to_string(index) + "_" + to_string(i)+".tmp"
		ofstream os(out);
		os << filecontent[i];
	}
	doSubmit(MAP, index);
}

void Worker::doReduce(int index)
{
	// Lab2: Your code goes here.
	map<string, vector<string>> tab;
	char filename[100];
	string key, value;
	for(int i = 0; i < mapers; ++i) {
		sprintf(filename, "%s/mr_%d_%d.tmp", basedir.c_str(), i, index);
		ifstream is(filename);
		while(is >> key) {
			is >> value;
			if(tab.find(key) == tab.end()) {
				tab[key] = vector<string>(1, value);
			} else {
				tab[key].emplace_back(value);
			}
		}
	}

	string res;
	sprintf(filename, "%s/mr-out_%d.out", basedir.c_str(), index);
	for(auto it = tab.begin(); it != tab.end(); ++it) {
		value = reducef(it->first, it->second);
		res += it->first;
		res += " ";
		res += value;
		res += "\n";
	}
	ofstream os(filename);
	os << res;
	doSubmit(REDUCE, index);
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab2: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
		mr_protocol::AskTaskResponse response;
		// 封装一个 req 发起请求
		mr_protocol::status status = cl->call(mr_protocol::asktask, 0, response);
		VERIFY( status == mr_protocol::OK );
		mapers = response.mapers;
		if(response.type == MAP) doMap(response.index, response.filename);
		else if(response.type == REDUCE) doReduce(response.index);
		else sleep(1);
	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}


