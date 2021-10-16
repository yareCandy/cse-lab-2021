//
// A simple sequential MapReduce for WordCount
//

#include <string>
#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>

using namespace std;

struct KeyVal {
    string key;
    string val;
    KeyVal() = default;
    KeyVal(string &k, string &v) {
        swap(k, key);
        swap(v, val);
    }
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
    // Your code goes here
    // Hints: split contents into an array of words.
    stringstream ss(content);
    vector<KeyVal> res;
    string key, value;
    cout << "Map:" << endl;
    while(ss >> key) {
        ss >> value;
        KeyVal tmp(key, value);
        cout << tmp.key << " " << tmp.val << endl; 
        res.emplace_back(tmp);
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
    // Your code goes here
    // Hints: return the number of occurrences of the word.
    int res = 0;
    cout << "Reduce:" << endl;
    for(auto &s: values) {
        cout << s << " ";
        res += s == key;
    }
    cout << endl;
    return to_string(res);
}

int main(int argc, char ** argv)
{
    if (argc < 2) {
        cout << "Usage: mrsequential inputfiles...\n";
        exit(1);
    }

    vector <string> filename;
    vector <KeyVal> intermediate;

    //
    // read each input file,
    // pass it to Map,
    // accumulate the intermediate Map output.
    //

    for (int i = 1; i < argc; ++i) {

        string filename = argv[i];
        string content;

        // Read the whole file into the buffer.
        getline(ifstream(filename), content, '\0');

        vector <KeyVal> KVA = Map(filename, content);

        intermediate.insert(intermediate.end(), KVA.begin(), KVA.end());

    }

    //
    // a big difference from real MapReduce is that all the
    // intermediate data is in one place, intermediate[],
    // rather than being partitioned into NxM buckets.
    //

    sort(intermediate.begin(), intermediate.end(),
    	[](KeyVal const & a, KeyVal const & b) {
		return a.key < b.key;
	});

    //
    // call Reduce on each distinct key in intermediate[],
    // and print the result to mr-out-0.
    //

    for (unsigned int i = 0; i < intermediate.size();) {
        unsigned int j = i + 1;
        for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;)
            j++;

        vector < string > values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = Reduce(intermediate[i].key, values);
        printf("%s %s\n", intermediate[i].key.data(), output.data());

        i = j;
    }
    return 0;
}

