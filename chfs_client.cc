// chfs client.  implements FS operations using extent server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

struct Entry {
    char name[80];
    chfs_client::inum inum;
};

bool
chfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

void
chfs_client::search(inum parent, const char *name, bool& found, inum &ino_out,
        int32_t &pos, std::string &content) {
    ino_out = 0;
    found = false;

    // get the content of parent dir
    if(!isdir(parent)) { printf("parent is not dir!\n"); return; }
    extent_protocol::attr a;
    ec->getattr(parent, a);
    ec->get(parent, content);

    const Entry *buf = (Entry*)const_cast<char*>(content.c_str());
    int32_t top = a.size / sizeof(Entry);
    printf("cc::search dir size: %u  top: %d\n", a.size, top);
    for(int32_t i = 0; i < top; ++i) {
        const Entry &tmp = buf[i];
        std::cout << "file name: " << tmp.name << " ino: " << tmp.inum << std::endl;
        if(tmp.inum == 0 && pos == -1) { pos = i; continue; }
        if(!strcmp(tmp.name, name)) { //found
            ino_out = tmp.inum;
            found = true;
            std::cout << "\t" << name << " has existed !!!!" << std::endl;
        }
    }
}

int
chfs_client::create_with_type(inum parent, const char *name, inum &ino_out, uint32_t type)
{
    bool found = false;
    int32_t pos = -1;
    std::string content;
    search(parent, name, found, ino_out, pos, content);
    if(found) { return EXIST; }
  
    ec->create(type, ino_out);

    // modify the dir content
    Entry *buf = (Entry*)const_cast<char*>(content.c_str());
    // printf("empty pos: %d\n", pos);
    if(pos == -1) {
        Entry entry;
        strcpy(entry.name, name);
        entry.inum = ino_out;
        std::string tmp;
        tmp.assign((char*)&entry, sizeof(Entry));
        content += tmp;
        // std::cout << "write name: " << entry.name << std::endl;
    } else {
        buf[pos].inum = ino_out; // modification for buf is also for content
        strcpy(buf[pos].name, name);
    }
    ec->put(parent, content);
    return OK;
}

int
chfs_client::symlink(inum parent, const char* link, const char* name, inum &ino_out)
{
    int r = OK;

    printf("------------ client symlink parent: %llu link: %s name: %s ----------\n", parent, link, name);
    r = create_with_type(parent, name, ino_out, extent_protocol::T_SYMLINK);
    if(r == EXIST) return EXIST;
    ec->put(ino_out, std::string(link));
    printf("------------ client symlink parent: %llu link: %s name: %s ----------\n", parent, link, name);

    return r;
}

int
chfs_client::readlink(inum ino, std::string &buf)
{
    printf("------------ client readlink ino: %llu ----------\n", ino);
    int r = OK;
    ec->get(ino, buf);
    printf("------------ client readlink ino: %llu ----------\n", ino);

    return r;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    // get the content of inode ino
    std::string buf;
    ec->get(ino, buf);
    // modify its size
    buf.resize(size, '\0');
    // write back
    ec->put(ino, buf);

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("--------- client create parent: %llu  name: %s -------------\n", parent, name);
    r = create_with_type(parent, name, ino_out, extent_protocol::T_FILE);
    printf("-------------- client create end ino_out: %llu --------------\n", ino_out);
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("--------- client mkdir parent: %llu  name: %s -------------\n", parent, name);
    r = create_with_type(parent, name, ino_out, extent_protocol::T_DIR);
    printf("-------------- client mkdir end ino_out: %llu --------------\n", ino_out);

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    printf("------------ client lookup parent: %llu name: %s ----------\n", parent, name);
    found = false;
    int32_t pos = -1;
    std::string content;
    search(parent, name, found, ino_out, pos, content);
    printf("------------ client lookup ino: %llu name: %s ----------\n", ino_out, name);

    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    // get the content of parent dir

    printf("--------- client readdir: %llu -------------\n", dir);
    if(!isdir(dir)) { printf("parent is not dir!\n"); return r; }
    extent_protocol::attr a;
    ec->getattr(dir, a);
    std::string content;
    ec->get(dir, content);
    
    Entry *buf = (Entry*)const_cast<char*>(content.c_str());
    int32_t top = a.size / sizeof(Entry);
    for(int32_t i = 0; i < top; ++i) {
        Entry &tmp = buf[i];
        if(tmp.inum) { // valid entry
            list.emplace_back(tmp.name, tmp.inum);
        }
    }
    printf("--------- client readdir: %llu -------------\n", dir);
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    printf("--------- client read: %llu -------------\n", ino);
    ec->get(ino, data);
    if(off < (off_t)data.size()) {
        data = data.substr(off, size);
    } else {
        data = "";
    }
    printf("--------- client read: %llu -------------\n", ino);

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    printf("--------- client write: %llu -------------\n", ino);
    std::string buf;
    ec->get(ino, buf);
    std::string tmp;
    tmp.assign(data, size);
    // tmp.resize(size, '\0');
    if((size_t)(off + size) < buf.size()) {
        tmp += buf.substr(off+size, buf.size());
    }
    buf.resize(off, '\0');
    buf += tmp;
    ec->put(ino, buf);
    bytes_written = size;
    printf("--------- client write: %llu -------------\n", ino);

    return r;
}

int chfs_client::unlink(inum parent, const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    printf("--------- client unlink: %llu  name: %s -------------\n", parent, name);
    extent_protocol::attr a;
    ec->getattr(parent, a);
    std::string content;
    ec->get(parent, content);
    
    Entry *buf = (Entry*)const_cast<char*>(content.c_str());
    int32_t top = a.size / sizeof(Entry);
    for(int32_t i = 0; i < top; ++i) {
        Entry &tmp = buf[i];
        if(tmp.inum && !strcmp(tmp.name, name)) {
            ec->remove(tmp.inum);
            tmp.inum = 0; // set the entry is invalid 
            memset(tmp.name, 0, 80);
            ec->put(parent, content);
            break;
        }
    }
    printf("--------- client unlink: %llu  name: %s -------------\n", parent, name);
    return r;
}


