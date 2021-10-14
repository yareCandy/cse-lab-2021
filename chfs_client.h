#ifndef chfs_client_h
#define chfs_client_h

#include <string>
#include <list>
//#include "chfs_protocol.h"
#include "extent_client.h"
#include <vector>


class chfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    chfs_client::inum inum; // 0 if entry is invalid
    dirent(std::string n, chfs_client::inum i): name(n), inum(i) {}
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  void search(inum, const char *, bool &, inum &, int32_t &, std::string &);
  int create_with_type(inum, const char *, inum &, uint32_t);

 public:
  chfs_client();
  chfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  
  /** you may need to add symbolic link related methods here.*/
  int symlink(inum, const char *, const char*, inum &);
  int readlink(inum, std::string &);
};

#endif 


