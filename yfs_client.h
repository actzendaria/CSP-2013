#ifndef yfs_client_h
#define yfs_client_h

#include <string>

#include "lock_protocol.h"
#include "lock_client_cache.h"

//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
  lock_release_user *lu;
  lock_client *lc;
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
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

 public:
  yfs_client(std::string, std::string);
  ~yfs_client();

  bool _isfile(inum);
  bool _isdir(inum);

  int _getfile(inum, fileinfo &);
  int _getdir(inum, dirinfo &);

  int _setattr(inum, size_t);
  int _lookup(inum, const char *, bool &, inum &);
  int _create(inum, const char *, mode_t, inum &, bool isdir);
  int _readdir(inum, std::list<dirent> &);
  int _write(inum, size_t, off_t, const char *, size_t &);
  int _read(inum, size_t, off_t, std::string &);
  int _unlink(inum,const char *);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &, bool isdir);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
};

#endif 
