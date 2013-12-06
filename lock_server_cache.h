#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include <deque>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  enum lstatus{FREE = 0, LOCKED};
  struct linfo{
    lstatus ls;
    std::string owner;
    std::deque<std::string> wclts; // waiting clts (which require the lock)
  };
  pthread_mutex_t mx;
  pthread_cond_t cv;
 public:
  lock_server_cache();
  std::map<lock_protocol::lockid_t, linfo> llock;
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
  //lab4
  int revoke(lock_protocol::lockid_t, std::string id, int &);
  int retry(lock_protocol::lockid_t, std::string id, int &);
};

#endif
