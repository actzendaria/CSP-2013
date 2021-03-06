// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <pthread.h>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {
public:
  enum lstatus{FREE = 0, LOCKED};
  struct linfo{
    lstatus ls;
    int owner;
  };
protected:
  int nacquire;
  std::map<lock_protocol::lockid_t, linfo> llock;
  pthread_mutex_t mx;
  pthread_cond_t cv;

public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







