// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("acq request from clt %d\n", clt);

  // use a simple spin-like block if locked
  std::map<lock_protocol::lockid_t, lstatus>::iterator it;
  if ( (it = llock.find(lid)) != llock.end() ) {
    // lock exist
    while(true) {
      if (it->second == FREE) {
        it->second = LOCKED;
        r = nacquire;
        return ret;
        break;
      }
    }
  }
  else { // create & lock
    llock.insert(std::pair<lock_protocol::lockid_t, lstatus>(lid, LOCKED));
    r = nacquire;
    return ret;
  }

  r = nacquire;
  ret = lock_protocol::RETRY;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("rls request from clt %d\n", clt);
  std::map<lock_protocol::lockid_t, lstatus>::iterator it;
  if ( (it = llock.find(lid)) != llock.end() ) {
    // lock exist
    if (it->second == LOCKED) {
      it->second = FREE;
      r = nacquire;
      return ret;
    }
    else {
      r = nacquire;
      ret = lock_protocol::NOENT;
      return ret;
    }
  }
  else {
    r = nacquire;
    ret = lock_protocol::NOENT;
    return ret;
  }

  r = nacquire;
  ret = lock_protocol::RETRY;
  return ret;
}
