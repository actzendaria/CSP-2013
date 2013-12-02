// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  VERIFY(pthread_mutex_init(&mx, NULL) == 0);
  VERIFY(pthread_cond_init(&cv, 0) == 0);
  printf("lock_server: init\n");
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&mx);
  lock_protocol::status ret = lock_protocol::OK;
  if (llock.find(lid) == llock.end()) {
    r = -2;
  }
  else {
    if ((llock[lid]).owner != clt)
      r = -1;
    else if (llock[lid].ls == FREE)
      r = 0;
    else
      r = llock[lid].owner;
  }

  pthread_mutex_unlock(&mx);
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  //printf("acquire request from clt %d\n", clt);
  lock_protocol::status ret = lock_protocol::OK;
  {
    pthread_mutex_lock(&mx);
    while (true) {
      if (llock.find(lid) == llock.end() || llock[lid].ls == FREE) {
        llock[lid].ls = LOCKED;
        llock[lid].owner = clt;
        break;
      }
      else {
        //if (llock[lid].owner == clt) {
        //  --nacquire;
        //  break;
        //}
        VERIFY(pthread_cond_wait(&cv, &mx) == 0);
      }
    }
  }
  ++nacquire;
  r = nacquire;
  pthread_mutex_unlock(&mx);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  r = nacquire;
  {
    pthread_mutex_lock(&mx);
    if (llock.find(lid) == llock.end()) {
      ret = lock_protocol::NOENT;
    }
    //else if (llock[lid].owner != clt) {
    //  ret = lock_protocol::NOENT;
    //}
    else {
      llock[lid].owner = -1;
      llock[lid].ls = FREE;
      --nacquire;
      VERIFY(pthread_cond_broadcast(&cv) == 0);
    }
  }
  pthread_mutex_unlock(&mx);
  return ret;
}
