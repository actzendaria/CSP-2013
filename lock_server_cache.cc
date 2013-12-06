// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  VERIFY(pthread_mutex_init(&mx, NULL) == 0);
  VERIFY(pthread_cond_init(&cv, 0) == 0);
  printf("lock_server: init\n");
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &r)
{
  lock_protocol::status ret = lock_protocol::OK;

  // client id is asking for lock lid
  {
    // lock (the global acq/rls!) for checking lid
    pthread_mutex_lock(&mx);

    std::map<lock_protocol::lockid_t, linfo>::iterator li;
    li = llock.find(lid);
    // if lock is not founded or it is free(not hold by any clts)
    if (li == llock.end() || llock[lid].ls == FREE) {
      // (create) lock and set the new owner
      llock[lid].ls = LOCKED;
      llock[lid].owner = id;
      ret = lock_protocol::OK;
      pthread_mutex_unlock(&mx);
    }
    // else we need to ask the clt which holds the lock to release
    // a rather simple solution is that server only serves the first
    // asking clt and stall it (
    //  1. save the clt
    //  2. revoke to the holdling client
    //  3. receive the lock
    //  4. send the lock to clt )
    // for other more clt asking for the lock, if the previous clt is
    // not served, we abandon the rest poor clts (RETRY).
    else if (llock[lid].wclts.size() == 0) {// no waiting clts for lid
      llock[lid].wclts.push_back(id);
      // rpx to the holdling clt
      std::string hid = llock[lid].owner;

      pthread_mutex_unlock(&mx);
      int tmp;
      rlock_protocol::status rret = rlock_protocol::OK;
      handle h(hid);
      if (h.safebind()) {
        rret =  h.safebind()->call(rlock_protocol::revoke, lid, tmp);
      }
      if (!h.safebind() || rret != rlock_protocol::OK) {
        tprintf("lock_server_cache: acquire() RPC to clt failed!\n");
        assert(true);
      }
      ret = lock_protocol::RETRY;
    }
    else { // lid locked and wclts not empty
      llock[lid].wclts.push_back(id);
      pthread_mutex_unlock(&mx);
      ret = lock_protocol::RETRY;
    }
  }
  r = (++nacquire);

  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  // a client releases its lock, possibly because server asked it to revoke
  lock_protocol::status ret = lock_protocol::OK;
  {
    pthread_mutex_lock(&mx);
    if (llock.find(lid) == llock.end()) {
      printf("lock_server(): release lock(%llu) not found!!!\n", lid);
      ret = lock_protocol::NOENT;
      pthread_mutex_unlock(&mx);
      r = (--nacquire);
      return ret;
    }
    if ((llock[lid].owner != id) || (llock[lid].ls != LOCKED))
    {
      printf("lock_server(): release lock(%llu) error!!!\nnot found or not locked\n", lid);
      ret = lock_protocol::RPCERR;
      pthread_mutex_unlock(&mx);
      r = (--nacquire);
      return ret;
    }
    if (llock[lid].wclts.empty())
    {
      printf("lock_server(): release lock(%llu), but no one is waiting for it (in wclts)\n", lid);
      ret = lock_protocol::OK;
      pthread_mutex_unlock(&mx);
      r = (--nacquire);
      return ret;
    }
    // someone is waiting for lid in wclts
    std::string nextclt = llock[lid].wclts.front();
    llock[lid].wclts.pop_front();
    llock[lid].owner = nextclt;
    // llock[lid].ls is LOCKED already and no need to change here

    // Server found that nextclt is waiting for lid, so send nextclt a retry RPC
    pthread_mutex_unlock(&mx);

    rlock_protocol::status rret;
    int tmp;
    handle h(nextclt);
    if (h.safebind())
    {
      rret = h.safebind()->call(rlock_protocol::retry, lid, tmp);
    }
    if (!h.safebind() || rret != rlock_protocol::OK) {
      tprintf("lock_server_cache: release() RPC to clt failed!\n");
      assert(true);
    }

    if (!llock[lid].wclts.empty()) // there are other wclts in list
    {
      // Server sent an immediate revoke after retry to the same clt
      // NOTICE: this revoke may reach clt even earlier than the previous retry!
      if (h.safebind())
      {
        tmp = 0;
        rret = h.safebind()->call(rlock_protocol::revoke, lid, tmp);
      }
      if (!h.safebind() || rret != rlock_protocol::OK) {
        tprintf("lock_server_cache: release() RPC to clt failed!\n");
        assert(true);
      }
    }
  }

  r = (--nacquire);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

