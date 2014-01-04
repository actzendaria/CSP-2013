// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#include "extent_client.h"
int lock_client_cache::last_port = 0;

void lock_client_cache::release_lock() {
  while(true)
  {
    lock_protocol::lockid_t lid = lhandler.outque();
    dprintf("lock_client_cache: release lock: got lock(%llu) sending release RPC to server\n", lid);
    int tmp;
    if (lu != NULL)
      lu->dorelease(lid);
    lock_protocol::status ret = cl->call(lock_protocol::release, lid, id, tmp);
    if (ret != lock_protocol::OK) {
      dprintf("lock_client_cache: lhandler thread: release RPC failed on lock(%llu)!\n", lid);
    }
    pthread_mutex_lock(&mx);
    llock[lid].ls = NONE;
    llock[lid].is_revoked = false;
    VERIFY(pthread_cond_broadcast(&(llock[lid].lcv)) == 0);
    pthread_mutex_unlock(&mx);
  }
}

//void* lock_client_cache::lhandler_thread(void *clt)
void* lhandler_thread(void *clt)
{
  dprintf("lock handler(release) thread in lock_client_cache init...\n");
  lock_client_cache* pclt = (lock_client_cache*) clt;
  pclt->release_lock();
  pthread_exit(NULL);
}

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  dprintf("lock_client_cache: initializing...\n");
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  pthread_t lock_handler_t;
  VERIFY(pthread_mutex_init(&mx, NULL) == 0);
  //pthread_create(&lock_handler_t, NULL, lhandler_thread, (void *)this);
  pthread_create(&lock_handler_t, NULL, lhandler_thread, (void *)this);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  lock_protocol::status ret = lock_protocol::OK;
  {
    pthread_mutex_lock(&mx);
    if (llock.find(lid) == llock.end()) {
      dprintf("client_cache: acquire lock(%llu) not found in client list, init...\n", lid);
      llock[lid].ls = NONE;
      llock[lid].is_revoked = false;
      llock[lid].owner = 0;
    }

    while (true) {
      if (llock[lid].ls == FREE) {
        dprintf("client_cache: acquire lock(%llu) FREE <break>.\n", lid);
        break;
      }
      else if (llock[lid].ls == RETRY) {
        dprintf("client_cache: acquire lock(%llu) RETRY <break>.\n", lid);
        break;
      }
      else if (llock[lid].ls == LOCKED) {
        dprintf("client_cache: acquire lock(%llu) LOCKED <sleep>.\n", lid);
        VERIFY(pthread_cond_wait(&(llock[lid].lcv), &mx) == 0);
      }
      else if (llock[lid].ls == ACQUIRING) {
        dprintf("client_cache: acquire lock(%llu) ACQUIRING <sleep>.\n", lid);
        VERIFY(pthread_cond_wait(&(llock[lid].lcv), &mx) == 0);
      }
      else if (llock[lid].ls == RELEASING) {
        dprintf("client_cache: acquire lock(%llu) RELEASING <sleep>.\n", lid);
        VERIFY(pthread_cond_wait(&(llock[lid].lcv), &mx) == 0);
      }
      else if (/*(llock[lid].ls == RELEASING) || */
               (llock[lid].ls == NONE)) {
        if (llock[lid].ls == NONE)
          dprintf("client_cache: acquire lock(%llu) NONE <sent acq RPC>.\n", lid);
        /*else if (llock[lid].ls == RELEASING)
          dprintf("client_cache: acquire lock(%llu) RELEASING <sent acq RPC>.\n", lid);*/
        llock[lid].ls = ACQUIRING;
        pthread_mutex_unlock(&mx);
        int tmp;
        ret = cl->call(lock_protocol::acquire, lid, id, tmp);
        VERIFY (ret == lock_protocol::OK || ret == lock_protocol::RETRY);// zzz debug
        pthread_mutex_lock(&mx);
        // sending RPC is not locked, so we may receive a retry before acquire RPC returned...
        // if retry, lock status may set to FREE, or even LOCKED/RELEASING/ACQUIRING(later)...
        if (ret == lock_protocol::OK) { // got the lock
          dprintf("client_cache: acquire lock(%llu) acq RPC response OK.\n", lid);
          break;
        }
        else if (llock[lid].ls == FREE) {
          dprintf("client_cache: acquire lock(%llu) acq RPC response RETRY, but lock is FREE now! <break>\n", lid);
          break;
        }
        else if (llock[lid].ls == RETRY) {
          dprintf("client_cache: acquire lock(%llu) acq RPC response RETRY, but lock is RETRY now! <break>\n", lid);
          break;
        }
        else if (ret == lock_protocol::RETRY) { // wait for server's retry RPC
          // normal RETRY --> wait for server's retry (retry_handler
          // delayed RETRY where lock is LOCKED --> wait for client's release(release by other threads)
          VERIFY(pthread_cond_wait(&(llock[lid].lcv), &mx) == 0);
        }
        else { // unknown lock status
          dprintf("lock_client_cache: acquire() unknown status!\n");
          assert(true);
        }
      }
    }
  }

  dprintf("lock_client_cache: acquire() lock(%llu) done!\n", lid);
  llock[lid].ls = LOCKED;
  llock[lid].owner = pthread_self();
  pthread_mutex_unlock(&mx);

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  // thread in clt announce to release, don't really release it if the server is not requiring(revoke) it! 

  // release
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mx);
  if (llock.find(lid) == llock.end()) {
    dprintf("lock_client_cache release(): cannot find lid!\n");
    ret = lock_protocol::NOENT;
    pthread_mutex_unlock(&mx);
    return ret;
  }

  if (!llock[lid].is_revoked) {
    // set it to free simply
    dprintf("lock_client_cache release(): lock(%llu) is not set revoked\n", lid);
    llock[lid].owner = 0;
    llock[lid].ls = FREE;

  }
  else { // needs to return back to server
    dprintf("lock_client_cache release(): lock(%llu) is set revoked\n", lid);
    llock[lid].owner = 0;
    llock[lid].ls = RELEASING;
    pthread_mutex_unlock(&mx);
    int tmp;
    if (lu != NULL)
      lu->dorelease(lid);
    ret = cl->call(lock_protocol::release, lid, id, tmp);
    if (ret != lock_protocol::OK)
      dprintf("lock_client_cache: release() RPC to clt failed!\n");
    
    pthread_mutex_lock(&mx);
    llock[lid].is_revoked = false;
    llock[lid].ls = NONE;
  }
  // TODO:Improve here later
  // now we don't broadcast other waiters for lid
  // they will wait for next acquire from server
  // 
  //VERIFY(pthread_cond_broadcast(&cv) == 0);
  // broadcast other waiters for lid
  VERIFY(pthread_cond_broadcast(&(llock[lid].lcv)) == 0);
  pthread_mutex_unlock(&mx);

  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  rlock_protocol::status ret = rlock_protocol::OK;
  {
    // revoke from server
    pthread_mutex_lock(&mx);

    if (llock.find(lid) == llock.end()) {
      dprintf("lock_client_cache revoke_handler(): lid(%llu) cannot found in client! Initing(?)\n", lid);
      llock[lid].ls = NONE;
      llock[lid].is_revoked = false;
      llock[lid].owner = 0;
    }
    else if (llock[lid].ls != NONE) {
      dprintf("lock_client_cache revoke_handler(): lid(%llu) status is not NONE(%d) when receiving revoke RPC!\n", lid, llock[lid].ls);
    }
      
    while (true) {
      if (llock[lid].ls == FREE) {
        dprintf("lock_client_cache revoke_handler(): lid(%llu) status is FREE now, push it to lhandler to release it. <mx locked when inqueing>\n", lid);
        llock[lid].ls = RELEASING;
        llock[lid].owner = 0;
        /*lock_protocol::status rret;
        int tmp;
        rret  = cl->call(lock_protocol::release, lid, id, tmp);

        pthread_mutex_lock(&mx);
        llock[lid].ls = NONE;
        llock[lid].is_revoke = false;*/
        lhandler.inque(lid);
        break;
      }
      else if ((llock[lid].ls == LOCKED) || (llock[lid].ls == ACQUIRING) || llock[lid].ls == RETRY) {
        // RETRY = (first)FREE, is it ok to set is_revoked here?
        // would the case be:
        //  set to is_revoked, but never triggered this boolean ever (to really release (i.e. inque to handler) this lock) ?
        if (llock[lid].ls == RETRY) {
          dprintf("lock_client_cache revoke_handler(): lid(%llu) status is RETRY, at least wait for one client acquire!. <is_revoked == true>\n", lid);
        }
        else {
          dprintf("lock_client_cache revoke_handler(): lid(%llu) status is LOCKED/ACQURING. <is_revoked == true>\n", lid);
        }
        llock[lid].is_revoked = true;
        break;
      }
      else if ((llock[lid].ls == RELEASING) || 
               (llock[lid].ls == NONE)) {
        // Case:
        //  server sent lock
        //  server sent revoke
        //  client received revoke
        //  client received lock
        if (llock[lid].ls == RELEASING) {
          dprintf("lock_client_cache revoke_handler(): lid(%llu) status is RELEASING. <is_revoked no set>\n", lid);
        }
        else {
          dprintf("lock_client_cache revoke_handler(): lid(%llu) status is NONE. <is_revoked no set>\n", lid);
        }
        //llock[lid].is_revoked = true;
        break;
      }
      else { // unknown lock status
        dprintf("lock_client_cache: revoke_handler() unknown status!\n");
        assert(true);
      }
    }
  }

  dprintf("lock_client_cache revoke_handler(): lid(%llu) done.\n", lid);
  pthread_mutex_unlock(&mx);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  {
    // retry from server, now clt owns lock lid!
    pthread_mutex_lock(&mx);

    if (llock.find(lid) == llock.end()) {
      dprintf("lock_client_cache retry_handler(): lock(%llu) not found! Init(?)\n", lid);
      llock[lid].ls = NONE;
      llock[lid].is_revoked = false;
      llock[lid].owner = 0;
    }
    if (llock[lid].ls != NONE) {
      // LOCKED/FREE/ACQURING/RELEASING
      dprintf("zzz: lock_client_cache retry_handler(): lock(%llu) status is not NONE ls(%d)\n", lid, llock[lid].ls);
    }
    llock[lid].ls = RETRY;
    llock[lid].owner = 0;
    VERIFY(pthread_cond_broadcast(&(llock[lid].lcv)) == 0);
    pthread_mutex_unlock(&mx);
  }
  return ret;
}

void lock_release_eclt::dorelease(lock_protocol::lockid_t lid) {
  if (ec == NULL) {
    printf("lock_release_eclt: dorelease ec is NULL!\n");
    return;
  }

  ec->flush((extent_protocol::extentid_t)lid);
}
