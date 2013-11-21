// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mtx, NULL);
  pthread_cond_init (&cv, NULL);
}

lock_server::~lock_server()
{
  pthread_mutex_destroy(&mtx);
  pthread_cond_destroy (&cv);
  //pthread_exit(NULL);
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

  pthread_mutex_lock(&mtx);
  if ( (it = llock.find(lid)) != llock.end() ) {
    // lock exist
    //pthread_mutex_lock(&mtx);

    while(it->second != FREE) {
      pthread_cond_wait(&cv, &mtx);
      if (it->second == FREE) {
        break;
      }
    }
    it->second = LOCKED;
    pthread_mutex_unlock(&mtx);
    //pthread_exit(NULL);
    r = nacquire;
    return ret;

    /*while(true) {
      if (it->second == FREE) {
        it->second = LOCKED;
        pthread_mutex_unlock(&mtx);
        r = nacquire;
        return ret;
        break;
      }
      else {
        pthread_cond_wait(&cv, &mtx);
      }
    }*/
  }
  else { // create & lock
    //pthread_mutex_lock(&mtx);
    llock.insert(std::pair<lock_protocol::lockid_t, lstatus>(lid, LOCKED));
    pthread_mutex_unlock(&mtx);
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

  pthread_mutex_lock(&mtx);
  if ( (it = llock.find(lid)) != llock.end() ) {
    // lock exist
    if (it->second == LOCKED) {
      it->second = FREE;
      pthread_cond_broadcast(&cv);
    }
    else {
      ret = lock_protocol::NOENT;
    }

    pthread_mutex_unlock(&mtx);
    r = nacquire;
    return ret;
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
