// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

// change dprintf to nothing to clean debug messages
#define dprintf(args...) do { \
        /*do nothing*/ /*tprintf(args...)*/   \
        } while (0);

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  enum lstatus{NONE, FREE, LOCKED, ACQUIRING, RELEASING, RETRY};
  struct linfo{
    lstatus ls;
    pthread_t owner;
    bool is_revoked;
    pthread_cond_t lcv;
    linfo() {
      ls = NONE;
      owner = 0;
      is_revoked = false;
      VERIFY(pthread_cond_init(&lcv, NULL) == 0);
    }
    ~linfo() {
      VERIFY(pthread_cond_destroy(&lcv) == 0);
    }
  };
  pthread_mutex_t mx;
  pthread_cond_t cv;

  class lock_rls_handler{
    private:
      pthread_mutex_t hmx;
      pthread_cond_t hcv;
    public:
      std::queue<lock_protocol::lockid_t> qlock;
      lock_rls_handler() {
        dprintf("lock_client_cache: handler child thread init...\n");
	VERIFY(pthread_mutex_init(&hmx, NULL) == 0);
	VERIFY(pthread_cond_init(&hcv, NULL) == 0);
      }

      ~lock_rls_handler() {
        dprintf("lock_client_cache: handler child thread deinit...\n");
	VERIFY(pthread_mutex_destroy(&hmx) == 0);
	VERIFY(pthread_cond_destroy(&hcv) == 0);
      }

      void inque(lock_protocol::lockid_t lid) {
        pthread_mutex_lock(&hmx);
        qlock.push(lid);
        VERIFY(pthread_cond_broadcast(&hcv) == 0);
        pthread_mutex_unlock(&hmx);
      }

      lock_protocol::lockid_t outque() {
        while(qlock.empty())
          pthread_cond_wait(&hcv, &hmx);
        lock_protocol::lockid_t lid = qlock.front();
        qlock.pop();
        pthread_mutex_unlock(&hmx);
        return lid;
      }
  };

 public:
  static int last_port;
  lock_rls_handler lhandler;
  std::map<lock_protocol::lockid_t, linfo> llock;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
  void release_lock();
};


#endif
