// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

#include <map>

class extent_client {
 public:
  class cache_content {
   public:
    std::string buf;
    extent_protocol::attr attr;
    bool dirty;
    bool valid_buf;
    bool valid_attr;
    cache_content() {dirty=false;valid_buf=false;valid_attr=false;}
  };
 private:
  rpcc *cl;
  std::map<extent_protocol::extentid_t, cache_content> extent_cache;

 public:
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
};

#endif 

