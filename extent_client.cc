// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

// a demo to show how to use RPC
extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  if (extent_cache.find(eid) == extent_cache.end()) {
    extent_cache[eid].dirty = false;
    extent_cache[eid].valid_buf = false;
    extent_cache[eid].valid_attr = false;
    printf("ec getattr(%llu), first time\n", eid);
  }

  if (!extent_cache[eid].valid_attr) {
    ret = cl->call(extent_protocol::getattr, eid, attr);
    extent_cache[eid].attr = attr;
    extent_cache[eid].valid_attr = true;
    printf("ec getattr(%llu), not valid, ask server...\n", eid);
  }
  else {
    //attr is cached~
    attr = extent_cache[eid].attr;
  }

  printf("zzz: ec: getattr eid(%llu)\n", eid);
  return ret;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::create, type, id);
  if (extent_cache.find(id) == extent_cache.end()) {
    extent_cache[id].dirty = false;
    extent_cache[id].valid_buf = false;
    extent_cache[id].valid_attr = false;
    printf("ec create(%llu), first time\n", id);
  }

  time_t tm = time(NULL);
  extent_cache[id].attr.type = type;
  extent_cache[id].attr.mtime = (uint32_t)tm;
  extent_cache[id].attr.ctime = (uint32_t)tm;

  // values not updated
  extent_cache[id].attr.atime = 0;
  extent_cache[id].buf = "";
  extent_cache[id].attr.size = 0;
  extent_cache[id].valid_attr = true;
  extent_cache[id].valid_buf = true;

  printf("zzz: ec: create eid(%llu) type(%u)\n", id, type);
  // Your lab3 code goes here
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  if (extent_cache.find(eid) == extent_cache.end()) {
    extent_cache[eid].dirty = false;
    extent_cache[eid].valid_buf = false;
    extent_cache[eid].valid_attr = false;
    printf("ec get(%llu), first time\n", eid);
  }

  if (!extent_cache[eid].valid_buf) {
    ret = cl->call(extent_protocol::get, eid, buf);
    extent_cache[eid].buf = buf;
    extent_cache[eid].valid_buf = true;
    printf("ec get(%llu), not valid, ask server...\n", eid);
  }
  else {
    //buf is cached~
    buf = extent_cache[eid].buf;
    time_t tm = time(NULL);
    extent_cache[eid].attr.atime = (uint32_t)tm;
  }
  printf("ec get(%llu) get_buf_sz(%u)\n", eid, buf.size());
  // Your lab3 code goes here
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int tmp;
  if (extent_cache.find(eid) == extent_cache.end()) {
    extent_cache[eid].dirty = false;
    extent_cache[eid].valid_buf = false;
    extent_cache[eid].valid_attr = false;
    printf("ec put(%llu), first time\n", eid);
  }

  // (uncached) put may change both buf & attr
  // extent_cache[eid].valid_buf = false;
  // extent_cache[eid].valid_attr = false;
  
  //if (!extent_cache[eid].valid_buf) {
    //ret = cl->call(extent_protocol::get, eid, buf);
    //extent_cache[eid].buf = buf;
    //extent_cache[eid].valid = true;
    //printf("ec put(%llu), not valid buf, ask server (SHOULD NOT HAPPEN!)...\n", eid);
    //extent_cache[eid].buf = buf;
  //}
  //else {
    // put -> es.put -> im.write_file; this may cause attr changes, but since we cache it on ec side,
    //  modification on attr only needs to be applied the last time since ec writes back (does RPC
    //  call to server), and the attr logic is still hold by im, so no need to bother attr here.
    //extent_cache[eid].buf = buf;
  //}
  extent_cache[eid].buf = buf;
  extent_cache[eid].dirty = true;
  time_t tm = time(NULL);
  extent_cache[eid].attr.mtime = (uint32_t)tm;
  if (extent_cache[eid].attr.type == extent_protocol::T_DIR)
    extent_cache[eid].attr.ctime = (uint32_t)tm;
  extent_cache[eid].attr.size = buf.size();
  extent_cache[eid].valid_buf = true;
  extent_cache[eid].valid_attr = true;
  

  printf("ec put(%llu) strsz(%u)%s\n", eid, buf.size(), buf.c_str());
  //ret = cl->call(extent_protocol::put, eid, buf, tmp);
  // Your lab3 code goes here
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int tmp;

  // cache for eid is no longer valid since it is removed
  extent_cache[eid].valid_buf = false;
  extent_cache[eid].valid_attr = false;
  extent_cache[eid].attr.type = 0;
  extent_cache[eid].attr.size = 0;
  extent_cache[eid].attr.atime = 0;
  extent_cache[eid].attr.mtime = 0;
  extent_cache[eid].attr.ctime = 0;
  extent_cache.erase(eid);

  //ret = cl->call(extent_protocol::remove, eid, tmp);
  printf("zzz: ec: remove eid(%llu)\n", eid);
  // Your lab3 code goes here
  return ret;
}


