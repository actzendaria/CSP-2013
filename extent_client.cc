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
  ret = cl->call(extent_protocol::getattr, eid, attr);
  printf("zzz: ec: getattr eid(%llu)\n", eid);
  return ret;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::create, type, id);
  printf("zzz: ec: create eid(%llu) type(%u)\n", id, type);
  // Your lab3 code goes here
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get, eid, buf);
  printf("zzz: ec: get eid(%llu) get_buf_sz(%u)\n", eid, buf.size());
  // Your lab3 code goes here
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  int tmp;
  printf("zzz: ec: put eid(%llu) strsz(%u)%s\n", eid, buf.size(), buf.c_str());
  ret = cl->call(extent_protocol::put, eid, buf, tmp);
  // Your lab3 code goes here
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int tmp;
  ret = cl->call(extent_protocol::remove, eid, tmp);
  printf("zzz: ec: remove eid(%llu)\n", eid);
  // Your lab3 code goes here
  return ret;
}


