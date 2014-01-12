#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>
#include "tprintf.h"
#include "lang/verify.h"

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  ScopedLock ml(&pxs_mutex);
  r = !stable;
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);
  my_n.n = 0;
  my_n.m = me;
}

void
proposer::setn()
{
  my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> cur_nodes, std::string newv)
{
  std::vector<std::string> accepts;
  std::vector<std::string> nodes;
  std::string v;
  bool r = false;

  ScopedLock ml(&pxs_mutex);
  tprintf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
	 print_members(cur_nodes).c_str(), instance, newv.c_str(), stable);
  if (!stable) {  // already running proposer?
    tprintf("proposer::run: already running\n");
    return false;
  }
  stable = false;
  setn();          // grab a unique and larger n ( > my_n.n AND highest_n_seen_in_prepare_of_acceptor)
  accepts.clear(); // record acceptors
  v.clear();
  
  //tprintf("----run(%s) prepare(ins(%u), Ncur_nodes(%lu)): my newv(%s)\n",
  //        me.c_str(), instance, cur_nodes.size(), newv.c_str());
  if (prepare(instance, accepts, cur_nodes, v)) {

    if (majority(cur_nodes, accepts)) {
      tprintf("paxos::manager: received a majority of prepare responses (%lu of %lu)\n",
              accepts.size(), cur_nodes.size());

      if (v.size() == 0) {
        tprintf("paxos::manager: majority responses but no value, use my own(newv).\n");
	v = newv;
      }

      breakpoint1();

      nodes = accepts;
      accepts.clear();
      accept(instance, accepts, nodes, v);

      if (majority(cur_nodes, accepts)) {
	tprintf("paxos::manager: received a majority of accept responses (%lu of %lu)\n",
                accepts.size(), cur_nodes.size());

	breakpoint2();

	decide(instance, accepts, v);
	r = true;
      } else {
	tprintf("paxos::manager: no majority of accept responses\n");
      }
    } else {
      tprintf("paxos::manager: no majority of prepare responses\n");
    }
  } else {
    tprintf("paxos::manager: prepare is rejected %d\n", stable);
  }
  stable = true;
  return r;
}

// proposer::run() calls prepare to send prepare RPCs to nodes
// and collect responses. if one of those nodes
// replies with an oldinstance, return false.
// otherwise fill in accepts with set of nodes that accepted,
// set v to the v_a with the highest n_a, and return true.
bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
  // You fill this in for the part of paxos
  // Note: if got an "oldinstance" reply, commit the instance using
  // acc->commit(...), and return false.
  int ret = rpc_const::timeout_failure;
  bool res = true;
  std::string m;
  paxos_protocol::preparearg parg;
  paxos_protocol::prepareres pres;
  prop_t n_a_h;
  n_a_h.n = 0;
  n_a_h.m = "";
  parg.instance = instance;
  parg.n = my_n;

  //tprintf("prepare to all nodes (%s)\n"/*, m.c_str()*/, me.c_str());
  for (unsigned i = 0; i < nodes.size(); i++) {
    m.clear();
    m = nodes[i];
    handle h(m);
    VERIFY(pthread_mutex_unlock(&pxs_mutex)==0);
    rpcc *cl = h.safebind();
    if (cl) {
      ret = cl->call(paxos_protocol::preparereq, me, parg, pres, 
	             rpcc::to(1000));
      VERIFY(pthread_mutex_lock(&pxs_mutex)==0);
    }
    else {
      VERIFY(pthread_mutex_lock(&pxs_mutex)==0);
      continue;
    }   
    if (ret != paxos_protocol::OK) {
      if (ret == rpc_const::atmostonce_failure || 
	  ret == rpc_const::oldsrv_failure) {
        //tprintf("!!!proposer prepare(): problem me(%s)->(%s) (%d) delete handler\n", 
	//       me.c_str(), m.c_str(), ret);
        //mgr.delete_handle(m);
      } else {
        //tprintf("!!!proposer prepare(): problem me(%s)->(%s) (%d)\n", 
	//       me.c_str(), m.c_str(), ret);
        //res = false;
      }
    }
    else {
      if (pres.oldinstance) {
        //tprintf("proposer prepare(): me(%s)->(%s) old instance(%u)\n", 
	//       me.c_str(), m.c_str(), instance);
        // acc->commit(...), and return false.
        acc->commit(instance, pres.v_a);
        res = false;
        return res;
      }
      else if (pres.accept) {
        accepts.push_back(m);
        if (n_a_h.m == "" && n_a_h.n == 0) {
          n_a_h = pres.n_a;
          v = pres.v_a;
        }
        else if (pres.n_a > n_a_h) {
          n_a_h = pres.n_a;
          v = pres.v_a;
        }
        else {
          /* Do not update v(value)with a lower n(proposal num).*/
        }
      }
      else if (!pres.accept) {
      }
      else {
        tprintf("proposer(%s) prepare(): unknown RPC result!!!\n", me.c_str());
      }
    }
  }
  tprintf("proposer(%s) prepare(): done %d\n", me.c_str(), res);
  return res;
}

// run() calls this to send out accept RPCs to accepts.
// fill in accepts with list of nodes that accepted.
void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
  // You fill this in for the part of paxos
  int ret = rpc_const::timeout_failure;
  std::string m;
  paxos_protocol::acceptarg aarg;
  bool ares;
  aarg.instance = instance;
  aarg.n = my_n;
  aarg.v = v;

  //tprintf("proposer(%s) accept() to all nodes.\n", me.c_str());
  for (unsigned i = 0; i < nodes.size(); i++) {
    //tprintf("proposer(%s) accept() to (%u)node (%s)\n", me.c_str(), i, nodes[i].c_str());
    m.clear();
    m = nodes[i];
    handle h(m);
    VERIFY(pthread_mutex_unlock(&pxs_mutex)==0);
    rpcc *cl = h.safebind();
    if (cl) {
      ret = cl->call(paxos_protocol::acceptreq, me, aarg, ares, 
	             rpcc::to(1000));
      VERIFY(pthread_mutex_lock(&pxs_mutex)==0);
    }
    else {
      VERIFY(pthread_mutex_lock(&pxs_mutex)==0);
      continue;
    }   
    if (ret != paxos_protocol::OK) {
      if (ret == rpc_const::atmostonce_failure || 
	  ret == rpc_const::oldsrv_failure) {
        //tprintf("!!!proposer accept(): cannot connect acceptor through RPC me(%s)->(%s) (%d) delete handler\n", 
	//       me.c_str(), m.c_str(), ret);
        //mgr.delete_handle(m);
      } else {
        //tprintf("!!!proposer accept(): cannot connect acceptor through RPC me(%s)->(%s) (%d)\n", 
	//       me.c_str(), m.c_str(), ret);
      }
    }
    else {
      if (!ares) { // proposal rejected
      }
      else { // proposal accpeted
        accepts.push_back(m);
      }
    }
  }
  tprintf("proposer(%s) accept(): done(void)\n", me.c_str());
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts, 
	      std::string v)
{
  // You fill this in for the part of paxos
  int ret = rpc_const::timeout_failure;
  std::string m;
  paxos_protocol::decidearg darg;
  int dres;
  darg.instance = instance;
  darg.v = v;

  //tprintf("proposer decide() to all accepts (%s) (why not all node!?)\n", me.c_str());
  for (unsigned i = 0; i < accepts.size(); i++) {
    m.clear();
    m = accepts[i];
    handle h(m);
    VERIFY(pthread_mutex_unlock(&pxs_mutex)==0);
    rpcc *cl = h.safebind();
    if (cl) {
      ret = cl->call(paxos_protocol::decidereq, me, darg, dres, 
	             rpcc::to(1000));
      VERIFY(pthread_mutex_lock(&pxs_mutex)==0);
    }
    else {
      VERIFY(pthread_mutex_lock(&pxs_mutex)==0);
      continue;
    }  
    if (ret != paxos_protocol::OK) {
      if (ret == rpc_const::atmostonce_failure || 
	  ret == rpc_const::oldsrv_failure) {
        //tprintf("!!!proposer decide(): problem me(%s)->(%s) (%d) delete handler\n", 
	//       me.c_str(), m.c_str(), ret);
        //mgr.delete_handle(m);
      } else {
        //tprintf("!!!proposer decide(): problem me(%s)->(%s) (%d)\n", 
	//       me.c_str(), m.c_str(), ret);
      }
    }
    else {
    }
  }
  tprintf("proposer(%s) decide(): done(void)\n", me.c_str());
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
  // You fill this in for the part of paxos
  // Remember to initialize *BOTH* r.accept and r.oldinstance appropriately.
  // Remember to *log* the proposal if the proposal is accepted.
  ScopedLock ml(&pxs_mutex);
  if (a.instance <= instance_h) {
    // reply oldinstance
    r.oldinstance = true;
    r.accept = false;
    //r.n_a = a.n; // ??? what is the proposal number for this old ins? Review: we won't change it then
    if (values.find(a.instance) == values.end())
    {
      tprintf("old instance(%u): ins_h(%u); value not found in acceptor values!\n", a.instance, instance_h); 
    }
    r.v_a = values[a.instance];
  }
  else if (a.n > n_h) {
    // accept the higher proposal
    n_h = a.n; // refresh the highest seen proposal num
    r.oldinstance = false;
    r.accept = true;
    r.n_a = n_a;
    r.v_a = v_a;
    // log the highest seen proposal
    l->logprop(n_h);
  }
  else {
    // reject a equal or lower proposal
    r.oldinstance = false;
    r.accept = false;
    tprintf("acceptor(%s) from(%s) preparereq(): rejected req(%u,%s) <= cur(%u,%s)\n",
            me.c_str(), src.c_str(), a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str()); 
  }
  return paxos_protocol::OK;
}

// the src argument is only for debug purpose
paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, bool &r)
{
  // You fill this in for the part of paxos
  // Remember to *log* the accept if the proposal is accepted.
  ScopedLock ml(&pxs_mutex);
  if (a.n >= n_h) {
    // reply accept_ok(n)
    // shall we need to update n_h(prepare) here?
    if (a.n > n_h) {
      tprintf("acceptor(%s) from(%s) acceptreq(): a.n(%u;%s) > n_h(%u;%s)!\n",
              me.c_str(), src.c_str(), a.n.n, a.n.m.c_str(), n_h.n, n_h.m.c_str());
    }
    n_a = a.n;
    v_a = a.v;
    // log the accept proposal
    // tprintf("acceptor(%s) acceptreq(): src_from(%s) logaccept n=(%u,%s) v=(%s)!\n",
    //        me.c_str(), src.c_str(), a.n.n, a.n.m.c_str(), a.v.c_str());
    l->logaccept(n_a, v_a);
    r = true;
  }
  else {
    // reply accept_reject
    r = false;
  }
  return paxos_protocol::OK;
}

// the src argument is only for debug purpose
paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{
  ScopedLock ml(&pxs_mutex);
  tprintf("decidereq for accepted instance %d (my instance %d) v=%s\n", 
	 a.instance, instance_h, v_a.c_str());
  if (a.instance == instance_h + 1) {
    VERIFY(v_a == a.v);
    commit_wo(a.instance, v_a);
  } else if (a.instance <= instance_h) {
    // we are ahead ignore.
  } else {
    // we are behind
    VERIFY(0);
  }
  return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  tprintf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
  if (instance > instance_h) {
    tprintf("commit: highest accepted instance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  ScopedLock ml(&pxs_mutex);
  commit_wo(instance, value);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    tprintf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    tprintf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    tprintf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    tprintf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
