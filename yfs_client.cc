// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);

  lc->acquire(1);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
  lc->release(1);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  bool ret; 
  lc->acquire(inum);
  ret = _isfile(inum);
  lc->release(inum);
  return ret;
}

bool
yfs_client::_isfile(inum inum)
{
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        //printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    //printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
  bool ret;
  lc->acquire(inum);
  ret = _isdir(inum);
  lc->release(inum);
  return ret;
}

bool
yfs_client::_isdir(inum inum)
{
    return ! _isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  lc->acquire(inum);
  int ret = _getfile(inum, fin);
  lc->release(inum);
  return ret;
}

int
yfs_client::_getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    //printf("getfile %016llx\n", inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    //printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  lc->acquire(inum);
  int r = _getdir(inum, din);
  lc->release(inum);
  return r;
}

int
yfs_client::_getdir(inum inum, dirinfo &din)
{
    int r = OK;

    //printf("getdir %016llx\n", inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

int
yfs_client::setattr(inum ino, size_t size)
{
  lc->acquire(ino);
  int r = _setattr(ino, size);
  lc->release(ino);
  return r;
}

// Only support set size of attr
int
yfs_client::_setattr(inum ino, size_t size)
{
    int r = OK;

    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        return IOERR;
    }
    
    if (size == a.size) {
        // nothing here..
    }
    else {
        std::string buf;
        if ((ec->get(ino, buf)) != extent_protocol::OK) {
            return IOERR;
        }
        if (size < a.size) { // truncate file
            buf = buf.substr(0, size);
            if ((ec->put(ino, buf)) != extent_protocol::OK) {
                return IOERR;
            }
        }
        else if (size > a.size) { // pad w/ '\0's
            buf.append((size - a.size), (char)0);
            if ((ec->put(ino, buf)) != extent_protocol::OK) {
                return IOERR;
            }
        }
    }
    // note: Since we have sent write requests back to ec(and ->es -> im),
    //       attr(e.g., size) of inode is updated, so no need to bother them explicitly.

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out, bool isdir)
{
  lc->acquire(parent);
  int r = _create(parent, name, mode, ino_out, isdir);
  lc->release(parent);
  return r;
}

int
yfs_client::_create(inum parent, const char *name, mode_t mode, inum &ino_out, bool isdir)
{
    int r = OK;

    bool exist;
    std::string buf_disk;
    if ((_lookup(parent, name, exist, ino_out)) != OK) {
        return IOERR;
    }
    if (exist) {
        return EXIST;
    }

    if (isdir) {
        if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        //if ((int)ino_out == 0) {
            printf("yfs: error creating dir\n");
            r = IOERR;
            return r;
        }
    } else {
        if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        //if ((int)ino_out == 0) {
            printf("yfs: error creating file\n");
            r = IOERR;
            return r;
        }
    }

    // build new entry
    std::stringstream ss;
    std::string strint;
    std::string strentry(" ");
    strentry.append(name);
    strentry.append(" ");
    ss << ino_out;
    ss >> strint;
    strentry.append(strint);

    // Update directory
    if ((ec->get(parent, buf_disk)) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    buf_disk.append(strentry);
    if ((ec->put(parent, buf_disk)) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
  lc->acquire(parent);
  int r = _lookup(parent, name, found, ino_out);
  lc->release(parent);
  return r;
}

int
yfs_client::_lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> list;
    std::list<dirent>::iterator it;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    if ((_readdir(parent, list)) != OK) {
        return IOERR;
    }

    found = false;
    ino_out = 0;
    for(it = list.begin(); it != list.end(); ++it)
    {
        if (((*it).name).compare(name) == 0)
        {
            found = true;
            ino_out = (*it).inum;
            break;
        }
    }

    // ino_out is set to 0 if found == false
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
  lc->acquire(dir);
  int r = _readdir(dir, list);
  lc->release(dir);
  return r;
}

int
yfs_client::_readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    std::string dirbuf;
    std::string filen;
    inum fileinum;
    dirent dire;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    /*
     * Seems that STL itself doesn't support container serialization :(
     * Choose a simple way to define dir content format.
     * <filename> <inum> <filename> <inum> ...
     * Use space as separator.
     * (Suppose that "blanks" should ***NOT*** exist in file names!
     */

    // There are two different return value standards here extent_protocol for ec & xxstatus for yfs retval, 
    // cannot ensure their consistency, r = extent_protocal::enum is bad...
    //if ((r = ec->get(dir, dirbuf)) != extent_protocol::OK)
    //    return r;
    if ((ec->get(dir, dirbuf)) != extent_protocol::OK) {
        return IOERR;
    }

    std::istringstream ist(dirbuf);
    while (ist >> filen) {
        ist >> fileinum;
        dire.name = filen;
        dire.inum = fileinum;

        list.push_back(dire);
    }

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
  lc->acquire(ino);
  int r = _read(ino, size, off, data);
  lc->release(ino);
  return r;
}

int
yfs_client::_read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    std::string buf;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    if ((ec->get(ino, buf)) != extent_protocol::OK) {
        return IOERR;
    }

    /*
    if (off >= buf.size())
        data = "";
    else if (off + size >= buf.size())
        data = buf.substr(off);
    else
        data = buf.substr(off, size);
    */
    data = buf.substr(off, size);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
  lc->acquire(ino);
  int r = _write(ino, size, off, data, bytes_written);
  lc->release(ino);
  return r;
}

int
yfs_client::_write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    std::string buf_disk;

    if (!data) {
        printf("!! yfs_client: write(): data is NULL!\n");
        return IOERR;
    }
    std::string strdata(data, size);

    //printf("zzz: yfs:write: ino(%llu), sz(%u), data=(%s)\n", ino, size, data);

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    bytes_written = 0;
    if ((ec->get(ino, buf_disk)) != extent_protocol::OK) {
        return NOENT;
    }

    if (buf_disk.length() >= off) {
        strdata = strdata.substr(0, size);
        buf_disk.replace(off, size, strdata);
        bytes_written += (size);
    }
    else {
        bytes_written += (off - buf_disk.length());
        buf_disk.append((off - buf_disk.length()), (char)0/*'\0'*/);
        strdata = strdata.substr(0, size);
        buf_disk = buf_disk.append(strdata);
        bytes_written += (size);
    }

    if ((ec->put(ino, buf_disk)) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
  lc->acquire(parent);
  int r = _unlink(parent, name);
  lc->release(parent);
  return r;
}

int yfs_client::_unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    bool exist;
    std::string buf_disk;
    inum ino;

    if ((_lookup(parent, name, exist, ino)) != OK) {
        return NOENT;
    }

    if ((ec->remove(ino)) != extent_protocol::OK) {
        return IOERR;
    }
    // Update parent's Entry
    std::list<dirent> list;
    std::list<dirent>::iterator it;

    r = _readdir(parent, list);
    if (r != OK) {
        return r;
    }

    // Write dir's content list back to disk
    buf_disk = "";
    for(it = list.begin(); it != list.end(); ++it)
    {
        if (((*it).name).compare(name) == 0) {
            // Success of Lookup should guarantee if & only if 1 condition walk through here
            continue;
        }
        else {
            buf_disk.append(" ");
            buf_disk.append((*it).name);
            buf_disk.append(" ");
            std::stringstream ss;
            std::string strint;
            ss << (*it).inum;
            ss >> strint;
            buf_disk.append(strint);
        }
    }

    if ((ec->put(parent, buf_disk)) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}
