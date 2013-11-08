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

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
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
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
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

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    //printf("yfs_c: setattr: %u/*%016llx*/\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        return IOERR;
    }
    
    printf("yfs_c: setattr(): size_new: %u; size_old: %u\n", size, a.size);
    if (size == a.size) {
        //nothing here..
    }
    else {
        std::string buf;
        if ((ec->get(ino, buf)) != extent_protocol::OK)
            return IOERR;

        if (size < a.size) { //truncate file
            printf("yfs_c setattr(): truncate before buf size: %u\n", buf.size());
            buf = buf.substr(0, size);
            printf("yfs_c setattr(): after buf size: %u\n", buf.size());
            if ((ec->put(ino, buf)) != extent_protocol::OK)
                return IOERR;
        }
        else if (size > a.size) { //pad w/ '\0's
            printf("yfs_c setattr(): appending before buf size: %u\n", buf.size());
            buf.append((size - a.size), (char)0);
            printf("yfs_c setattr(): after buf size: %u\n", buf.size());
            if ((ec->put(ino, buf)) != extent_protocol::OK)
                return IOERR;
        }
    }
    // note: Since we have sent write requests back to ec(and ->es -> im),
    //       attr(e.g., size) of inode is updated, so we don't need to bother them explicitly.

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out, bool isdir)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    printf("yfs_c: create(): pinum: %lld, n: %s, dir: %d\n", (uint64_t)parent, name, (int)isdir);
    bool exist;
    std::string buf_disk;
    if ((lookup(parent, name, exist, ino_out)) != OK)
        return IOERR;
    if (exist) {
        return EXIST;
    }

    //extent_protocol::extentid_t id;
    //extent_protocol::attr a;

    //memset(&a, 0, sizeof(a));

    if (isdir) {
        ec->create(extent_protocol::T_DIR, ino_out);
        if ((int)ino_out == 0) {
            printf("yfs: error creating dir\n");
            return IOERR;
        }
    } else {
        ec->create(extent_protocol::T_FILE, ino_out);
        if ((int)ino_out == 0) {
            printf("yfs: error creating file\n");
            return IOERR;
        }
    }

    printf("yfs_c: create(): n: %s, ino: %lld\n", name, (uint64_t)ino_out);

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
    if ((ec->get(parent, buf_disk)) != extent_protocol::OK)
        return IOERR;

    //printf("yfs_c: create(): dir_cnt:\n%s\n", buf_disk.c_str());

    buf_disk.append(strentry);
    //printf("yfs_c: create(): dir_ctn after:\n%s\n", buf_disk.c_str());
    if ((ec->put(parent, buf_disk)) != extent_protocol::OK)
        return IOERR;

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> list;
    std::list<dirent>::iterator it;
    printf("yfs_c: lookup(): pinum: %lld, n: %s\n", (uint64_t)parent, name);

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    if ((readdir(parent, list)) != OK)
        return IOERR;

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

    // ino_out is 0 (history:0<-undefined) if found == false
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
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
     * Daniel:
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
    if ((ec->get(dir, dirbuf)) != extent_protocol::OK)
        return IOERR;

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
    int r = OK;
    std::string buf;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    // There are two different return value standards here extent_protocol for ec & xxstatus for yfs retval, 
    // cannot ensure their consistency, r = extent_protocal::enum is bad...
    //if ((r = ec->get(ino, buf)) != extent_protocol::OK)
    //    return r;
    printf("yfs_c read(): ino:%llu ;sz:%zu ;off:%llu\n", ino, size, off);
    if ((ec->get(ino, buf)) != extent_protocol::OK)
        return IOERR;

    printf("yfs_c read(): buf_sz:%u\n", buf.size());
    if (off >= buf.size())
        data = "";
    else if (off + size >= buf.size())
        data = buf.substr(off);
    else
        data = buf.substr(off, size);

    printf("yfs_c read(): data_out_final_sz:%u\n", data.size());
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    std::string buf_disk;

    // need check if data == NULL 
    std::string strdata(data);
    printf("yfs_c write(): ino:%llu ;sz:%zu ;off:%llu ;indatasz:%zu\n", ino, size, off, strdata.size());
    //printf("yfs_c write(): buf start:%s\n", strdata.substr(0, 20).c_str());
    //printf("yfs_c write(): buf end:%s\n", strdata.substr(strdata.size()-20, 20).c_str());

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    bytes_written = 0;
    if ((ec->get(ino, buf_disk)) != extent_protocol::OK)
        return IOERR;

    printf("yfs_c write(): buf_on_disk_original_sz:%zu\n", buf_disk.length());

    if (buf_disk.length() >= off) {
        //buf_disk = buf_disk.substr(0, off);
        strdata = strdata.substr(0, size);
        buf_disk.replace(off, size, strdata);
        bytes_written += (size);
    }
    else {
        bytes_written += (off - buf_disk.length());
        //buf_disk.replace(buf_disk.length(), 0, (off - buf_disk.length()), 0/*'\0'*/);
        buf_disk.append((off - buf_disk.length()), (char)0/*'\0'*/);
        strdata = strdata.substr(0, size);
        //printf("yfs_c write() over end: bytes_extend:%zu; data_sz:%zu data:\n%s\n", bytes_written, strdata.size(), strdata.c_str());
        buf_disk = buf_disk.append(strdata);
        printf("yfs_c write() over end: data_to_write_all_final_sz:%zu\n", buf_disk.size());
        bytes_written += (size);
        printf("yfs_c write() over end: bytes_final_written_sz:%zu\n", bytes_written);
        //printf("yfs_c write() over end: str:\n%s\n", buf_disk.c_str());
        //std::cout << ("yfs_c write() over end: str:\n") << buf_disk << "\n";
    }

    printf("yfs_c write(): buf_to_write_all: (sz)%zu \n", buf_disk.length());

    if ((ec->put(ino, buf_disk)) != extent_protocol::OK)
        return IOERR;

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
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

    if ((lookup(parent, name, exist, ino)) != OK) {
        printf("yfs_c: unlink() parent not found!\n");
        return IOERR;
    }

    if ((ec->remove(ino)) != extent_protocol::OK) {
        printf("yfs_c: unlink() cannot remove!\n");
        return IOERR;
    }
    // Update parent's Entry
    std::list<dirent> list;
    std::list<dirent>::iterator it;

    r = readdir(parent, list);
    if (r != OK) {
        return r;
    }

    /*
    for(it = list.begin(); it != list.end(); ++it)
    {
        // since lookuped, one entry should match below
        if (((*it).name).compare(name) == 0) {
            list.erase(it);
            break;
        }
    }
    */

    // Write dir's content list back to disk
    buf_disk = "";
    int zzz_count=0;
    printf("yfs_c: unlink inname=%s; dirlist=%d\n", name, list.size());
    for(it = list.begin(); it != list.end(); ++it)
    {
        // since lookuped, one entry should match below
        if (((*it).name).compare(name) == 0) {
            zzz_count++;
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
    if (zzz_count != 1)
        printf("yfs_c: unlink count wrong!!!\n");
    printf("yfs_c: unlink buf_to_write:\n%s\n", buf_disk.c_str());

    if ((ec->put(parent, buf_disk)) != extent_protocol::OK)
        return IOERR;

    return r;
}
