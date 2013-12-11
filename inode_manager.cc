#include <time.h>
#include "inode_manager.h"
#include "utils.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  blockid_t valid_blockid = 0, iter;
  char block_buf[BLOCK_SIZE];
  int byte_idx, bit_idx;

  // iteration through blocks
  for(iter = BBH; (iter < BBT) && !valid_blockid; ++iter) {
    // Q: What if read_block() failed? Seems that we cannot detect it w/o modifying read_block()... :( 
    read_block(iter, block_buf);
    // iteration through bytes in a block
    for(byte_idx = 0; (byte_idx < BLOCK_SIZE) && !valid_blockid; ++byte_idx) {
      // iteration through but in a byte, order: MSB --> LSB
      for(bit_idx = BYTE_SIZE - 1; (bit_idx >= 0) && !valid_blockid; --bit_idx) {
        // --Start  Critical Section (An atomic test&set operation is preferred)
        if(!testn(bit_idx, (unsigned char&)block_buf[byte_idx])) {
          setn(bit_idx, (unsigned char&)block_buf[byte_idx]);
          // --End Critical Section
          valid_blockid = (BYTE_SIZE - 1 - bit_idx) + (byte_idx * BYTE_SIZE) + ((iter - BBH) * BLOCK_SIZE);
          // Write bitmap back to disk
          write_block(BBLOCK(valid_blockid), block_buf);
        }
      }
    }
  }

  if (valid_blockid == 0)
      printf("\tim: alloc() block use up!\n");

  return valid_blockid;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char block_buf[BLOCK_SIZE];

  blockid_t start = (IBLOCK(INODE_NUM, sb.nblocks) + 1), end = (sb.nblocks - 1);//start block, end block
  if (id < start || id > end) {
    printf("\tim: free() out of range!\n");
    return;
  }


  read_block(BBLOCK(id), block_buf);

  if (! (testn(BYTE_SIZE - 1 - (id%BPB)%BYTE_SIZE, (unsigned char&)block_buf[(id%BPB)/BYTE_SIZE])) ) {
    printf("\tim: free() unable to free id!");
    return;
  }

  unsetn(BYTE_SIZE - 1 - (id%BPB)%BYTE_SIZE, (unsigned char&)block_buf[(id%BPB)/BYTE_SIZE]);
  write_block(BBLOCK(id), block_buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  blockid_t iter;
  char block_buf[BLOCK_SIZE];
  unsigned int byte_idx, totbits=0, done=0;
  int bit_idx;

  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  // set bitmap for superblocks, bitmap blocks, inode table blocks...
  for(iter = BBH; (iter < BBT) && !done; ++iter) {
    read_block(iter, block_buf);
    for(byte_idx = 0; (byte_idx < BLOCK_SIZE) && !done; ++byte_idx) {
      for(bit_idx = BYTE_SIZE - 1; (bit_idx >= 0) && !done; --bit_idx, ++totbits) {
        if(totbits >= DBH(sb.nblocks)) {
          done = 1;
          continue;
        }
        setn(bit_idx, (unsigned char&)block_buf[byte_idx]);
      }
    }
  }

  // Write back bitmap block(s)
  for(iter = BBLOCK(0); iter <= BBLOCK(totbits - 1); ++iter) { 
    write_block(iter, block_buf);
  }
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  // According to IBLOCK(0) == IBLOCK(1) == IBLOCK(2), seems that
  // the 1st inode is for root_dir, instead of the 1st inode block?

  inode *ino, *ino_disk;
  uint32_t inum;
  char buf[BLOCK_SIZE];
  time_t tm;
 
  if (type == 0) {
    printf("\tim: alloc inode type %d\n", type);
    return 0;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  tm = time(NULL);
  ino->type = type;
  ino->size = 0;  
  //ino->atime = (uint32_t)tm;  
  ino->mtime = (uint32_t)tm;  
  ino->ctime = (uint32_t)tm;  

  // Q: According to inode_manager() and IBLOCK(), inode(0) is not used? 
  // Since inum starts from 1, we can use either 0 or inum > INODE_NUM (e.g. MAX(uint_32t)) as abnormal return value
  // We use NULL here (need to modify get_inode() if using 0)
  for(inum = 1; (inum < INODE_NUM); ++inum) {
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode*)buf + inum%IPB;
    // Find an empty entry in inode table 
    if (ino_disk->type == 0) {
      break;
    }
  }

  if (inum >= INODE_NUM) {
     printf("\tim: Cannot alloc inode! Probably inode run out!\n");
     free(ino);
    return 0;
  }

  switch (type) {
    case extent_protocol::T_DIR:
    case extent_protocol::T_FILE:
      put_inode(inum, ino);
      break;
    default:
      // Unknown type, ignore alloc request
      inum = 0;
  }

  free(ino);
  return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode *ino;
  ino = get_inode(inum);

  if (ino == NULL) {
    // Return some error code
    return;
  }
  ino->type = 0;
  ino->size = 0;
  // unset is not necessary below...
  ino->atime = 0;
  ino->mtime = 0;
  ino->ctime = 0;

  // Write back to disk
  put_inode(inum, ino);
  free(ino);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  //printf("\tim: get_inode %u\n", inum);

  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum(%u) out of range\n", inum);
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  //printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  inode *ino;
  char block_buf[BLOCK_SIZE], indblock_buf[BLOCK_SIZE];
  blockid_t blockn; // Block numbers
  time_t tm;


  ino = get_inode(inum);

  if (ino == NULL) {
    printf("\tim: cannot read inum!\n");
    return;
  }

  *size = ino->size;
  *buf_out = (char *)malloc(ino->size);
  memset(*buf_out, 0, *size);
  blockn = ((*size) + BLOCK_SIZE - 1) / BLOCK_SIZE;
  if (!blockn) {
    tm = time(NULL);
    ino->atime = (uint32_t)tm;
    put_inode(inum, ino);
    free(ino);
    return;
  }

  // indirect blocks not used
  if (blockn <= NDIRECT){
    for (blockid_t i = 0; i < blockn - 1; ++i) {
      bm->read_block(ino->blocks[i], &(*buf_out)[i*BLOCK_SIZE]);
    }
    
    memset(block_buf, 0, BLOCK_SIZE);
    bm->read_block(ino->blocks[blockn - 1], block_buf);
    memcpy(&(*buf_out)[(blockn - 1)*BLOCK_SIZE], block_buf, (*size) - (blockn - 1)*BLOCK_SIZE);
  }
  else { // indirect blocks used
    for (blockid_t i = 0; i < NDIRECT; ++i) {
      bm->read_block(ino->blocks[i], &(*buf_out)[i*BLOCK_SIZE]);
    }
    bm->read_block(ino->blocks[NDIRECT], indblock_buf);
    
    // indirect blocks (if exist)
    for (blockid_t i = 0; i < blockn-NDIRECT - 1; ++i) {
      bm->read_block(*(blockid_t*)&indblock_buf[i*sizeof(blockid_t)], &(*buf_out)[(i + NDIRECT)*BLOCK_SIZE]);
    }

    bm->read_block(*(blockid_t*)&indblock_buf[(blockn - NDIRECT - 1)*sizeof(blockid_t)], block_buf);
    memcpy(&(*buf_out)[(blockn - 1)*BLOCK_SIZE], block_buf, (*size) - (blockn - 1)*BLOCK_SIZE);  
  }

  // Update attrs of inode
  tm = time(NULL);
  ino->atime = (uint32_t)tm;
  put_inode(inum, ino);
  free(ino);
   
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  struct inode *ino;
  blockid_t bold, bnew, indbold, indbnew, dbold, dbnew; // b = block number; ind = indirect; d = direct
  char block_buf[BLOCK_SIZE], indblock_buf[BLOCK_SIZE];
  time_t tm;

  ino = get_inode(inum);
  if (!ino){
    // printf("\tim: cannot get inode!\n");
    return;
  }
  bold = ((ino->size) + BLOCK_SIZE - 1) / BLOCK_SIZE;
  bnew = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  if (bnew > MAXFILE){
    //printf("\tim: cannot support big file!\n");
    free(ino);   
    return;
  }

  if (bold > NDIRECT) {
    indbold = (bold - NDIRECT);
    dbold = NDIRECT;
  }
  else {
    indbold = 0;
    dbold = bold;
  }
  if (bnew > NDIRECT) {
    indbnew = (bnew - NDIRECT);
    dbnew = NDIRECT;
  }
  else {
    indbnew = 0;
    dbnew = bnew;
  }
  
  // free blocks
  if (bnew < bold) {
    if (indbold > 0) {
      bm->read_block(ino->blocks[NDIRECT], block_buf);
      for (blockid_t i = indbnew; i < indbold; i++) {
        bm->free_block(*((blockid_t*)&block_buf[i*sizeof(block_buf)]));
      }
      // bnew don't need indirect list block, free it
      if (!indbnew) {
        bm->free_block(ino->blocks[NDIRECT]);
        // set to 0 not necessary?
        ino->blocks[NDIRECT] = 0;
      }
    }

    for (blockid_t i = dbnew; i < dbold;i++) {
      bm->free_block(ino->blocks[i]);
      ino->blocks[NDIRECT]=0;
    }

  }
  else if (bnew > bold) {
    for (blockid_t i = dbold; i < dbnew; ++i) {
      ino->blocks[i] = bm->alloc_block();
    }

    if (indbnew > 0) {
      // bnew need a new indirect list block, alloc one
      if (!indbold) {
        ino->blocks[NDIRECT] = bm->alloc_block();
      }
      bm->read_block(ino->blocks[NDIRECT], block_buf);
      for (blockid_t i = indbold; i < indbnew; ++i) {
        *((blockid_t*)&block_buf[i*sizeof(blockid_t)]) = bm->alloc_block();
      } 
      bm->write_block(ino->blocks[NDIRECT], block_buf);
    }
  }
  
  // Quick path here if write nothing here, just update attrs
  if (size == 0) {
    tm = time(NULL);
    ino->size = size;
    if (ino->type == extent_protocol::T_DIR)
      ino->ctime = (uint32_t)tm;
    ino->mtime = (uint32_t)tm;
    put_inode(inum, ino);
    free(ino);   
    return;
  }

  for (blockid_t i = 0; i < MIN(dbnew, bnew-1); ++i) {
    bm->write_block(ino->blocks[i], &buf[i*BLOCK_SIZE]);
  } 
  if (indbnew > 0) {
    bm->read_block(ino->blocks[NDIRECT], block_buf);
    for (blockid_t i = 0; i < indbnew - 1; ++i) {
      bm->write_block(*((blockid_t*)&block_buf[i*sizeof(blockid_t)]), &buf[(i+NDIRECT)*BLOCK_SIZE]);
    }
  }

  memset(indblock_buf, 0, BLOCK_SIZE);
  memcpy(indblock_buf, &buf[(bnew - 1)*BLOCK_SIZE], size - (bnew - 1)*BLOCK_SIZE);
  if (bnew <= NDIRECT)
    bm->write_block(ino->blocks[bnew-1], indblock_buf);
  else
    bm->write_block(*(blockid_t*)&block_buf[(bnew - NDIRECT - 1)*sizeof(blockid_t)], indblock_buf);

  tm = time(NULL);
  ino->size = size;
  ino->ctime = (uint32_t)tm;
  ino->mtime = (uint32_t)tm;
  put_inode(inum, ino);
  free(ino);   
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */

  inode* ino;

  ino = get_inode(inum);
  if (ino == NULL) {
    // Error fetching!
    return;
  }
  a.type = (uint32_t)ino->type;  
  a.size = (uint32_t)ino->size;  
  a.atime = (uint32_t)ino->atime;  
  a.mtime = (uint32_t)ino->mtime;  
  a.ctime = (uint32_t)ino->ctime;

  free(ino);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  inode *ino;
  int bnum, i;
  blockid_t *indblocks;
  char buf_indblocks[BLOCK_SIZE];

  ino = get_inode(inum);
  if (ino == NULL) {
    // Error, cannot fetch inode:<inum>!
    return;
  }

  bnum = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  // Free indirect block
  if (bnum > NDIRECT) {
    bm->read_block(ino->blocks[NDIRECT], buf_indblocks);
    indblocks = (blockid_t*)buf_indblocks;
    for (i = 0; i < (bnum - NDIRECT); ++i) {
      bm->free_block(indblocks[i]);
    }
    bm->free_block(ino->blocks[NDIRECT]);
  }

  for (i = 0; i < bnum; ++i) {
    bm->free_block(ino->blocks[i]);
  }

  free_inode(inum);
  free(ino);
  return;
}

