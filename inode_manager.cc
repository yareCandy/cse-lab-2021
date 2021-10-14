#include "inode_manager.h"
#include <cstring>
#include <iostream>
#include <ctime>
#include <stdint.h>

#define CEIL(x, y) (x) % (y) ? (x)/(y)+1 : (x)/(y);

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if(id >= BLOCK_NUM) return;
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{  
  if(id >= BLOCK_NUM) return;
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  // num -- block bitmap

  blockid_t num = BBLOCK(IBLOCK(INODE_NUM, BLOCK_NUM));
  blockid_t top = BBLOCK(BLOCK_NUM);
  uint32_t i, j;
  unsigned char buf[BLOCK_SIZE];

  while(num < top) { // for each bitmap block
    d->read_block(num, (char*)buf);
    i = 0;
    while(i < BLOCK_SIZE) { // for each byte in buf
      auto cur_byte = (unsigned char)buf[i];
      unsigned char mask = 0x80;
      j = 0;
      while(mask) { // for each bit in cur_byte
        if((cur_byte & ~mask) == cur_byte) {
          // found a free block, then mask bitmap
          buf[i] = cur_byte | mask;
          d->write_block(num, (char *)buf);
          return (((num-2)*BLOCK_SIZE + i) * 8) + j;
        }
        mask >>= 1;
        ++j;
      }
      ++i;
    }
    ++num;
  }
  return BLOCK_NUM;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  // get the block number of id
  blockid_t bit_block = BBLOCK(id);
  uint32_t pos = id % BPB / 8;
  unsigned char r = id % BPB % 8;

  unsigned char buf[BLOCK_SIZE];
  d->read_block(bit_block, (char*)buf);

  buf[pos] ^= 1 << (7-r);
  d->write_block(bit_block, (char *)buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  // set control blocks bitmap
  char buf[BLOCK_SIZE];
  blockid_t data_start_block = IBLOCK(INODE_NUM, sb.nblocks);
  // top -- 存放控制信息的block对应的bitmap存放需要的完整的block数
  blockid_t top = BBLOCK(data_start_block);
  // remainder -- 剩余的bit所需要的完整字节数
  uint32_t remainder = (data_start_block % BPB)/8;
  // last -- 最后一个凑不满一个字节的 bit 数
  unsigned char last = (data_start_block % BPB)%8;
  for(blockid_t block_num = 2; block_num < top; ++block_num) {
    memset(buf, 0xff, BLOCK_SIZE);
    d->write_block(block_num, buf);
  }
  memset(buf, 0, BLOCK_SIZE);
  for(uint32_t i = 0; i < remainder; ++i) {
    buf[i] = 0xff;
  }
  unsigned char tmp = ~((1<< (8-last)) - 1);
  buf[remainder] = tmp;
  d->write_block(top, buf);
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
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  // start from end block of bitmap, to search free inode
  blockid_t num = 1;
  while(num < INODE_NUM) {
    inode_t *node1 = get_inode(num);
    if(node1 == nullptr) {
      inode_t node;
      node.type = type;
      node.size = 0;
      uint32_t cur_time = time(nullptr);
      node.ctime = cur_time;
      node.mtime = cur_time;
      node.atime = cur_time;
      put_inode(num, &node);
      return num;
    }
    free(node1);
    ++num;
  }
  return num;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *node = get_inode(inum);
  if(node == nullptr) return;
  node ->type = 0;
  node ->size = 0;
  put_inode(inum, node);
  free(node);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  // printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
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

  // printf("\tim: put_inode %d\n", inum);
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
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
//  printf("\tread file %d blocks \n", inum);

  inode_t *node = get_inode(inum);
  if(node == nullptr) { buf_out = nullptr; *size = 0; return;}
  
  *size = node->size;
  blockid_t item = CEIL(*size, BLOCK_SIZE);
  *buf_out = (char *)malloc(BLOCK_SIZE * item);

  uint32_t old1 = MIN(NDIRECT, item);
  uint32_t old2 = item - old1;
  uint32_t offset = 0;

//  printf("\tsize: %d item: %d old1: %d old2: %d\n", node->size, item, old1, old2);
  for(uint32_t i = 0; i < old1; ++i) {
    bm->read_block(node->blocks[i], *buf_out+offset);
    offset += BLOCK_SIZE;
  }
//  printf("-------------- 1 --------------\n");
  if(old2) {
    uint32_t buf[NINDIRECT];
    bm->read_block(node->blocks[NDIRECT], (char *)buf);
    for(uint32_t i = 0; i < old2; ++i) {
//      printf("------------ i: %d blockid: %d offset: %d------------\n", i, buf[i], offset);
      bm->read_block(buf[i], *buf_out+offset);
      offset += BLOCK_SIZE;
    }
  }
  node->atime = time(nullptr);
  put_inode(inum, node);
  free(node);
//  printf("\tYes!\n");
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode_t *node = get_inode(inum);
  if(node == nullptr) {
    printf("\nwrite_file: inode not exists"); 
    return;
  }

  blockid_t oldnums = CEIL(node->size, BLOCK_SIZE);
  blockid_t old1 = MIN(oldnums, NDIRECT);
  blockid_t old2 = oldnums - old1;
  blockid_t neednums = CEIL(size, BLOCK_SIZE);
  blockid_t need1 = MIN(neednums, NDIRECT);
  blockid_t need2 = neednums - need1;
  uint32_t offset = 0;
  
  //printf("\twrite file:\n\tinum: %d\n", inum);
  //printf("\toldnums: %d old1: %d old2: %d neednums: %d need1: %d need2:%d\n", oldnums, old1, old2, neednums, need1, need2);

  if(neednums > MAXFILE) {
      std::cout << "Warning: file size is too large!" << std::endl;
  }

  uint32_t index = 0;
  //std::cout << "block num: ";
  while(index < MIN(old1, need1)) {
    bm->write_block(node->blocks[index], buf+offset);
    //std::cout << node->blocks[index] << " ";
    offset += BLOCK_SIZE;
    ++index;
  }
  while(old1 > index) {
    bm->free_block(node->blocks[index++]);
  }
  while(need1 > index) {
    blockid_t next_block = bm->alloc_block();
    bm->write_block(next_block, buf+offset);
    node->blocks[index] = next_block;
    ++index;
    offset += BLOCK_SIZE;
    //std::cout << next_block << " ";
  }

  if(old2 == 0) {
    node->blocks[NDIRECT] = bm->alloc_block();
  }

  blockid_t secondary[NINDIRECT];
  bm->read_block(node->blocks[NDIRECT], (char*)secondary);
  while(old2 > 0 && old2 > need2) {
    bm->free_block(secondary[--old2]);
  }
  while(old2 < NINDIRECT && old2 < need2) {
     secondary[old2++] = bm->alloc_block();
  }
  bm->write_block(node->blocks[NDIRECT], (char*)secondary);

  if(need2 == 0) {
    bm->free_block(node->blocks[NDIRECT]);
  } else {
    for(blockid_t i = 0; i < need2 && i < NINDIRECT; ++i) {
      bm->write_block(secondary[i], buf + offset);
      offset += BLOCK_SIZE;
      //std::cout << secondary[i] << " ";
    }
  }

  //std::cout << std::endl;
  uint32_t t = time(nullptr);
  node->atime = t;
  node->mtime = t;
  node->ctime = t;
  node->size = size;
  put_inode(inum, node);
  free(node);
//  printf("\tYES!\n");
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *node = get_inode(inum);
  if(node == nullptr) return;
  a.type = node->type;
  a.size = node->size;
  a.atime = node->atime;
  a.ctime = node->ctime;
  a.mtime = node->mtime;
  free(node);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *node = get_inode(inum);
  if(node == nullptr) return;
 
  // 1. 释放文件的 blocks
  blockid_t items = CEIL(node->size, BLOCK_SIZE);
  blockid_t old1 = MIN(items, NDIRECT);
  blockid_t old2 = items - old1;
  if(old2) {
    uint32_t buf[NINDIRECT];
    bm->read_block(node->blocks[NDIRECT], (char *)buf);
    while(old2) {
      bm->free_block(buf[--old2]);
    }
    bm->free_block(node->blocks[NDIRECT]);
  }
  
  while(old1) {
    bm->free_block(node->blocks[--old1]);
  }
  // 2. free_inode
  free_inode(inum);
  free(node);
}

