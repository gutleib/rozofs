#include <linux/fs.h>
#include <unistd.h>
#include <errno.h>
#include "rozofs_ext4.h"
#include "xattr.h"
#include <rozofs/common/acl.h>
#include <string.h>
#include <rozofs/common/export_track.h>

#define BUFFER_HEAD_MAX 16
unsigned int fake_buffer_idx = 0;
struct buffer_head   fake_buffer_head[BUFFER_HEAD_MAX];

/*
**________________________________________________________
*/
/**
*  allocate a fake buffer  
*/
struct buffer_head *allocate_buffer_head()
{
   struct buffer_head *bh;
   
    int idx  = (++fake_buffer_idx)%(BUFFER_HEAD_MAX);
    bh = &fake_buffer_head[idx];
    memset(bh,0,sizeof(struct buffer_head));
    bh->b_blocknr = 0;
    return bh;
}
/*
**_________________________________________________________
*/
/**
*  read the extended attribute from disk

   @param superblock : tracking context associated with the export
   @param inode_key  : inode value of the extended attribute
*/
struct buffer_head *sb_bread(lv2_entry_t *inode,uint64_t inode_key)
{  
  struct buffer_head *bh = NULL;
  /**
  * check the presence of the extended attribute block
  */
  if (inode_key == 0) return NULL;
  /*
  ** check if there is an entry in the level 2 cache for extended attribute block
  */
  if (inode->extended_attr_p != NULL)
  {
    /*
    ** allocate a fake buffer header and fill in the reference of the block pointer
    */
    bh = allocate_buffer_head();
    bh->b_size = ROZOFS_XATTR_BLOCK_SZ-sizeof(uint64_t);
    bh->b_data = (char*) inode->extended_attr_p;    
    bh->b_blocknr = inode->attributes.s.i_file_acl;
    return bh;
  }
  /*
  ** It is always assume that the export provides the data, so we don't attempt to allocate
  ** memory on the client
  */
  return NULL;
}
/*
**__________________________________________________________________
*/
/**
*  release buffer

   @param bh : buffer handle: take care since the pointer might be null
*/
void brelse(struct buffer_head *bh)
{
  return ;

}

/*
**__________________________________________________________________
*/
/**
*  get the pointer to the inode
*/
int ext4_get_inode_loc(lv2_entry_t *inode,struct ext4_iloc *iloc)
{

   iloc->offset = 0;
   /*
   ** allocate a fake buffer handle
   */
   iloc->bh = allocate_buffer_head();
   iloc->bh->b_size = ROZOFS_INODE_SZ;
   iloc->bh->b_data = (char*) inode;
   return 0;

}
/*
**__________________________________________________________________
*/
/**
*  set the inode has dirty in order to re-write it on disk
   @param create : assert to 1 for allocated a block
   @param inode: pointer to the in memory inode
   @param bh: pointer to the data relative to the inode
*/

int ext4_handle_dirty_metadata(int create,lv2_entry_t *inode,struct buffer_head *bh)
{
   /*
   ** not supported on the client
   */
   return -EINVAL;
}
/*
**__________________________________________________________________
*/
/**
*  set the inode has dirty in order to re-write it on disk

   @param inode: pointer to the in memory inode
   @param bh: pointer to the data relative to the inode
*/
int ext4_mark_iloc_dirty(void *unused,lv2_entry_t *inode,struct ext4_iloc *iloc_p)
{
   /*
   ** not supported on the client
   */
   return -EINVAL;
}
/*
**__________________________________________________________________
*/
/**
*   get a memory block for storing extended attributes
*/

struct buffer_head *sb_getblk(lv2_entry_t *inode)
{
  struct buffer_head *bh = NULL;

  if (inode->extended_attr_p != NULL) 
  {
     printf("inode->extended_attr_p MUST be null\n");
     exit(0);
  }
  inode->extended_attr_p = malloc(ROZOFS_XATTR_BLOCK_SZ);
  if (inode->extended_attr_p == NULL) return NULL;
    
  bh =  allocate_buffer_head();
  bh->b_size = ROZOFS_XATTR_BLOCK_SZ;
  bh->b_data = (char*) inode->extended_attr_p;    
  return bh;
}
