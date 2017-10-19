
#ifndef DISK_TABLE_SERVICE_H
#define DISK_TABLE_SERVICE_H

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <rozofs/common/log.h>


#define DISK_TB_EMPTY_ENTRY 0x8000

#define DISK_TABLE_MAX_ENTRY_PER_FILE  2048  /**< max entries per file    */

#define DISK_TB_HEADER_SZ (DISK_TABLE_MAX_ENTRY_PER_FILE*sizeof(uint16_t))




/**
* internal structure used for bitmap of files
*/
#define DISK_MAX_IDX_FOR_FILE_ID(p) (p->bitmap_size*8)
typedef struct _disk_tb_bitmap_file_t
{
   int dirty; /**< assert to one if the bitmap must be re-written on disk */
   char bitmap[1];
} disk_tb_bitmap_file_t;



typedef struct _disk_table_header_t
{  
   char *basename;    /**< basename of the table   */
   uint16_t entry_sz;  /**< size of the object stored in the table */
   uint32_t bitmap_size;  /**< size of the bitmap in bytes  */

   char *root_path;  /**< root path of the table              */
   disk_tb_bitmap_file_t *file_btmap_p;
   void *trck_inode_p;  /**< memory structure used for inode tracking */
} disk_table_header_t;
   
   
   
/*
**__________________________________________________________________
*/
/*
**  read one entry of the table

    @param ctx_p : pointer to the table context
    @param entry_id : index of the entry to search
    @param data_p : pointer to the data buffer that will contain the data associated with entry_id
    
    @retval > 0 : length of the data
    @retal  = 0 : no data, the entry does not exist
    @retval < 0 : error, see errno for details
    
*/
int disk_tb_read_entry(disk_table_header_t *ctx_p,uint32_t entry_id,void *data_p);

/*
**__________________________________________________________________
*/
/*
**  write one entry of the table

    @param ctx_p : pointer to the table context
    @param entry_id : index of the entry to search
    @param data_p : pointer to the data buffer that contains the data associated with entry_id
    
    @retval > 0 : length of the data
    @retal  = 0 : no data, the entry does not exist
    @retval < 0 : error, see errno for details
    
*/
int disk_tb_write_entry(disk_table_header_t *ctx_p,uint32_t entry_id,void *data_p);

/*
**__________________________________________________________________
*/
/**
*  release a disk table context
  @param ctx_p: pointer to the context to release
  
  @retval none
*/
void disk_tb_ctx_release(disk_table_header_t *ctx_p);
/*
**__________________________________________________________________
*/
/**
*  allocate a disk table context
  @param root: root_path
  @param name: name of the table
  @param entry_sz : size of a record
  @param bitmap_sz_powerof2 : bitmap file size in power of 2
  
  @retval <> NULL pointer to the allocated context
  @retval == NULL error (see errno for details)
*/
disk_table_header_t *disk_tb_ctx_allocate(char *root_path,char *name,int entry_sz,int bitmap_sz_powerof2);

/*
**__________________________________________________________________
*/
/*
**  read one entry of the table

    @param ctx_p : pointer to the table context
    @param entry_id : index of the entry to search
    @param data_p : pointer to the data buffer that will contain the data associated with entry_id
    
    @retval > 0 : length of the data
    @retal  = 0 : no data, the entry does not exist
    @retval < 0 : error, see errno for details
    
*/
int disk_tb_get_next_file_entry(disk_table_header_t *ctx_p,uint32_t *entry_file_idx_next_p);

/*
**__________________________________________________________________
*/    
/**
*   Open the table file given as input argument and return the current records
    within the file
    
    @param ctx_p : pointer to the table context
    @param file_idx : index of the file to open
    @param fd_p : pointer to the file descriptor
    
    @retval < 0 error
    @retval > 0 number of records in the file (fd_p contains the reference of the file descriptor
    @retval = 0 no records (file is closed)
*/   
int  disk_tb_get_nb_records(disk_table_header_t *ctx_p,int entry_file_idx,int *fd_p);
/*
**__________________________________________________________________
*/    
/**
*   Open the table file given as input argument and return the current records
    within the file
    
    @param ctx_p : pointer to the table context
    @param file_idx : index of the file to open
    @param fd_p : pointer to the file descriptor
    
    @retval < 0 error (see errno for details)
    @retval > 0 size of the read data
    @retval = 0 end of file
*/    
int  disk_tb_get_next_record(disk_table_header_t *ctx_p,int record_id,int fd,void *data_p);

#endif
