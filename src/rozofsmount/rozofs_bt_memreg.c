/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_queue.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "rozofs_bt_api.h"
#include "rozofs_bt_proto.h"

pthread_rwlock_t rozofs_bt_shm_lock;
rozofs_memreg_t *rozofs_shm_tb=NULL;
int rozofs_shm_count;
rozofs_memreg_t *rozofs_bt_stc_mem_p = NULL;
int rozofs_bt_mem_owner_is_rozofsmount = 0;

int rozofs_bt_add_memreg(rozofs_memreg_t *mem_p);

/*__________________________________________________________________________
*/
/**
* display the configuration of the shared memories associated with the storcli
*/
void rozofs_bt_shmem_display(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    int i;

    rozofs_memreg_t *p = &rozofs_shm_tb[0];

    pthread_rwlock_rdlock(&rozofs_bt_shm_lock);  

    pChar += sprintf(pChar, "   name           |    local address  |  remote address  |      size      | lock | fds  |\n");
    pChar += sprintf(pChar, "------------------+-------------------+------------------+----------------+------+------+\n");
    for (i = 0; i < ROZOFS_SHARED_MEMORY_REG_MAX; i ++,p++)
    {
      if (p->name == NULL) continue;
      pChar +=sprintf(pChar," %16s | %16p  | %16p |%15llu |%4d  |%4d  |\n",p->name,
                      p->addr,
                      p->remote_addr,
                      (long long unsigned int)p->length,
                      p->lock,p->fd_share
                      );    
    
    }
    pthread_rwlock_unlock(&rozofs_bt_shm_lock);  
    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}



/*__________________________________________________________________________
*/
/**
* display the configuration of the shared memories associated with the storcli
*/
void rozofs_bt_stc_shmem_display(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    char bufall[64];
    char bufall2[64];

    int i,k;

    rozofs_memreg_t *q = rozofs_bt_stc_mem_p;
    rozofs_stc_memreg_t *p;
    if (q == NULL)
    {
       pChar += sprintf(pChar,"no shared memory between rozofsmount and storcli\n");
       uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
    }
    p= (rozofs_stc_memreg_t*)q->addr;


    pChar += sprintf(pChar, "   name           |  remote address   |      size      | delReq |      storcli ack bitmap         |      storcli done bitmap         |\n");
    pChar += sprintf(pChar, "------------------+-------------------+----------------+--------+---------------------------------+----------------------------------+\n");
    for (i = 0; i < ROZOFS_SHARED_MEMORY_REG_MAX; i ++,p++)
    {
      if (p->name[0] == 0) continue;
      if (p->delete_req==0)
      {
        memset(bufall,0x20,32);
        bufall[32]=0;
      }
      else
      {
	for (k= 0; k <ROZOFS_BT_MAX_STORCLI;k++) {
          if (p->storcli_ack[k] ==0) bufall[k]=0x30;
	  else bufall[k]=0x31;	
	}
	bufall[k] = 0;
      }
      for (k= 0; k <ROZOFS_BT_MAX_STORCLI;k++) {
        if (p->storcli_done[k] ==0) bufall2[k]=0x30;
	else bufall2[k]=0x31;	
      }
      bufall2[k] = 0;

      
      pChar +=sprintf(pChar," %16s | %16p  |%15llu |%s|%s |%s |\n",p->name,
                      p->remote_addr,
                      (long long unsigned int)p->length,
                      (p->delete_req) ? "   YES  ":"    NO  ",
		      bufall,bufall2
                      );    

    }
    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}
/*
**_________________________________________________________________________________________________
*/
/**
   Create or map a shared memory
   
   @param name: name of the shared memory
   @param length : length of the shared memory (only for create)
   @param create: assert to 1 for shared memory creation
   @param fd: pointer to the file descriptor of the shared memory

   @retval <> NULL: pointer to the mapped shared memory
   @retval NULL (error: see errno for details
*/   
void *rozofs_shm_create(rozofs_memreg_t *mem_p,int create)
{

   char filename[128];
   int ret;
   int flags = O_RDWR;
   void *p;   
   struct stat sb;   
   size_t length;
   
   sprintf(filename,"/%s",mem_p->name);
   if (create)
   {
      ret =shm_unlink(filename);
      if (ret < 0)
      {
        if(errno != ENOENT)
	{
          severe("error of shared memory creation:%s:%s\n",mem_p->name,strerror(errno));
	  return NULL;
	}
      }
   }
   if (create) flags = flags | O_CREAT;
   ret = shm_open(filename,flags,0644);     
   if (ret < 0)
   {
     severe("error of shared memory creation:%s:%s\n",mem_p->name,strerror(errno));
     return NULL;
   }
   if (create)
   {
     if (ftruncate (ret, mem_p->length) == -1) {
      severe ("ftruncate failure for %s : %s",mem_p->name,strerror(errno));
      close(ret);
      return NULL;
     } 
     length = mem_p->length;
   } 
   else
   {

     if (fstat (ret, &sb) == -1) {
       severe ("fstat failure for %s : %s",mem_p->name,strerror(errno));
       close(ret);
       return NULL;
     }
     length = sb.st_size;   
   
   } 
   p = mmap (0, length, PROT_READ|PROT_WRITE, MAP_SHARED, ret, 0);
   if (p == MAP_FAILED) {
           severe ("map failure for %s : %s",mem_p->name,strerror(errno));
	   close(ret);
           return NULL;
   }
   if (create)  memset(p,0,length);
   /*
   ** fill up the context
   */
   mem_p->addr = p;
   mem_p->length =length;
   mem_p->fd_share = ret;

   return p;
}

/*
**_________________________________________________________________________________________________
*/
#if 0
void rozofs_shm_deregister_on_disconnect(int fd)
{
    int i;
     rozofs_memreg_t *p = &rozofs_shm_tb[0];
     
    pthread_rwlock_wrlock(&rozofs_bt_shm_lock);  
     
    for (i = 0; i < ROZOFS_SHARED_MEMORY_REG_MAX; i++,p++)
    {
      if (p->fd !=fd) continue;
      munmap(p->addr,p->length);
      close(p->fd_share);
      shm_unlink(p->name);
      info("sharemem /dev/shm/%s disconnected !!",p->name);
      p->fd= -1;
      rozofs_shm_count--;
      free(p->name);
      p->name = NULL;
      break;    
    }
    pthread_rwlock_unlock(&rozofs_bt_shm_lock);  
}
#endif
/*
**_________________________________________________________________________________________________
*/
/**
   allocate a context to handle shared memory registration & creation
   
   @param name: name of the shared memory
   
   @retval <> NULL : pointer to the allocated context
   @retval  NULL: error (see errno for details
   
*/
rozofs_memreg_t *rozofs_shm_context_alloc(char *name)
{
    int i;
    int nb_entries = 0;
    int freeslot = -1;
    rozofs_memreg_t *p = &rozofs_shm_tb[0];

    
    if (rozofs_shm_count == 0) freeslot = 0;
    if (rozofs_shm_count != 0)
    {
      for (i = 0; i < ROZOFS_SHARED_MEMORY_REG_MAX; p++,i++)
      {
	if (p->name == NULL) {
	    if (freeslot < 0) freeslot = i;
	    continue;
	}
	nb_entries++;
	if (strcmp(p->name,name) == 0)
	{
          errno=EEXIST;
	  goto  error;
	}
	if (rozofs_shm_count == nb_entries)
	{
	  freeslot = i+1;
	  break;
	}
      }
    }
    if ((freeslot < 0) ||( freeslot >= ROZOFS_SHARED_MEMORY_REG_MAX))
    {
      errno = ENOMEM;
      goto  error;
    }
    p = &rozofs_shm_tb[freeslot];
    p->name = strdup(name);
    p->lock = 0;
    p->remote_addr = 0;
    p->addr = 0;
    p->rozofs_key.s.ctx_idx =  freeslot;
    rozofs_shm_count++;
    p->rozofs_key.s.alloc_idx = p->rozofs_key.s.alloc_idx+1;
    
    return p;

error:      
    return NULL;
}


/*
**_________________________________________________________________________________________________
*/
/**
   Register a shared memory
   
   @param name: name of the shared memory
   @param remote_addr: remote address of the owner
   @param owner: reference of the socket owner
   
   @retval >= 0 : rozofs key for using the shared memory
   @retval < 0: error on registration
   
*/
int rozofs_shm_register(char *name,void *remote_addr,int owner,size_t length,int fd)
{
    int i;
    int nb_entries = 0;
    int freeslot = -1;
    rozofs_memreg_t *p = &rozofs_shm_tb[0];
 
    
    if (rozofs_shm_count == 0) freeslot = 0;
    if (rozofs_shm_count != 0)
    {
      for (i = 0; i < ROZOFS_SHARED_MEMORY_REG_MAX; p++)
      {
	if (p->name == NULL) {
	    if (freeslot < 0) freeslot = i;
	    continue;
	}
	nb_entries++;
	if (strcmp(p->name,name) == 0)
	{
          errno=EEXIST;
	  goto error;
	}
	if (rozofs_shm_count == nb_entries)
	{
	  freeslot = i+1;
	  break;
	}
      }
    }
    if ((freeslot < 0) ||( freeslot >= ROZOFS_SHARED_MEMORY_REG_MAX))
    {
      errno = ENOMEM;
      goto error;
    }
    p = &rozofs_shm_tb[freeslot];
    p->name = strdup(name);
    p->fd = fd;
    rozofs_shm_count++;
    
    p->addr = rozofs_shm_create(p,0); 
    p->rozofs_key.s.ctx_idx =  freeslot;
    p->rozofs_key.s.alloc_idx = p->rozofs_key.s.alloc_idx+1;
    
    if (p->addr != NULL)
    {
      p->remote_addr = remote_addr;
      p->owner = owner;
      /*
      ** it the current module is rozofsmount : we register the information in its shared memory
      */
      if (rozofs_bt_mem_owner_is_rozofsmount) rozofs_bt_add_memreg(p);
      return (int) p->rozofs_key.u32;
    }
    free(p->name);
    p->name = NULL;
    rozofs_shm_count--;
error: 
    return -1;    
}



/*
**_________________________________________________________________________________________________
*/
/**
   Register a shared memory on the storcli side
   
   @param rozofs_key: key of the shared memory
   @param name: name of the shared memory
   @param remote_addr: remote address of the owner
   @param length: length of the shared memory
   
   @retval >= 0 : rozofs key for using the shared memory
   @retval < 0: error on registration
   
*/
int rozofs_shm_stc_register(rozofs_mem_key rozofs_key,char *name,void *remote_addr,size_t length)
{

    rozofs_memreg_t *p = &rozofs_shm_tb[0];
    
    p = p+rozofs_key.s.ctx_idx;
    /*
    ** check if the shared memory must not be deleted before doing a new registration
    */
    if (p->fd_share != 0)
    {
      munmap(p->addr,p->length);
      close(p->fd_share);
      p->fd_share = 0;
      shm_unlink(p->name);
      info("sharemem /dev/shm/%s disconnected !!",p->name);
      p->fd= -1;
      rozofs_shm_count--;
      free(p->name);
      p->name = NULL;
    }
    
    p->name = strdup(name);
    rozofs_shm_count++;
    
    p->addr = rozofs_shm_create(p,0); 
    p->rozofs_key.u32 =  rozofs_key.u32;
    
    if (p->addr != NULL)
    {
      p->remote_addr = remote_addr;
      p->owner = 0;
      /*
      ** it the current module is rozofsmount : we register the information in its shared memory
      */
      if (rozofs_bt_mem_owner_is_rozofsmount) rozofs_bt_add_memreg(p);
      return (int) p->rozofs_key.u32;
    }
    free(p->name);
    p->name = NULL;
    rozofs_shm_count--;

    return -1;    
}


/*
**_________________________________________________________________________________________________
*/
/**
   get a shared memory context
   
   @param key: name of the shared memory

   
   @retval <>NULL : pointer to the share memory context
   @retval NULL: not found (see errno for details)
   
*/
rozofs_memreg_t *rozofs_shm_lookup(uint32_t key)
{
  rozofs_memreg_t  *p;
  rozofs_mem_key rozofs_key;
  
  rozofs_key.u32 = key;
  
  if (rozofs_key.s.ctx_idx >= ROZOFS_SHARED_MEMORY_REG_MAX)
  {
     errno = ERANGE;
     return NULL;
  }
  p = &rozofs_shm_tb[rozofs_key.s.ctx_idx];
  if (p->rozofs_key.s.alloc_idx != rozofs_key.s.alloc_idx)
  {
    errno = ENOENT;
    return NULL;
  }
  return p;
}


/**
**_______________________________________________________________________________________
*/
/**
*   Registration of a memory that has been created by the client (ROZO_BATCH_MEMREG)

    @param recv_buf: buffer that contains the command
    @paral socket_id: file descriptor used by the client on the AF_UNIX socket created for processing the requests
    
    @retval none
*/
void rozofs_bt_process_memreg(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   rozo_memreg_cmd_t *cmd_p;
   int i;
   int ret;
   rozofs_bt_rsp_buf response;
   

   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_memreg_cmd_t*)(hdr_p+1);
   memcpy(&response.hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
       pthread_rwlock_wrlock(&rozofs_bt_shm_lock); 
       ret = rozofs_shm_register(cmd_p->name,cmd_p->buf,0,cmd_p->length,socket_id);
      pthread_rwlock_unlock(&rozofs_bt_shm_lock); 
      if (ret < 0)
      {
        response.res[i].data = cmd_p->data;
	response.res[i].status = -1;
	response.res[i].size = errno;
      }
      else
      {
        response.res[i].data = cmd_p->data;
	response.res[i].status = 0;
	response.res[i].size = ret;
      }   
   }
   response.hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_memreg_cmd_t);
   errno = 0;
   ret = send(socket_id,&response,response.hdr.msg_sz+sizeof(uint32_t),0);
   ruc_buf_freeBuffer(recv_buf);
} 

/**
**_______________________________________________________________________________________
*/
int rozofs_bt_memcreate(char *name,int owner,size_t length,int fd)
{

   rozofs_memreg_t *p;

   
   p = rozofs_shm_context_alloc(name);
   if (p == NULL) return -1;
   /*
   ** set the reference of the socket
   */
   p->fd = fd;
   /*
   ** OK , now create the shared memory
   */
   /*
   **  prepare the context
   */
   p->length = length;
   p->owner = owner;
   p->addr = 0;
   p->remote_addr = 0;
   
   p->addr = rozofs_shm_create(p,1);
   if (p->addr == NULL)
   {

     rozofs_shm_count--;
     if (p->name) free(p->name);
     p->name = NULL;
     return -1;
   }
   return (int) p->rozofs_key.u32; 
}
/**
**_______________________________________________________________________________________
*/

/**
*   Creation of a shared memory for a client (ROZO_BATCH_MEMCREATE)

    @param recv_buf: buffer that contains the command
    @paral socket_id: file descriptor used by the client on the AF_UNIX socket created for processing the requests
    
    @retval none
*/
void rozofs_bt_process_memcreate(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   rozo_memreg_cmd_t *cmd_p;
   int i;
   int ret;
   rozofs_bt_rsp_buf response;
   

   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_memreg_cmd_t*)(hdr_p+1);
   memcpy(&response.hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
      pthread_rwlock_wrlock(&rozofs_bt_shm_lock); 
      ret = rozofs_bt_memcreate(cmd_p->name,0,cmd_p->length,socket_id);
      pthread_rwlock_unlock(&rozofs_bt_shm_lock); 
      if (ret < 0)
      {
        response.res[i].data = cmd_p->data;
	response.res[i].status = -1;
	response.res[i].size = errno;
      }
      else
      {
        response.res[i].data = cmd_p->data;
	response.res[i].status = 0;
	response.res[i].size = ret;
      }   
   }
   response.hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_memreg_cmd_t);
   errno = 0;
   ret = send(socket_id,&response,response.hdr.msg_sz+sizeof(uint32_t),0);
   ruc_buf_freeBuffer(recv_buf);
}     

/**
**_______________________________________________________________________________________
*/   
int rozofs_bt_mem_register_addr(rozo_memreg_cmd_t *cmd_p)
{
     rozofs_memreg_t *mem_p;

     mem_p = rozofs_shm_lookup(cmd_p->rozofs_shm_ref);
     if (mem_p == NULL)
     {
       errno = ENOENT;
       return -1;
     }
     mem_p->remote_addr = cmd_p->buf;
     /*
     ** it the current module is rozofsmount : we register the information in its shared memory
     */
     if (rozofs_bt_mem_owner_is_rozofsmount) rozofs_bt_add_memreg(mem_p);
     return (int) mem_p->rozofs_key.u32;;
}
/**
**_______________________________________________________________________________________
*/

/**
*   registration of the address used on the client side (ROZO_BATCH_MEMADDR_REG)

    @param recv_buf: buffer that contains the command
    @paral socket_id: file descriptor used by the client on the AF_UNIX socket created for processing the requests
    
    @retval none
*/
void rozofs_bt_process_mem_register_addr(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   rozo_memreg_cmd_t *cmd_p;
   int i;
   int ret;
   rozofs_bt_rsp_buf response;
   

   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_memreg_cmd_t*)(hdr_p+1);
   memcpy(&response.hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
      ret = rozofs_bt_mem_register_addr(cmd_p);
      if (ret < 0)
      {
        response.res[i].data = cmd_p->data;
	response.res[i].status = -1;
	response.res[i].size = errno;
      }
      else
      {
        response.res[i].data = cmd_p->data;
	response.res[i].status = 0;
	response.res[i].size = ret;
      }   
   }
   response.hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_memreg_cmd_t);
   errno = 0;
   ret = send(socket_id,&response,response.hdr.msg_sz+sizeof(uint32_t),0);
   ruc_buf_freeBuffer(recv_buf);
}    



/**
**_______________________________________________________________________________________
*/
void rozofs_bt_process_memcreate_from_main_thread(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   
   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   hdr_p->private = (uint64_t)socket_id;
   
   rozofs_queue_put(&rozofs_bt_rq_queue,recv_buf);
}


/**
**_______________________________________________________________________________________
*/
void rozofs_bt_process_memreg_from_main_thread(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   
   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   hdr_p->private = (uint64_t)socket_id;
   
   rozofs_queue_put(&rozofs_bt_rq_queue,recv_buf);
}

/*
**______________________________________________________________________________________________
*/
int rozofs_bt_add_memreg(rozofs_memreg_t *mem_p)
{
   rozofs_memreg_t *p = rozofs_bt_stc_mem_p;
   rozofs_stc_memreg_t *q;
   if (p == NULL)
   {
     severe("no shared memory between rozofsmount and storcli\n");
     return -1;
   }
   q = (rozofs_stc_memreg_t*) p->addr;
   q = q+mem_p->rozofs_key.s.ctx_idx;
   
   strcpy(q->name,mem_p->name);
   q->rozofs_key.u32 = mem_p->rozofs_key.u32;
   q->length = mem_p->length;
   q->remote_addr = mem_p->remote_addr;
   
   return 0;
   
}
/*
**______________________________________________________________________________________________
*/
/** Create shared memory used to handle the registration of user shared memory by storcli

    @param rozofsmount_id: instance number of rozofsmount
    @param create: assert to 1 for create (rozofsmount:0/storcli:1)
    
    @retval 0 on success
    @retval -1 on error
*/
int rozofs_bt_create_memreg_mgt(int rozofsmount_id,int create)
{
   char filename[128];
   int ret;
   int flags = O_RDWR;
   void *p;   
   struct stat sb;   
   size_t length;

   rozofs_memreg_t *mem_p = malloc(sizeof(rozofs_memreg_t));
   if (mem_p == NULL)
   {
     return -1;
   }
   memset(mem_p,0,sizeof(rozofs_memreg_t));
   sprintf(filename,"/ROZOFS_BT_MEM_EXC_%d",rozofsmount_id);
   mem_p->name = strdup(filename);
   mem_p->length = sizeof(rozofs_stc_memreg_t)*ROZOFS_SHARED_MEMORY_REG_MAX;
   if (create)
   {
      ret =shm_unlink(filename);
      if (ret < 0)
      {
        if(errno != ENOENT)
	{
          severe("error of shared memory creation:%s:%s\n",mem_p->name,strerror(errno));
	  return -1;
	}
      }
   }
   if (create) flags = flags | O_CREAT;
   ret = shm_open(filename,flags,0644);     
   if (ret < 0)
   {
     severe("error of shared memory creation:%s:%s\n",mem_p->name,strerror(errno));
     return -1;
   }
   if (create)
   {
     if (ftruncate (ret, mem_p->length) == -1) {
      severe ("ftruncate failure for %s : %s",mem_p->name,strerror(errno));
      close(ret);
      return -1;
     } 
     length = mem_p->length;
   } 
   else
   {

     if (fstat (ret, &sb) == -1) {
       printf ("fstat failure for %s : %s",mem_p->name,strerror(errno));
       close(ret);
       return -1;
     }
     length = sb.st_size;   
   
   } 
   p = mmap (0, length, PROT_READ|PROT_WRITE, MAP_SHARED, ret, 0);
   if (p == MAP_FAILED) {
           severe ("map failure for %s : %s",mem_p->name,strerror(errno));
	   close(ret);
           return -1;
   }
   if (create)  memset(p,0,length);
   /*
   ** fill up the context
   */
   mem_p->addr = p;
   mem_p->length =length;
   mem_p->fd_share = ret;
   
   rozofs_bt_stc_mem_p = mem_p;

   return 0;


}


/*
**_________________________________________________________________________________________________
*/
/**
*   Init of the array that maintain the shared memory that have been registered with rozofsmount

    @param rozofsmount_id: rozofsmount instance
    @param create: assert to 1 for creation of the shared memory, 0 for just a registration
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_shm_init(int rozofsmount_id,int create)
{
  int ret;
  
  pthread_rwlock_init(&rozofs_bt_shm_lock, NULL);
  rozofs_shm_count = 0;
  rozofs_shm_tb = malloc(sizeof(rozofs_memreg_t)*ROZOFS_SHARED_MEMORY_REG_MAX);
  if (rozofs_shm_tb == NULL) return -1;
  memset(rozofs_shm_tb,0,sizeof(rozofs_memreg_t)*ROZOFS_SHARED_MEMORY_REG_MAX);
  uma_dbg_addTopic("bt_shmem", rozofs_bt_shmem_display);

  
  ret = rozofs_bt_create_memreg_mgt(  rozofsmount_id,create);
  if (create) rozofs_bt_mem_owner_is_rozofsmount =1;
  
    if (rozofs_bt_mem_owner_is_rozofsmount) uma_dbg_addTopic("bt_stc_shmem",rozofs_bt_stc_shmem_display);
  return ret;

} 
