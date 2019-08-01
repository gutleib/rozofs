#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <unistd.h>

#define MAX_LINE_LEN 4096

typedef struct list {
    struct list *next, *prev;
} list_t;

static inline void list_init(list_t * list) {
    list->next = list;
    list->prev = list;
}

static inline void list_insert(list_t * new, list_t * prev, list_t * next) {
    next->prev = new;
    new->next = next;
    new->prev = prev;
    prev->next = new;
}

static inline void list_push_front(list_t * head, list_t * new) {
    list_insert(new, head, head->next);
}

static inline void list_push_back(list_t * head, list_t * new) {
    list_insert(new, head->prev, head);
}

static inline void list_remove(list_t * list) {
    list->next->prev = list->prev;
    list->prev->next = list->next;
    list->next = (void *) list;
    list->prev = (void *) list;
}

static inline int list_empty(list_t * head) {
    return head->next == head;
}



#define list_entry(ptr, type, member) \
	((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

#define list_first_entry(ptr, type, member) \
        list_entry((ptr)->next, type, member)

#define list_last_entry(head, type, member) \
        ((head)->prev==(head))? NULL : list_entry((head)->prev, type, member);
#define list_1rst_entry(head, type, member) \
        ((head)->prev==(head))? NULL : list_entry((head)->next, type, member);
	
#define list_for_each_forward(pos, head) \
	for (pos = (head)->next; pos != (head); pos = pos->next)

#define list_for_each_backward(pos, head) \
	for (pos = (head)->prev; pos != (head); pos = pos->prev)

#define list_for_each_forward_safe(pos, n, head)\
    for (pos = (head)->next, n = pos->next; pos != (head); \
    	pos = n, n = pos->next)

#define list_for_each_backward_safe(pos, n, head)\
    for (pos = (head)->prev, n = pos->prev; pos != (head); \
    	pos = n, n = pos->prev)


#define BUCKET_NB  (64*1024)
list_t bucket[BUCKET_NB];

typedef struct _dir_entry_t {
  list_t   list;
  char   * name;
  int      name_len;
  int      success;
  int      failures;
} dir_entry_t;

#define ERROR(fmt, ...) printf(fmt, ##__VA_ARGS__)

/*-----------------------------------------------------------------------------
**
**  FNV hash compute
**
**----------------------------------------------------------------------------
*/
static inline uint32_t bucket_hash_fnv(void * name, int len) {
    uint32_t hash;
    unsigned char *d = (unsigned char *) name;
    int i = 0;

    hash = 2166136261U;
    for (i = 0; i < len; d++, i++) {
        hash = (hash * 16777619)^ *d;
    }

    return hash % BUCKET_NB;
}
/*-----------------------------------------------------------------------------
**
**  Allocate a new directory entry
**
**----------------------------------------------------------------------------
*/
static inline dir_entry_t * allocate_dir_entry(char * name, int name_len, int ex) {
  dir_entry_t * entry;
  
  entry = malloc(sizeof(dir_entry_t));
  if (entry == NULL) {
    ERROR("Out of memory\n");
    return NULL;
  }
  
  list_init(&entry->list);
  entry->name = strdup(name);
  if (entry->name == NULL) {
    ERROR("Out of memory\n");
    return NULL;
  }
  entry->name_len = name_len;
  if (ex == 0) {
    entry->success = 1;
    entry->failures = 0;
  }
  else {
    entry->success = 0;
    entry->failures = 1;
  }
  return entry;    
}
/*-----------------------------------------------------------------------------
**
**  Lookup for an entry in the cache
**
**----------------------------------------------------------------------------
*/
static inline dir_entry_t * lookup_entry(uint32_t hash, char * name, int name_len) {
  dir_entry_t * entry;
  list_t      * p;
  
  list_for_each_forward(p, &bucket[hash]) {
    entry = (dir_entry_t *) p;
    if (entry->name_len != name_len) continue;
    if (strcmp(entry->name, name)==0) return entry;
  } 
  return NULL;
}
/*-----------------------------------------------------------------------------
**
**  Lookup for an entry in the cache
**
**----------------------------------------------------------------------------
*/
static void delete_entry(dir_entry_t * entry) {
  free(entry->name);
  list_remove(&entry->list);
  free(entry);
}
/*-----------------------------------------------------------------------------
**
**  Add a directory entry in the hash table
**
**----------------------------------------------------------------------------
*/
static inline dir_entry_t * add_entry(char * name, int ex) {
  dir_entry_t * entry;
  uint32_t     hash;
  int          name_len;
  
  /*
  ** Compute the hah from the file name 
  */
  name_len = strlen(name);
  if (name_len==0) return (dir_entry_t *)0x1;
  
  hash = bucket_hash_fnv(name, name_len);
  
  /*
  ** Lookup for the directory in the cache
  */
  entry = lookup_entry(hash, name, name_len);

  /*
  ** Found 
  */
  if (entry) {
    if (ex == 0) {
      entry->success = 1;
    }
    else {
      entry->failures++;
    }
    return entry;
  }
  
  /*
  ** Not found
  */
  entry = allocate_dir_entry(name, name_len, ex);   
  if (entry == NULL) {
    ERROR("allocate_dir_entry\n");
    return NULL;
  }
  list_push_front(&bucket[hash], &entry->list); 
  return entry;
}
/*-----------------------------------------------------------------------------
**
**  Display usage
**
**----------------------------------------------------------------------------
*/
void syntax(char * fmt, ...) {
  va_list   args;
  char      error_buffer[512];
  
  /*
  ** Display optionnal error message if any
  */
  if (fmt) {
    va_start(args,fmt);
    vsprintf(error_buffer, fmt, args);
    va_end(args);   
    printf("%s\n",error_buffer);
  }
  
  /*
  ** Display usage
  */
//  printf("RozoFS  - %s\n", VERSION);
  printf("Usage: rozo_analyze_results <synchroPath> <//in> <//out> <failed> <remaining>\n\n");
  printf("  <synchroPath>   RozoFS synchronization directory full path.\n");
  printf("  <//in>          GNU parallel input file.\n");
  printf("  <//out>         GNU parallel output file.\n");
  printf("  <failed>        output file to write the failed directory list.\n");
  printf("  <remaining>     output file to write the remaining directory list.\n");
  
  if (fmt) exit(EXIT_FAILURE);
  exit(EXIT_SUCCESS); 
}

/*-----------------------------------------------------------------------------
**
**  M A I N
**
**----------------------------------------------------------------------------
*/
int main(int argc, char **argv) {
  FILE          * fout = NULL;
  FILE          * fin = NULL;
  char            line[MAX_LINE_LEN];
  char          * pline = line;
  size_t          len = MAX_LINE_LEN;
  ssize_t         read;
  int             seq;
  char            host[64];
  char            start[64];
  char            job[64];
  int             send;
  int             rec;
  int             ex;
  int             sig;
  char            cmd[128];
  char            dir1[1024];
  char            dir2[1024];
  char            dir3[1024];
  char            dir4[1024];
  char            dir5[1024];
  char            dir6[1024];
  char            dir7[1024];
  char            dir8[1024];
  int             i;
  dir_entry_t   * entry;
  uint32_t        hash;    
  FILE          * ffailed = NULL;
  FILE          * fremaining = NULL;
  int             success=0;
  int             failed=0;
  int             remaining=0;   
  
  /*
  ** Check the number of parameters
  */
  
  if (argc<6) {
    syntax("Missing parameters");
  }  
  
  for (i=0; i<BUCKET_NB;i++) {
    list_init(&bucket[i]);
  }
  
  /*
  ** Open outpout file. It contains on each line
  ** Seq	Host	Starttime	JobRuntime	Send	Receive	Exitval	Signal	Command
  ** rozofs-demo-07	1528113861.994	     0.179	0	0	0	0	/mnt/private/.rozo_synchro/eid1/process_synchro.sh ./dir2/dir1
  */
  fout = fopen(argv[3],"r");
  if (fout == NULL) {
    syntax("fopen(<//out>=%s) %s", argv[3], strerror(errno)); 
  }
  
  /*
  ** Open input file. It contains a directory on each line
  */
  fin = fopen(argv[2],"r");
  if (fout == NULL) {
    syntax("fopen(<//in>=%s) %s", argv[2], strerror(errno)); 
  }
  
  /*
  ** Parse //out file 
  */
  while ((read = getline(&pline, &len, fout)) != -1) {
    switch (sscanf(pline, "%d %s %s %s %d %d %d %d %s %s %s %s %s %s %s %s %s", &seq, host, start, job, &send, &rec, &ex, &sig, cmd, 
                           dir1, dir2, dir3, dir4, dir5, dir6, dir7, dir8)) {
       case 17:
         entry = add_entry(dir8,ex);
         if (entry==NULL) {
           exit(1);
         }       
       case 16:
         entry = add_entry(dir7,ex);
         if (entry==NULL) {
           exit(1);
         }       
       case 15:
         entry = add_entry(dir6,ex);
         if (entry==NULL) {
           exit(1);
         }       
       case 14:
         entry = add_entry(dir5,ex);
         if (entry==NULL) {
           exit(1);
         }       
       case 13:
         entry = add_entry(dir4,ex);
         if (entry==NULL) {
           exit(1);
         }       
       case 12:
         entry = add_entry(dir3,ex);
         if (entry==NULL) {
           exit(1);
         }       
       case 11:
         entry = add_entry(dir2,ex);
         if (entry==NULL) {
           exit(1);
         }       
       case 10:
         entry = add_entry(dir1,ex);
         if (entry==NULL) {
           exit(1);
         }       
         break;
       default:
         break;  
    } 
  }  
  
  unlink(argv[4]);
  ffailed = NULL;
  
  unlink(argv[5]);
  fremaining = NULL;

  /*
  ** Read //in file 
  */
  while ((read = getline(&pline, &len, fin)) != -1) {
    if (sscanf(pline, "%s", dir1)==1) {
      /*
      ** Compute the hah from the file name 
      */
      read = strlen(dir1);
      hash = bucket_hash_fnv(dir1, read);
      /*
      ** Lookup for the directory in the cache
      */
      entry = lookup_entry(hash, dir1, read);

      dir1[read] = '\n';
      read++;
      dir1[read] = 0;
      
      if (entry == NULL) {
        remaining++;
        if (fremaining == NULL) {
          fremaining = fopen(argv[5],"w");
          if (fremaining == NULL) {
            syntax("fopen(%s) %s", argv[5], strerror(errno));
          }
        }
        fwrite(dir1,read,1,fremaining);
        continue;
      }
      if (entry->success) { 
        success++;
      }
      else {
        failed++;      
        if (ffailed == NULL) {
          ffailed = fopen(argv[4],"w");
          if (ffailed == NULL) {
            syntax("fopen(%s) %s", argv[4], strerror(errno));
          }
        }
        fwrite(dir1,read,1,ffailed);
      }  
      delete_entry(entry);
    }  
  }   
  if (fremaining) fclose(fremaining);
  if (ffailed)    fclose(ffailed);
  fclose(fin);
  fclose(fout);      
  printf("success %d failed %d remaining %d\n",success,failed,remaining);
  exit(0);
}
