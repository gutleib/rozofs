#define _XOPEN_SOURCE 500

#include <unistd.h>

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <stdlib.h>

#include <rozofs/core/rozofs_string.h>
#include "mdirent.h"

typedef uuid_t fid_t; /**< file id */
unsigned int bucket = -1;
unsigned int scoll_idx = -1;
unsigned int rcoll_idx = -1;
unsigned int display_chunks = 0;
char *       input_dir = NULL;

/*
 **______________________________________________________________________________
 */
static void usage() {
    printf("zoro_read_dirent_file [-b <bucket #>] [-d <dir>] [-c] <file1> [ <file2> ] \n");
    printf(" [ -b <bucket #> ]        A specific bucket number to display (default all)\n");
    printf(" [ -c ]                   To display chunk information\n");
    printf(" [ -d < dir> ]            Prepand <dir> to fle names\n");    
    printf(" [ -scoll <coll_idx> ]    assert presence of collision file <coll_idx>\n");
    printf(" [ -rcoll <coll_idx> ]    reset presence of collision file <coll_idx>\n");
    exit(0);

}
/*
 **______________________________________________________________________________
 */
int read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;

    if (argc <= 1) usage();

    idx = 1;
    while (idx < argc) {

        /* -b <bucket number> */
        if (strcmp(argv[idx], "-b") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx - 1]);
                usage();
            }
            ret = sscanf(argv[idx], "%d", &bucket);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx - 1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }

        /* -scoll <coll_idx> */
        if (strcmp(argv[idx], "-scoll") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx - 1]);
                usage();
            }
            ret = sscanf(argv[idx], "%d", &scoll_idx);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx - 1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }

        /* -rcoll <coll_idx> */
        if (strcmp(argv[idx], "-rcoll") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx - 1]);
                usage();
            }
            ret = sscanf(argv[idx], "%d", &rcoll_idx);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx - 1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }

        /* -c */
        if (strcmp(argv[idx], "-c") == 0) {
            idx++;
            display_chunks = 1;
            continue;
        }

        /* -d */
        if (strcmp(argv[idx], "-d") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx - 1]);
                usage();
            }
            input_dir = argv[idx];
            idx++;
            continue;
        }

        return idx;

        usage();
    }

    return 0;
}

/*
 **______________________________________________________________________________
 */
char formated_type[32];
char *print_next(mdirents_hash_ptr_t *hash_bucket_p) {

    if (hash_bucket_p->type == MDIRENTS_HASH_PTR_EOF) {
      sprintf(formated_type,"E   ");
      return formated_type;
    }  
    if (hash_bucket_p->type == MDIRENTS_HASH_PTR_LOCAL) {
      sprintf(formated_type,"L%3d",hash_bucket_p->idx);
      return formated_type;
    }  
    if (hash_bucket_p->type == MDIRENTS_HASH_PTR_COLL) {
      sprintf(formated_type,"C%3d",hash_bucket_p->idx);
      return formated_type;
    }  
    if (hash_bucket_p->type == MDIRENTS_HASH_PTR_FREE) {
      sprintf(formated_type,"F%3d",hash_bucket_p->idx);
      return formated_type;
    }
    return "UNKN";
}
/*
 **______________________________________________________________________________
 */
void print_hash_entry(mdirents_hash_entry_t *hash_entry_p) {
  int    bucket_idx = DIRENT_HASH_ENTRY_GET_BUCKET_IDX(hash_entry_p);
  
  printf(" B%3d H%7.7x C%3d/%d %4s ", bucket_idx, hash_entry_p->hash,
           hash_entry_p->chunk_idx, hash_entry_p->nb_chunk,
           print_next(&hash_entry_p->next));
} 

/*
 **______________________________________________________________________________
 */
char chunk_buff[8192];
char out_buff[256];
void print_chunk(int fd, int offset,mdirents_hash_entry_t *hash_entry_p) {
  int i;
  int ret;
  char * pChar = out_buff;
  char * pName;
  int chunk_nb = hash_entry_p->chunk_idx;
  int chunk_sz = hash_entry_p->nb_chunk;
  if (chunk_sz==0) return;
  
  chunk_sz *= MDIRENTS_NAME_CHUNK_SZ;
  
  offset *= MDIRENT_SECTOR_SIZE;
  offset += chunk_nb * MDIRENTS_NAME_CHUNK_SZ;
  pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, chunk_nb);
  pChar += rozofs_string_append(pChar, " offset 0x");
  pChar += rozofs_x32_append(pChar, offset);
  *pChar++ = '/';
  pChar += rozofs_u32_append(pChar, chunk_sz);
  pChar += rozofs_string_append(pChar, " NAME(");

  ret = pread(fd, chunk_buff, chunk_sz, offset);
  if (ret < 0) {
      /*
       ** need to figure out what need to be done since this might block a chunk of file
       */
      printf("pread chunk %d failed offset %d size %d: %s\n", 
              chunk_nb, offset, chunk_sz,
             strerror(errno));
      return;
  }
  
  mdirents_name_entry_t * name_entry = (mdirents_name_entry_t*) chunk_buff;

  pChar += rozofs_u32_append(pChar, name_entry->len);
  pChar += rozofs_string_append(pChar, ") \"");
  
  pName = name_entry->name;
  for (i=0; i<name_entry->len; i++) {
    *pChar++ = *pName++;
  }
  pChar += rozofs_string_append(pChar,"\" FID ");
  pChar += rozofs_fid_append(pChar,name_entry->fid);
  pChar += rozofs_string_append(pChar," mode ");
  pChar += rozofs_mode2String(pChar,name_entry->type);
  *pChar++ = '\n';
  printf("%s",out_buff);
  return;
}  
/*
 **______________________________________________________________________________
 * Read the mdirents file on disk
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *pathname: pointer to the pathname to read
 *
 * @retval NULL if this mdirents file doesn't exist
 * @retval pointer to the mdirents file
 */
void set_coll_idx(int coll_idx, char *pathname, int set) {
    int fd = -1;
    int flag = O_RDWR;
    char *path_p;
    off_t offset;
    mdirents_file_t *dirent_file_p = NULL;
    mdirent_sector0_not_aligned_t *sect0_p = NULL;
    mdirents_btmap_coll_dirent_t *coll_bitmap_p = NULL;
    int ret;
    uint32_t u8, idx;

    /*
     ** clear errno
     */
    errno = 0;

    /*
     ** build the filename of the dirent file to read
     */
    path_p = pathname;

    if ((fd = open(path_p, flag, S_IRWXU)) == -1) {
        //printf("Cannot open the file %s, error %s at line %d\n",path_p,strerror(errno),__LINE__);
        /*
         ** check if the file exists. If might be possible that the file does not exist, it can be considered
         ** as a normal error since the exportd might have crashed just after the deletion of the collision dirent
         ** file but before  the update of the dirent root file.
         */
        if (errno == ENOENT) {
            goto out;

        }
        /*
         ** fatal error on file opening
         */

        printf("Cannot open the file %s, error %s at line %d\n", path_p, strerror(errno), __LINE__);
        errno = EIO;
        goto out;
    }
    /*
     ** allocate a working array for storing the content of the file except the part
     ** that contains the name/fid and mode
     */
    dirent_file_p = malloc(sizeof (mdirents_file_t))
            ;
    if (dirent_file_p == NULL) {
        /*
         ** the system runs out of memory
         */
        errno = ENOMEM;
        printf("Out of Memory at line %d", __LINE__)
                ;
        goto error;
    }
    /*
     ** read the fixed part of the dirent file
     */
    offset = DIRENT_HEADER_BASE_SECTOR * MDIRENT_SECTOR_SIZE;
    ret = pread(fd, dirent_file_p, sizeof (mdirents_file_t), offset);
    if (ret < 0) {
        /*
         ** need to figure out what need to be done since this might block a chunk of file
         */
        printf("pread failed in file %s: %s", pathname, strerror(errno));
        errno = EIO;
        goto error;

    }
    if (ret != sizeof (mdirents_file_t)) {

        /*
         ** we consider that error as the case of the file that does not exist. By ignoring that file
         ** we just lose potentially one file at amx
         */
        printf("incomplete pread in file %s %d (expected: %d)", pathname, ret, (int) sizeof (mdirents_file_t));
        errno = ENOENT;
        goto error;
    }


    sect0_p = (mdirent_sector0_not_aligned_t *) & dirent_file_p->sect0;
    coll_bitmap_p = &sect0_p->coll_bitmap;

    u8 = coll_idx / 8;
    idx = coll_idx % 8;

    if (set) {
        coll_bitmap_p->bitmap[u8] &= ~(1 << idx);
    } else {
        coll_bitmap_p->bitmap[u8] |= (1 << idx);
    }

    ret = pwrite(fd, dirent_file_p, sizeof (mdirents_file_t), offset);
    if (ret < 0) {
        /*
         ** need to figure out what need to be done since this might block a chunk of file
         */
        printf("pwrite failed in file %s: %s", pathname, strerror(errno));
        errno = EIO;
        goto error;
    }


out:
    if (fd != -1)
        close(fd);
    if (dirent_file_p != NULL)
        free(dirent_file_p);
    return;

error:
    if (dirent_file_p != NULL)
        free(dirent_file_p);
    if (fd != -1)
        close(fd);
    return;
}
/*
 **______________________________________________________________________________
 */

/**
 * Read the mdirents file on disk
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *pathname: pointer to the pathname to read
 *
 * @retval NULL if this mdirents file doesn't exist
 * @retval pointer to the mdirents file
 */
void do_read_mdirents_file(char *pathname) {
    int fd = -1;
    int flag = O_RDONLY;
    off_t offset;
    mdirents_file_t *dirent_file_p = NULL;
    //mdirent_sector0_not_aligned_t *sect0_p = NULL;
    int ret;
    mdirents_header_new_t * header;
    char    filename[1024];
    char  * path_p ;

    /*
     ** clear errno
     */
    errno = 0;

    /*
    ** build the filename of the dirent file to read
    */
    if (input_dir) {
      path_p = filename;
      sprintf(filename,"%s/%s", input_dir, pathname);
    }
    else {
      path_p = pathname;
    }
    printf("___________________________________________________\n%s\n___________________________________________________\n", path_p);
     
    if ((fd = open(path_p, flag, S_IRWXU)) == -1) {
        //printf("Cannot open the file %s, error %s at line %d\n",path_p,strerror(errno),__LINE__);
        /*
         ** check if the file exists. If might be possible that the file does not exist, it can be considered
         ** as a normal error since the exportd might have crashed just after the deletion of the collision dirent
         ** file but before  the update of the dirent root file.
         */
        if (errno == ENOENT) {
            printf("No such file : %s !!!\n\n", path_p);
            goto out;

        }
        /*
         ** fatal error on file opening
         */

        printf("Cannot open the file %s, error %s at line %d\n", path_p, strerror(errno), __LINE__);
        errno = EIO;
        goto out;
    }
    /*
     ** allocate a working array for storing the content of the file except the part
     ** that contains the name/fid and mode
     */
    dirent_file_p = malloc(sizeof (mdirents_file_t))
            ;
    if (dirent_file_p == NULL) {
        /*
         ** the system runs out of memory
         */
        errno = ENOMEM;
        printf("Out of Memory at line %d", __LINE__)
                ;
        goto error;
    }
    /*
     ** read the fixed part of the dirent file
     */
    offset = DIRENT_HEADER_BASE_SECTOR * MDIRENT_SECTOR_SIZE;
    ret = pread(fd, dirent_file_p, sizeof (mdirents_file_t), offset);
    if (ret < 0) {
        /*
         ** need to figure out what need to be done since this might block a chunk of file
         */
        printf("pread failed in file %s: %s", path_p, strerror(errno));
        errno = EIO;
        goto error;

    }
    if (ret != sizeof (mdirents_file_t)) {

        /*
         ** we consider that error as the case of the file that does not exist. By ignoring that file
         ** we just lose potentially one file at amx
         */
        printf("incomplete pread in file %s %d (expected: %d)", path_p, ret, (int) sizeof (mdirents_file_t));
        errno = ENOENT;
        goto error;
    }

    header = (mdirents_header_new_t*)dirent_file_p;

    /*
     ** sector 2  :
     ** Go through the hash table bucket an allocated memory for buckets that are no empty:
     **  there are   256 hash buckets of 16 bits
     */
    {
        int i;
        int j = 1000;

        mdirents_hash_ptr_t *hash_bucket_p = (mdirents_hash_ptr_t*) & dirent_file_p->sect2;
        printf("BUCKETS:\n");
        if (bucket == -1) {
            printf("    |   0  |   1  |  2   |  3   |  4   |  5   |  6   |  7   |  8   |  9   |");
            for (i = 0; i < MDIRENTS_HASH_TB_INT_SZ; i++, hash_bucket_p++) {
                if (j > 9) {
                    printf("\n%3d |", i);
                    j = 0;
                }
                j++;
                printf(" %4s |", print_next(hash_bucket_p));
            }
        } else {
            printf(" %3d %4s\n", bucket, print_next(&hash_bucket_p[bucket]));
        }
    }
    /*
     ** sector 3 : hash entries
     */

    {
        int i;
        int j = 1000;
        printf("\n...............................................................\nHASH ENTRIES:\n");

        mdirents_hash_entry_t *hash_entry_p = (mdirents_hash_entry_t*) & dirent_file_p->sect3;
        int bucket_idx;
        if (bucket == -1) {
            for (i = 0; i < MDIRENTS_ENTRIES_COUNT; i++, hash_entry_p++) {
                if (j > 4) {
                    printf("\n%3d |", i);
                    j = 0;
                }
                j++;
                print_hash_entry(hash_entry_p);
                printf("|");
            }
        } else {
            for (i = 0; i < MDIRENTS_ENTRIES_COUNT; i++, hash_entry_p++) {
                bucket_idx = DIRENT_HASH_ENTRY_GET_BUCKET_IDX(hash_entry_p);
                if (bucket_idx != bucket) continue;
                printf("%3d",i);
                print_hash_entry(hash_entry_p);
                printf("\n");

            }
        }
        printf("\n");
    }
    
    /*
    ** Chunks
    */
    if (display_chunks) {
        int i;
        printf("\n...............................................................\nCHUNKS:\n");
        
        mdirents_hash_entry_t *hash_entry_p = (mdirents_hash_entry_t*) & dirent_file_p->sect3;
        int bucket_idx;
        if (bucket == -1) {
            for (i = 0; i < MDIRENTS_ENTRIES_COUNT; i++, hash_entry_p++) {
                print_chunk(fd,header->sector_offset_of_name_entry, hash_entry_p);
            }
        } 
        else {
            for (i = 0; i < MDIRENTS_ENTRIES_COUNT; i++, hash_entry_p++) {
                bucket_idx = DIRENT_HASH_ENTRY_GET_BUCKET_IDX(hash_entry_p);
                if (bucket_idx != bucket) continue;
                print_chunk(fd,header->sector_offset_of_name_entry, hash_entry_p);
            }
        }
        printf("\n");     
    }

out:
    if (fd != -1)
        close(fd);
    if (dirent_file_p != NULL)
        free(dirent_file_p);
    return;

error:
    if (dirent_file_p != NULL)
        free(dirent_file_p);
    if (fd != -1)
        close(fd);
    return;
}

/*_______________________________________________________________________
 */
int main(int argc, char **argv) {
    int idx;

    idx = read_parameters(argc, argv);

    if (scoll_idx != -1) {
        set_coll_idx(scoll_idx, argv[idx], 1);
        exit(0);
    }

    if (rcoll_idx != -1) {
        set_coll_idx(rcoll_idx, argv[idx], 0);
        exit(0);
    }


    while (idx < argc) {
        do_read_mdirents_file(argv[idx]);
        idx++;
    }

    exit(0);
}
