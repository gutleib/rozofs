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
#include <rozofs/common/export_track.h>
#include <rozofs/rozofs.h>
#include "mdirent.h"

typedef uuid_t fid_t; /**< file id */
unsigned int bucket = -1;
unsigned int scoll_idx = -1;
unsigned int rcoll_idx = -1;
unsigned int display_chunks = 0;


static void usage() {
    printf("zoro_compute_hashes <pfid> <name> \n");
    exit(0);

}
;
/*_______________________________________________________________________
 */
int main(int argc, char **argv) {
    fid_t fid_parent;
    uint32_t hash1;
    uint32_t hash2;
    int      len;
    int      bucket_idx;
    

    if (argc < 3) usage();
    
    if (rozofs_uuid_parse(argv[1], fid_parent) != 0) {
      printf("Bad input parent fid \"%s\"\n",argv[1]);
      usage();
    }  

    /*
    ** deassert de delete pending bit of the parent
    */
    exp_metadata_inode_del_deassert(fid_parent);
    rozofs_inode_set_dir(fid_parent);

    /*
    ** compute the hash of the entry to search
    */
    hash1 = filename_uuid_hash_fnv(0, argv[2], fid_parent, &hash2, &len);
    bucket_idx = ((hash2 >> 16) ^ (hash2 & 0xffff)) & 0xFF;

    printf("H1         : 0x%x\n",hash1);
    printf("H2         : 0x%x\n",hash2);
    printf("root idx   : %d / %d / %d \n",hash1 & 1, hash1 & 0xF, hash1 & 0xFFF);
    printf("bucket idx : %d\n", bucket_idx);
    printf("H3         : 0x%x\n", hash2 & DIRENT_ENTRY_HASH_MASK);
    exit(0);
}
