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

#ifndef _MDIR_H
#define _MDIR_H

#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>

/** API mdir management functions.
 * TODO
 */
#define MDIR_ATTRS_FNAME "attributes"

typedef struct mdir {
    int fdp;        ///< directory file descriptor
    int fdattrs;    ///< attributes file descriptor
} mdir_t;


/** Initialize the mdir structure
 *
 * @param mdir: the mdir structure to initialize
 */
static inline void mdir_init(mdir_t *mdir) {
  mdir->fdp     = -1;
  mdir->fdattrs = -1;
}
/** open the mdir
 *
 * @param mdir: the mdir to open
 * @param path: the corresponding path
 */
int mdir_open(mdir_t *mdir, const char *path);

/** close the mdir
 *
 * @param mdir: the mdir to close
 */
void mdir_close(mdir_t *mdir);

/** read attributes from the underlying directory file
 *
 * @param mdir: an opened lv2 mdir_t
 *
 * @return: 0 on success -1 otherwise (errno is set).
 */
int mdir_read_attributes(mdir_t *mdir, mattr_t *mattr);

/** write attributes to the underlying regular file
 *
 * @param mdir: an opened lv2 mdir_t
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int mdir_write_attributes(mdir_t *mdir, mattr_t *mattr);

/** set an extended attribute value for a lv2 directory.
 *
 * @param mdir: the mdir.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 *  with this extended attribute.
 * @param flags: parameter can be used to refine the semantics of the operation.
 * 
 * @return: On success, zero is returned.  On failure, -1 is returned.
 */
int mdir_set_xattr(mdir_t *mdir, const char *name, const void *value, size_t size, int flags);

/** retrieve an extended attribute value from the lv2 directory.
 *
 * @param mdir: the mdir to read from.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 *  with this extended attribute.
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t mdir_get_xattr(mdir_t *mdir, const char *name, void *value, size_t size);

/** remove an extended attribute from the lv2 directory.
 *
 * @param mdir: the mdir for this directory.
 * @param name: the extended attribute name.
 * 
 * @return: On success, zero is returned.  On failure, -1 is returned.
 */
int mdir_remove_xattr(mdir_t *mdir, const char *name);

/** list extended attribute names from the lv2 directory.
 *
 * @param mdir: the mdir for this directory.
 * @param list: list of extended attribute names associated with this directory.
 * @param size: the size of a buffer to hold the list of extended attributes.
 * 
 * @return: On success, the size of the extended attribute name list.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t mdir_list_xattr(mdir_t *mdir, char *list, size_t size);

#endif
