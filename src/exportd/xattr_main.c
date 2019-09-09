#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "rozofs_ext4.h"
#include "xattr.h"
#include "xattr_main.h"

export_tracking_table_t *xattr_trk_p;

static const char *
strcmp_prefix(const char *a, const char *a_prefix)
{
	while (*a_prefix && *a == *a_prefix) {
		a++;
		a_prefix++;
	}
	return *a_prefix ? NULL : a;
}


/**
* release the memory used for extended attributes storage

*/
void rozofs_xattr_extended_release(lv2_entry_t *inode)
{
   if (inode->extended_attr_p != NULL) 
   {
     free(inode->extended_attr_p);
     inode->extended_attr_p = NULL;
   }
}
/*
 * In order to implement different sets of xattr operations for each xattr
 * prefix with the generic xattr API, a filesystem should create a
 * null-terminated array of struct xattr_handler (one for each prefix) and
 * hang a pointer to it off of the s_xattr field of the superblock.
 *
 * The rozofs_fooxattr() functions will use this list to dispatch xattr
 * operations to the correct xattr_handler.
 */
#define for_each_xattr_handler(handlers, handler)		\
		for ((handler) = *(handlers)++;			\
			(handler) != NULL;			\
			(handler) = *(handlers)++)

/*
 * Find the xattr_handler with the matching prefix.
 */
static const struct xattr_handler *
xattr_resolve_name(const struct xattr_handler **handlers, const char **name)
{
	const struct xattr_handler *handler;

	if (!*name)
		return NULL;

	for_each_xattr_handler(handlers, handler) {
		const char *n = strcmp_prefix(*name, handler->prefix);
		if (n) {
			*name = n;
			break;
		}
	}
	return handler;
}

/*
 * Find the handler for the prefix and dispatch its get() operation.
 */
ssize_t
rozofs_getxattr(struct dentry *dentry, const char *name, void *buffer, size_t size)
{
	const struct xattr_handler *handler;
	int ret;
	/*
	** save the current tracking context needed for allocation
	*/
	xattr_set_tracking_context(dentry);

	handler = xattr_resolve_name(ext4_xattr_handlers, &name);
	if (!handler)
	{
	   errno = EOPNOTSUPP;
           return -1;
	}
	ret = handler->get(dentry, name, buffer, size, handler->flags);
	if (ret < 0 )
	{
	  ret = -1;
	}
	rozofs_xattr_extended_release(dentry->d_inode);
	return ret;
}

/*
 * Find the handler for the prefix and dispatch its get() operation.
   Caution: the allocated buffer used for storing the extended attributes is not released by this function
 */
ssize_t
rozofs_getxattr_raw(struct dentry *dentry, const char *name, void *buffer, size_t size)
{
	const struct xattr_handler *handler;
	int ret;
	/*
	** save the current tracking context needed for allocation
	*/
	xattr_set_tracking_context(dentry);

	handler = xattr_resolve_name(ext4_xattr_handlers, &name);
	if (!handler)
	{
	   errno = EOPNOTSUPP;
           return -1;
	}
	ret = handler->get(dentry, name, buffer, size, handler->flags);
	if (ret < 0 )
	{
	  ret = -1;
	}
	return ret;
}

/*
 * Combine the results of the list() operation from every xattr_handler in the
 * list.
 */
ssize_t
rozofs_listxattr(struct dentry *dentry, char *buffer, size_t buffer_size)
{
   int size;
   /*
   ** save the current tracking context needed for allocation
   */
   xattr_set_tracking_context(dentry);

   size =  ext4_listxattr(dentry, buffer, buffer_size);
   if (size < 0)
   {
      errno = 0-size;
      rozofs_xattr_extended_release(dentry->d_inode);
      return -1;
   }
   rozofs_xattr_extended_release(dentry->d_inode);
   return size;
}

/*
 * Find the handler for the prefix and dispatch its set() operation.
 */
int
rozofs_setxattr(struct dentry *dentry, const char *name, const void *value, size_t size, int flags)
{
	const struct xattr_handler *handler;
	int ret;

	xattr_set_tracking_context(dentry);

	if (size == 0)
		value = "";  /* empty EA, do not remove */
	handler = xattr_resolve_name(ext4_xattr_handlers, &name);
	if (!handler)
	{
	   errno = EOPNOTSUPP;
           return -1;
	}	
	ret = handler->set(dentry, name, value, size, flags, handler->flags);
	if (ret < 0 )
	{
	  ret = -1;
	}
	rozofs_xattr_extended_release(dentry->d_inode);
	return ret;
}

/*
 * Find the handler for the prefix and dispatch its set() operation to remove
 * any associated extended attribute.
 */
int
rozofs_removexattr(struct dentry *dentry, const char *name)
{
	const struct xattr_handler *handler;
	int ret;

	xattr_set_tracking_context(dentry);

	handler = xattr_resolve_name(ext4_xattr_handlers, &name);
	if (!handler)
	{
	   errno = EOPNOTSUPP;
           return -1;
	}	
	ret= handler->set(dentry, name, NULL, 0,
			    XATTR_REPLACE, handler->flags);
	if (ret < 0 )
	{
	  ret = -1;
	}
	rozofs_xattr_extended_release(dentry->d_inode);
	return ret;
}
