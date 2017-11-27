## ABOUT ROZOFS

RozoFS is a scale-out NAS file system. RozoFS aims to provide an open source high performance and high availability scale out storage software appliance  for  intensive disk IO data center scenario. It comes as a free software, licensed under the GNU GPL v2. RozoFS provides an easy way to scale to petabytes storage but using erasure coding it was designed to provide very high availability levels with optimized raw capacity usage on heterogenous commodity hardwares.

RozoFS provide a native open source POSIX file system, build on top of a usual out-band scale-out storage architecture. The RozoFS specificity lies in the way data is stored. The data to be stored is translated into several chunks named projections using Mojette Transform and distributed across storage devices in such a way that it can be retrieved even if several pieces are unavailable. On the other hand, chuncks are meaningless alone. Redundancy schemes based on coding techniques like the one used by RozoFS allow to achieve signiﬁcant storage savings as compared to simple replication.

**Note**: [xxx] means optional, \<xxx\> means required.

## BUILDING AND INSTALLING FROM SOURCES

### Prerequisites

To build the RozoFS source code, it is necessary to install several libraries and tools. RozoFS uses the cross-platform build system **cmake** to get you started quickly. RozoFS dependencies are the following:

-   `gcc`
-   `make`
-   `cmake`
-   `libfuse-dev` (>= 2.9.0)
-   `libattr1-dev`
-   `uuid-dev`
-   `libconfig-dev`
-   `libreadline-dev`
-   `pyro`
-   `python2.7-dev`
-   `python-yaml`
-   `libpthread`
-   `libcrypt`
-   `rpcbind`
-   `swig`
-   `libnuma-dev`

### Build
Using default values will compile RozoFS in Release mode and install it on `/usr/local`.
```
# mkdir build
# cd build
# cmake -G "Unix Makefiles" [-DCMAKE_INSTALL_PREFIX=/foo/bar -DCMAKE_BUILD_TYPE=Debug] ../
# make
```
### Install
```
# [sudo] make install
```
If you use default values, make will place the executables in `/usr/local/bin`, build options (CMAKE_INSTALL_PREFIX, CMAKE_BUILD_TYPE...) of generated build tree can be modified with the following command :
```
# make edit_cache
```
### Uninstall

```
# make uninstall
```

## SETTING UP ROZOFS

### Prerequisites

#### exportd prerequisites:

-   a running portmap/rpcbind (see `portmap(8)`/`rpcbind(8)`)
-   extend attributes on used file system (see `mount(8)` and `fstab(5)`)
-   `libattr`
-   `libuuid`
-   `libconfig`

#### storaged prerequisites:

-   a running portmap/rpcbind (see `portmap(8)`/`rpcbind(8)`)
-   `libuuid`
-   `libconfig`

#### rozofsmount prerequisites:

-   `fuse`
-   `libfuse` (>= 2.9.0)
-   `fuse-utils`

### Start

1. install exportd on one host
2. install storaged on multiple hosts
3. fill in `export.conf` (in `/etc/rozofs` or `/usr/local/etc/rozofs` according to CMAKE_INSTALL_PREFIX)
4. fill in all `storage.conf`
5. `` # [sudo] exportd``
6. `` # [sudo] storaged``
7. wherever you want: `` # [sudo] rozofsmount -H [EXPORT_HOST] -E [EXPORT_PATH] -P [PASSWD] [MOUNTPOINT] ``

### Stop
1. ``# [sudo] umount [MOUNTPOINT]``
2. ``# [sudo] killall exportd``
3. ``# [sudo] killall storaged``

## MORE INFO
See https://github.com/rozofs/rozofs/wiki.

## BUGS
See https://github.com/rozofs/rozofs/issues.
