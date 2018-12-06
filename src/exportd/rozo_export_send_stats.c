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
#include "econfig.h"
#include "rozofs_ip4_flt.h"
#include <contrib/influxdb-c/influxdb.h>
#include <getopt.h>
#include <rozofs/rpc/export_profiler.h>
#include <stdlib.h>

#define P_COUNT 0
#define P_TIME 1

#define SHOW_E_PROFILING(the_probe) \
    fprintf(stdout, "%s -> count: %" PRIu64 ", time: %" PRIu64 "\n", #the_probe, mprof->the_probe[P_COUNT], mprof->the_probe[P_TIME]);
#define SHOW_E_PROFILING_IO(the_probe)                                                   \
    fprintf(stdout, "%s -> count: %" PRIu64 ", time: %" PRIu64 ", bytes: %" PRIu64 "\n", \
            #the_probe, mprof->the_probe[P_COUNT], mprof->the_probe[P_TIME], mprof->the_probe[P_BYTES]);

#define SEND_E_PROFILING(the_probe, the_eid)              \
    send_udp(                                             \
        &iclient,                                         \
        INFLUX_MEAS("export_prof"),                       \
        INFLUX_TAG("host", hostname),                     \
        INFLUX_TAG("eid", the_eid),                       \
        INFLUX_TAG("prof", #the_probe),                   \
        INFLUX_F_INT("count", mprof->the_probe[P_COUNT]), \
        INFLUX_F_INT("etime", mprof->the_probe[P_TIME]),  \
        INFLUX_END);

char exportd_config_file[PATH_MAX] = EXPORTD_DEFAULT_CONFIG;
econfig_t exportd_config;
int rozofs_no_site_file;
/*----------------------------------------------------------------------------
**
**  Display usage
**
**----------------------------------------------------------------------------
*/
void usage(char *fmt, ...)
{
    va_list args;
    char error_buffer[512];

    /*
  ** Display optional error message if any
  */
    if (fmt)
    {
        va_start(args, fmt);
        vsprintf(error_buffer, fmt, args);
        va_end(args);
        printf("%s", error_buffer);
        printf("%s\n", error_buffer);
    }

    /*
  ** Display usage
  */
    printf("RozoFS export stats sender to an InfluxDB host - %s\n", VERSION);
    printf("Usage: rozo_export_send_stats [OPTIONS]\n\n");
    printf("   -h, --help\t\t\tprint this message.\n");
    printf("   -c, --config=config-file\tspecify config file to use (default: "
           "%s).\n",
           EXPORTD_DEFAULT_CONFIG);
    printf("   -H, --host\tspecify the hostname to use for sending stats "
           "(default: 127.0.0.1).\n");
    printf("   -p, --port\tspecify the remote port to use for sending stats "
           "(default: 8090).\n");
    printf("   -d, --dbname\tspecify the database name to use for sending stats "
           "(default: sensu).\ns");

    if (fmt)
        exit(EXIT_FAILURE);
    exit(EXIT_SUCCESS);
}

int read_profiler(char *path, void *buf)
{

    int fd = -1;
    int ret = -1;
    if ((fd = open(path, O_RDONLY, S_IRWXU)) < 0)
    {
        printf("error while opening %s\n", strerror(errno));
        return -1;
    }

    ret = pread(fd, buf, sizeof(export_one_profiler_t), 0);
    if (ret < 0)
    {
        close(fd);
        printf("cannot read %s", strerror(errno));
        return -1;
    }

    close(fd);
    return 0;
}

/*----------------------------------------------------------------------------
**
**  M A I N
**
**----------------------------------------------------------------------------
*/
int main(int argc, char *argv[])
{
    int c = 0;
    char *host = "127.0.0.1";
    uint16_t port = 8090;
    char *db_name = "sensu";
    list_t *l = NULL;
    int ret = -1;
    int verbose = 0;
    static struct option long_options[] = {{"help", no_argument, 0, 'h'},
                                           {"config", required_argument, 0, 'c'},
                                           {"host", required_argument, 0, 'H'},
                                           {"port", required_argument, 0, 'p'},
                                           {"dbname", required_argument, 0, 'd'},
                                           {"verbose", required_argument, 0, 'v'},
                                           {0, 0, 0, 0}};

    while (1)
    {

        int option_index = 0;
        c = getopt_long(argc, argv, "hmCc:H:p:d:v", long_options, &option_index);

        if (c == -1)
            break;

        switch (c)
        {

        case 'h':
            usage(NULL);
            break;
        case 'c':
            if (!realpath(optarg, exportd_config_file))
            {
                printf("get config failed for path: %s (%s)\n", optarg,
                       strerror(errno));
                exit(EXIT_FAILURE);
            }
            break;
        case 'H':
            host = strdup(optarg);
            break;
        case 'd':
            db_name = strdup(optarg);
            break;
        case 'p':
            ret = sscanf(optarg, "%lu", (long unsigned int *)&port);
            break;
        case 'v':
            verbose = 1;
            break;
        case '?':
            usage(NULL);
            break;
        default:
            usage("Unexpected option \'%c\'", c);
            break;
        }
    }

    // Initialize exportd config
    econfig_initialize(&exportd_config);

    if (verbose)
        fprintf(stdout, "Reading rozofs-exportd configuration file: %s\n",
                exportd_config_file);

    // Read the configuration file
    if (econfig_read(&exportd_config, exportd_config_file) != 0)
    {
        fprintf(stderr, "Failed to parse export configuration file (%s): %s.\n",
                exportd_config_file, strerror(errno));
        goto error;
    }

    // Check the configuration
    if (econfig_validate(&exportd_config) != 0)
    {
        fprintf(stderr, "Inconsistent export configuration file: %s.\n",
                strerror(errno));
        goto error;
    }

    // Prepare UDP client
    if (verbose)
        fprintf(stdout, "Stats will be send to host: %s:%u, database: %s\n", host,
                port, db_name);

    influx_client_t iclient = {host, port, db_name, NULL, NULL};

    // Compute export KPI base path
    char exportd_kpi_base_path[256];
    char *pChar = exportd_kpi_base_path;

    pChar += rozofs_string_append(pChar, ROZOFS_KPI_ROOT_PATH);
    pChar += rozofs_string_append(pChar, "/export/");

    // Get hostname
    char hostname[256];
    hostname[0] = 0;
    gethostname(hostname, 256);
    if (verbose)
        fprintf(stdout, "Our hostname is %s\n", hostname);

    uint8_t eid = 0;
    list_for_each_forward(l, &exportd_config.exports)
    {

        export_config_t *e = list_entry(l, export_config_t, list);
        eid = e->eid;
        char eid_str[8];
        sprintf(eid_str, "%d", eid);

        // Compute export KPI file
        char export_kpi_path[1024];
        sprintf(export_kpi_path, "%seid_%d%s", exportd_kpi_base_path, eid,
                "/profiler");

        // Reading export KPI file
        if (verbose)
            fprintf(stdout, "Reading export kpi file path: %s\n", export_kpi_path);

        export_one_profiler_t *mprof = NULL;
        mprof = malloc(sizeof(export_one_profiler_t));
        memset(mprof, 0, sizeof(export_one_profiler_t));

        if ((ret = read_profiler(export_kpi_path, mprof)) != 0)
        {
            fprintf(stderr, "Unable to read file: %s\n", export_kpi_path);
            goto error;
        }

        //  Sending export stats to InfluxDB
        if (verbose)
        {
            SHOW_E_PROFILING(ep_statfs);
            SHOW_E_PROFILING(ep_lookup);
            SHOW_E_PROFILING(ep_getattr);
            SHOW_E_PROFILING(ep_setattr);
            SHOW_E_PROFILING(ep_mknod);
            SHOW_E_PROFILING(ep_mkdir);
            SHOW_E_PROFILING(ep_rmdir);
            SHOW_E_PROFILING(ep_unlink);
            SHOW_E_PROFILING(ep_rename);
            SHOW_E_PROFILING(ep_readdir);
            SHOW_E_PROFILING(ep_write_block);
            SHOW_E_PROFILING(ep_link);
            SHOW_E_PROFILING(ep_symlink);
            SHOW_E_PROFILING(ep_setxattr);
            SHOW_E_PROFILING(ep_getxattr);
            SHOW_E_PROFILING(ep_removexattr);
            SHOW_E_PROFILING(ep_listxattr);
            SHOW_E_PROFILING(ep_rename);
            SHOW_E_PROFILING(ep_readdir);
        }

        // Sending UDP requests
        SEND_E_PROFILING(ep_statfs, eid_str)
        SEND_E_PROFILING(ep_lookup, eid_str)
        SEND_E_PROFILING(ep_getattr, eid_str)
        SEND_E_PROFILING(ep_setattr, eid_str)
        SEND_E_PROFILING(ep_mknod, eid_str)
        SEND_E_PROFILING(ep_mkdir, eid_str)
        SEND_E_PROFILING(ep_rmdir, eid_str)
        SEND_E_PROFILING(ep_unlink, eid_str)
        SEND_E_PROFILING(ep_rename, eid_str)
        SEND_E_PROFILING(ep_readdir, eid_str)
        SEND_E_PROFILING(ep_write_block, eid_str)
        SEND_E_PROFILING(ep_link, eid_str)
        SEND_E_PROFILING(ep_symlink, eid_str)
        SEND_E_PROFILING(ep_setxattr, eid_str)
        SEND_E_PROFILING(ep_getxattr, eid_str)
        SEND_E_PROFILING(ep_removexattr, eid_str)
        SEND_E_PROFILING(ep_listxattr, eid_str)
        SEND_E_PROFILING(ep_rename, eid_str)
        SEND_E_PROFILING(ep_readdir, eid_str)

        // Release mprof
        if (mprof)
        {
            free(mprof);
        }
    }

    econfig_release(&exportd_config);
    exit(EXIT_SUCCESS);

error:
    econfig_release(&exportd_config);
    exit(EXIT_FAILURE);
}