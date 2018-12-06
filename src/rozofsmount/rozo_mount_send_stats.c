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
#include <getopt.h>
#include <stdlib.h>
#include <contrib/influxdb-c/influxdb.h>
#include <rozofs/rpc/mpproto.h>

#define P_COUNT 0
#define P_TIME 1
#define P_BYTES 2

#define SHOW_M_PROFILING(the_probe)                                  \
    fprintf(stdout, "%s -> count: %" PRIu64 ", time: %" PRIu64 "\n", \
            #the_probe, mprof->the_probe[P_COUNT], mprof->the_probe[P_ELAPSE]);

#define SHOW_M_PROFILING_IO(the_probe)                                                   \
    fprintf(stdout, "%s -> count: %" PRIu64 ", time: %" PRIu64 ", bytes: %" PRIu64 "\n", \
            #the_probe, mprof->the_probe[P_COUNT], mprof->the_probe[P_ELAPSE], mprof->the_probe[P_BYTES]);

#define SEND_M_PROFILING_IO(the_measure, the_probe)        \
    send_udp(                                              \
        &iclient,                                          \
        INFLUX_MEAS(the_measure),                          \
        INFLUX_TAG("host", hostname),                      \
        INFLUX_TAG("prof", #the_probe),                    \
        INFLUX_F_INT("count", mprof->the_probe[P_COUNT]),  \
        INFLUX_F_INT("etime", mprof->the_probe[P_ELAPSE]), \
        INFLUX_F_INT("bytes", mprof->the_probe[P_BYTES]),  \
        INFLUX_END);

#define SEND_M_PROFILING(the_measure, the_probe)           \
    send_udp(                                              \
        &iclient,                                          \
        INFLUX_MEAS(the_measure),                          \
        INFLUX_TAG("host", hostname),                      \
        INFLUX_TAG("prof", #the_probe),                    \
        INFLUX_F_INT("count", mprof->the_probe[P_COUNT]),  \
        INFLUX_F_INT("etime", mprof->the_probe[P_ELAPSE]), \
        INFLUX_END);

#define ADD_M_PROFILING_IO(the_measure, the_probe)         \
    INFLUX_MEAS(the_measure),                              \
        INFLUX_TAG("host", hostname),                      \
        INFLUX_TAG("prof", #the_probe),                    \
        INFLUX_F_INT("count", mprof->the_probe[P_COUNT]),  \
        INFLUX_F_INT("etime", mprof->the_probe[P_ELAPSE]), \
        INFLUX_F_INT("bytes", mprof->the_probe[P_BYTES]),

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
    printf("RozoFS mount stats sender to an InfluxDB host - %s\n", VERSION);
    printf("Usage: rozo_mount_send_stats [OPTIONS]\n\n");
    printf("   -h, --help\t\t\tprint this message.\n");
    printf("   -c, --config=config-file\tspecify config file to use (default: "
           "%s).\n",
           STORAGED_DEFAULT_CONFIG);
    printf("   -H, --host\tspecify the hostname to use for sending stats (default: 127.0.0.1).\n");
    printf("   -p, --port\tspecify the remote port to use for sending stats (default: 8090).\n");
    printf("   -d, --dbname\tspecify the database name to use for sending stats (default: sensu).\ns");

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

    ret = pread(fd, buf, sizeof(mpp_profiler_t), 0);
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
    // list_t *l = NULL;
    int ret = -1;
    int verbose = 0;
    int instance = -1;
    static struct option long_options[] = {{"help", no_argument, 0, 'h'},
                                           {"host", required_argument, 0, 'H'},
                                           {"port", required_argument, 0, 'p'},
                                           {"dbname", required_argument, 0, 'd'},
                                           {"verbose", required_argument, 0, 'v'},
                                           {0, 0, 0, 0}};

    while (1)
    {

        int option_index = 0;
        c = getopt_long(argc, argv, "hm:H:p:d:v", long_options, &option_index);

        if (c == -1)
            break;

        switch (c)
        {

        case 'h':
            usage(NULL);
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

    // Prepare UDP client
    if (verbose)
        fprintf(stdout, "Stats will be send to host: %s:%u, database: %s\n", host, port, db_name);
    influx_client_t iclient = {host, port, db_name, NULL, NULL};

    // Get hostname
    char hostname[256];
    hostname[0] = 0;
    gethostname(hostname, 256);
    if (verbose)
        fprintf(stdout, "Our hostname is %s\n", hostname);

    // Compute mount KPI base path
    char mount_kpi_base_path[256];
    char *pChar = mount_kpi_base_path;

    pChar += rozofs_string_append(pChar, ROZOFS_KPI_ROOT_PATH);
    pChar += rozofs_string_append(pChar, "/mount/");

    if (verbose)
        fprintf(stdout, "Check KPI directories under %s\n", mount_kpi_base_path);

    // Iterate the directory
    DIR *d;
    struct dirent *dir;
    d = opendir(mount_kpi_base_path);
    if (d)
    {
        while ((dir = readdir(d)) != NULL)
        {
            instance = -1;

            if (sscanf(dir->d_name, "inst_%d", &instance))
            {
                // Compute mount KPI file
                char mount_kpi_path[1024];
                sprintf(mount_kpi_path, "%sinst_%d%s", mount_kpi_base_path, instance, "/mount/profiler");

                char instance_str[8];
                sprintf(instance_str, "%d", instance);

                // Reading mount KPI file
                if (verbose)
                    fprintf(stdout, "Reading mount kpi file path: %s\n", mount_kpi_path);

                mpp_profiler_t *mprof = NULL;
                mprof = malloc(sizeof(mpp_profiler_t));

                if ((ret = read_profiler(mount_kpi_path, mprof)) != 0)
                {
                    fprintf(stderr, "Unable to read file: %s\n", mount_kpi_path);
                    goto error;
                }

                //  Sending mount stats to InfluxDB
                if (verbose)
                {
                    SHOW_M_PROFILING(rozofs_ll_statfs)
                    SHOW_M_PROFILING(rozofs_ll_lookup)
                    SHOW_M_PROFILING(rozofs_ll_lookup_agg)
                    SHOW_M_PROFILING(rozofs_ll_getattr)
                    SHOW_M_PROFILING(rozofs_ll_setattr)
                    SHOW_M_PROFILING(rozofs_ll_mknod)
                    SHOW_M_PROFILING(rozofs_ll_mkdir)
                    SHOW_M_PROFILING(rozofs_ll_rmdir)
                    SHOW_M_PROFILING(rozofs_ll_unlink)
                    SHOW_M_PROFILING(rozofs_ll_rename)
                    SHOW_M_PROFILING(rozofs_ll_readdir)
                    SHOW_M_PROFILING(rozofs_ll_link)
                    SHOW_M_PROFILING(rozofs_ll_symlink)
                    SHOW_M_PROFILING(rozofs_ll_setxattr)
                    SHOW_M_PROFILING(rozofs_ll_getxattr)
                    SHOW_M_PROFILING(rozofs_ll_removexattr)
                    SHOW_M_PROFILING(rozofs_ll_listxattr)
                    SHOW_M_PROFILING(rozofs_ll_access)
                    SHOW_M_PROFILING(rozofs_ll_create)
                    SHOW_M_PROFILING_IO(rozofs_ll_read)
                    SHOW_M_PROFILING_IO(rozofs_ll_write)
                }

                SEND_M_PROFILING("mount_prof", rozofs_ll_statfs)
                SEND_M_PROFILING("mount_prof", rozofs_ll_lookup)
                SEND_M_PROFILING("mount_prof", rozofs_ll_lookup_agg)
                SEND_M_PROFILING("mount_prof", rozofs_ll_getattr)
                SEND_M_PROFILING("mount_prof", rozofs_ll_setattr)
                SEND_M_PROFILING("mount_prof", rozofs_ll_mknod)
                SEND_M_PROFILING("mount_prof", rozofs_ll_mkdir)
                SEND_M_PROFILING("mount_prof", rozofs_ll_rmdir)
                SEND_M_PROFILING("mount_prof", rozofs_ll_unlink)
                SEND_M_PROFILING("mount_prof", rozofs_ll_rename)
                SEND_M_PROFILING("mount_prof", rozofs_ll_readdir)
                SEND_M_PROFILING("mount_prof", rozofs_ll_link)
                SEND_M_PROFILING("mount_prof", rozofs_ll_symlink)
                SEND_M_PROFILING("mount_prof", rozofs_ll_setxattr)
                SEND_M_PROFILING("mount_prof", rozofs_ll_getxattr)
                SEND_M_PROFILING("mount_prof", rozofs_ll_removexattr)
                SEND_M_PROFILING("mount_prof", rozofs_ll_listxattr)
                SEND_M_PROFILING("mount_prof", rozofs_ll_access)
                SEND_M_PROFILING("mount_prof", rozofs_ll_create)
                SEND_M_PROFILING_IO("mount_prof", rozofs_ll_read)
                SEND_M_PROFILING_IO("mount_prof", rozofs_ll_write)

                // Release mprof
                if (mprof)
                {
                    free(mprof);
                }
            }
        }
        closedir(d);
    }

    exit(EXIT_SUCCESS);

error:
    exit(EXIT_FAILURE);
}