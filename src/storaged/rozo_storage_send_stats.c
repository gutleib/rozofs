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
#include "sconfig.h"
#include <contrib/influxdb-c/influxdb.h>
#include <getopt.h>
#include <rozofs/rpc/spproto.h>
#include <stdlib.h>

#define P_COUNT 0
#define P_ELAPSE 1
#define P_BYTES 2

#define SHOW_S_PROFILING(the_probe)                                  \
    fprintf(stdout, "%s -> count: %" PRIu64 ", time: %" PRIu64 "\n", \
            #the_probe, sprof->the_probe[P_COUNT], sprof->the_probe[P_ELAPSE]);

#define SHOW_S_PROFILING_IO(the_probe)                                                   \
    fprintf(stdout, "%s -> count: %" PRIu64 ", time: %" PRIu64 ", bytes: %" PRIu64 "\n", \
            #the_probe, sprof->the_probe[P_COUNT], sprof->the_probe[P_ELAPSE], sprof->the_probe[P_BYTES]);

#define SEND_S_PROFILING_IO(the_measure, the_probe)        \
    send_udp(                                              \
        &client,                                           \
        INFLUX_MEAS(the_measure),                          \
        INFLUX_TAG("host", hostname),                      \
        INFLUX_TAG("ip", ip),                              \
        INFLUX_TAG("prof", #the_probe),                    \
        INFLUX_F_INT("count", sprof->the_probe[P_COUNT]),  \
        INFLUX_F_INT("etime", sprof->the_probe[P_ELAPSE]), \
        INFLUX_F_INT("bytes", sprof->the_probe[P_BYTES]),  \
        INFLUX_END);

#define SEND_S_PROFILING(the_measure, the_probe)           \
    send_udp(                                              \
        &client,                                           \
        INFLUX_MEAS(the_measure),                          \
        INFLUX_TAG("host", hostname),                      \
        INFLUX_TAG("ip", ip),                              \
        INFLUX_TAG("prof", #the_probe),                    \
        INFLUX_F_INT("count", sprof->the_probe[P_COUNT]),  \
        INFLUX_F_INT("etime", sprof->the_probe[P_ELAPSE]), \
        INFLUX_END);

#define ADD_S_PROFILING_IO(the_measure, the_probe)         \
        INFLUX_MEAS(the_measure),                          \
        INFLUX_TAG("host", hostname),                      \
        INFLUX_TAG("ip", ip),                              \
        INFLUX_TAG("prof", #the_probe),                    \
        INFLUX_F_INT("count", sprof->the_probe[P_COUNT]),  \
        INFLUX_F_INT("etime", sprof->the_probe[P_ELAPSE]), \
        INFLUX_F_INT("bytes", sprof->the_probe[P_BYTES]),

char storaged_config_file[PATH_MAX] = STORAGED_DEFAULT_CONFIG;
sconfig_t storaged_config;

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
    printf("RozoFS storage stats sender to an InfluxDB host - %s\n", VERSION);
    printf("Usage: rozo_storage_send_stats [OPTIONS]\n\n");
    printf("   -h, --help\t\t\tprint this message.\n");
    printf("   -c, --config=config-file\tspecify config file to use (default: "
           "%s).\n",
           STORAGED_DEFAULT_CONFIG);
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

    ret = pread(fd, buf, sizeof(spp_profiler_t), 0);
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
            if (!realpath(optarg, storaged_config_file))
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

    // Initialize the list of storage config
    sconfig_initialize(&storaged_config);

    if (verbose)
        fprintf(stdout, "Reading rozofs-storaged configuration file: %s\n",
                storaged_config_file);

    // Read the configuration file
    if (sconfig_read(&storaged_config, storaged_config_file, 0) != 0)
    {
        fprintf(stderr, "Failed to parse storage configuration file (%s): %s.\n",
                storaged_config_file, strerror(errno));
        goto error;
    }

    // Check the configuration
    if (sconfig_validate(&storaged_config) != 0)
    {
        fprintf(stderr, "Inconsistent storage configuration file: %s.\n",
                strerror(errno));
        goto error;
    }

    // Prepare UDP client
    if (verbose)
        fprintf(stdout, "Stats will be send to host: %s:%u, database: %s\n", host,
                port, db_name);
    influx_client_t client = {host, port, db_name, NULL, NULL};

    // Compute storage KPI base path
    char storaged_kpi_base_path[256];
    char *pChar = storaged_kpi_base_path;

    pChar += rozofs_string_append(pChar, ROZOFS_KPI_ROOT_PATH);
    pChar += rozofs_string_append(pChar, "/storage/");
    pChar += rozofs_ipv4_append(pChar, sconfig_get_this_IP(&storaged_config, 0));

    // Compute storaged KPI file
    char storaged_kpi_path[1024];
    sprintf(storaged_kpi_path, "%s%s", storaged_kpi_base_path,
            "/storaged/profiler");

    // Get IP
    char ip[256];
    rozofs_ipv4_append(ip, sconfig_get_this_IP(&storaged_config, 0));
    if (verbose)
        fprintf(stdout, "Our IP is %s\n", ip);

    // Get hostname
    char hostname[256];
    hostname[0] = 0;
    gethostname(hostname, 256);
    if (verbose)
        fprintf(stdout, "Our hostname is %s\n", hostname);

    // Reading storaged KPI file
    if (verbose)
        fprintf(stdout, "Reading storaged kpi file path: %s\n", storaged_kpi_path);

    spp_profiler_t *sprof = NULL;
    sprof = malloc(sizeof(spp_profiler_t));

    if ((ret = read_profiler(storaged_kpi_path, sprof)) != 0)
    {
        fprintf(stderr, "Unable to read file: %s\n", storaged_kpi_path);
        goto error;
    }

    //  Sending storaged stats to InfluxDB
    if (verbose)
        SHOW_S_PROFILING(remove);

    // Send one request
    SEND_S_PROFILING("storaged_prof", remove)

    // Release sprof
    if (sprof)
    {
        free(sprof);
    }

    uint64_t bitmask[4] = {0};
    uint8_t cid, rank, bit;
    list_for_each_forward(l, &storaged_config.storages)
    {

        storage_config_t *sc = list_entry(l, storage_config_t, list);
        cid = sc->cid;

        /* Is this storage is already used */
        rank = (cid - 1) / 64;
        bit = (cid - 1) % 64;
        if (bitmask[rank] & (1ULL << bit))
        {
            continue;
        }

        bitmask[rank] |= (1ULL << bit);

        // Compute storio KPI file
        char storio_kpi_path[1024];
        sprintf(storio_kpi_path, "%s%s%d%s", storaged_kpi_base_path, "/storio_",
                cid, "/profiler");

        // Reading storio KPI file
        if (verbose)
            fprintf(stdout, "Reading storio kpi file path: %s\n", storio_kpi_path);

        spp_profiler_t *sprof = NULL;
        sprof = malloc(sizeof(spp_profiler_t));

        if ((ret = read_profiler(storio_kpi_path, sprof)) != 0)
        {
            fprintf(stderr, "Unable to read file: %s\n", storaged_kpi_path);
            goto error;
        }

        //  Sending storio stats to InfluxDB
        if (verbose)
        {
            SHOW_S_PROFILING_IO(read)
            SHOW_S_PROFILING_IO(write)
        }

        // Sending 2 request
        // SEND_S_PROFILING_IO("storio_prof", read)
        // SEND_S_PROFILING_IO("storio_prof", write)

        // All in one request
        if ((send_udp(
                &client,
                ADD_S_PROFILING_IO("storio_prof", read)
                ADD_S_PROFILING_IO("storio_prof", write)
                INFLUX_END)) != 0)
        {
            fprintf(stderr, "failed to send storio stats to InfluxDB: %s",
                    strerror(errno));
            goto error;
        }

        // Release sprof
        if (sprof)
        {
            free(sprof);
        }
    }

    sconfig_release(&storaged_config);
    exit(EXIT_SUCCESS);

error:
    sconfig_release(&storaged_config);
    exit(EXIT_FAILURE);
}