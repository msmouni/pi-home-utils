#include "model.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int display_model_to_json(const display_model_t *m, char *buf, size_t size)
{
    return snprintf(buf, size,
                    "{"
                    "\"version\":%d,"
                    "\"timestamp\":%ld,"
                    "\"wifi_connected\":%d,"
                    "\"wifi_ssid\":\"%s\","
                    "\"ip\":\"%s\","
                    "\"cpu_temp\":%d,"
                    "\"cpu_load\":%d,"
                    "\"ram_total\":%ld,"
                    "\"ram_usage_pr\":%d,"
                    "\"uptime\":%ld,"
                    "\"has_bmp280\":%d,"
                    "\"bmp280_temp\":%.2f,"
                    "\"bmp280_press\":%.2f,"
                    "\"has_htu21d\":%d,"
                    "\"htu21d_temp\":%.2f,"
                    "\"htu21d_hum\":%.2f"
                    "}",
                    DISPLAY_MODEL_VERSION, (long)m->timestamp, m->wifi_connected, m->wifi_ssid,
                    m->ip, m->cpu_temp, m->cpu_load, m->ram_total, m->ram_usage_pr, m->uptime,
                    m->has_bmp280, m->bmp280_temperature, m->bmp280_pressure, m->has_htu21d,
                    m->htu21d_temperature, m->htu21d_humidity);
}

int display_model_from_json(const char *json, display_model_t *m)
{
    if (!json || !m)
        return -1;

    int wifi;
    int has_bmp280, has_htu21d;
    long timestamp;

    int ret =
        sscanf(json,
               "{"
               "\"version\":%*d,"
               "\"timestamp\":%ld,"
               "\"wifi_connected\":%d,"
               "\"wifi_ssid\":\"%31[^\"]\","
               "\"ip\":\"%31[^\"]\","
               "\"cpu_temp\":%d,"
               "\"cpu_load\":%d,"
               "\"ram_total\":%ld,"
               "\"ram_usage_pr\":%d,"
               "\"uptime\":%ld,"
               "\"has_bmp280\":%d,"
               "\"bmp280_temp\":%f,"
               "\"bmp280_press\":%f,"
               "\"has_htu21d\":%d,"
               "\"htu21d_temp\":%f,"
               "\"htu21d_hum\":%f"
               "}",
               &timestamp, &wifi, m->wifi_ssid, m->ip, &m->cpu_temp, &m->cpu_load, &m->ram_total,
               &m->ram_usage_pr, &m->uptime, &has_bmp280, &m->bmp280_temperature,
               &m->bmp280_pressure, &has_htu21d, &m->htu21d_temperature, &m->htu21d_humidity);

    if (ret < 15)
        return -1;

    m->timestamp = (time_t)timestamp;
    m->wifi_connected = wifi ? true : false;
    m->has_bmp280 = has_bmp280 ? true : false;
    m->has_htu21d = has_htu21d ? true : false;

    return 0;
}