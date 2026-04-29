#ifndef DISPLAY_MODEL_H
#define DISPLAY_MODEL_H

#include <stdbool.h>
#include <time.h>

#define MAX_PRINT_SIZE 128

#define DISPLAY_MAX_IP_LEN 32
#define DISPLAY_WIFI_SSID_LEN 32

#define DISPLAY_MODEL_VERSION 1

#define DISPLAY_MODEL_JSON_SIZE 512

typedef struct {
    /* Sensors data */
    bool has_bmp280;
    float bmp280_temperature;
    float bmp280_pressure;

    bool has_htu21d;
    float htu21d_temperature;
    float htu21d_humidity;

    /* System info */
    bool wifi_connected;
    char wifi_ssid[DISPLAY_WIFI_SSID_LEN];
    char ip[DISPLAY_MAX_IP_LEN];
    time_t timestamp;
    int cpu_temp;
    int cpu_load;
    long ram_total;
    int ram_usage_pr;
    long uptime;

} display_model_t;

int display_model_to_json(const display_model_t *model, char *buf, size_t size);
int display_model_from_json(const char *json, display_model_t *model);

#endif /* DISPLAY_MODEL_H */
