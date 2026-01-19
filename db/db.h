#ifndef DB_H
#define DB_H

#include <sqlite3.h>

typedef enum sensors_db_mode { SENSORS_DB_PRODUCER, SENSORS_DB_CONSUMER } sensors_db_mode_t;

typedef struct sensors_db {
    sqlite3 *db;
    int data_limit;
    sensors_db_mode_t mode;
} sensors_db_t;

/* One row of sensor data */
typedef struct sensors_sample {
    int id;
    char timestamp[32];
    float bmp280_temperature;
    float bmp280_pressure;
    float htu21d_temperature;
    float htu21d_humidity;
} sensors_sample_t;

sensors_db_t *sensors_db_open(const char *db_file, sensors_db_mode_t mode, int data_limit);

int sensors_db_store_data(sensors_db_t *self, float bmp280_temp, float bmp280_pressure,
                          float htu21d_temp, float htu21d_humidity);

int sensors_db_read_latest(sensors_db_t *self, sensors_sample_t *out);

/* Read last N samples */
int sensors_db_read_n(sensors_db_t *self, sensors_sample_t *out_array, int max_samples);

void sensors_db_close(sensors_db_t *self);

#endif /* DB_H */
