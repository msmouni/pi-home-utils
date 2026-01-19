#include "db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CREATE_TABLE_SQL                                                                           \
    "CREATE TABLE IF NOT EXISTS SensorData ("                                                      \
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "                                                       \
    "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, "                                               \
    "bmp280_temperature REAL, "                                                                    \
    "bmp280_pressure REAL, "                                                                       \
    "htu21d_temperature REAL, "                                                                    \
    "htu21d_humidity REAL);"

sensors_db_t *sensors_db_open(const char *db_file, sensors_db_mode_t mode, int data_limit)
{
    sensors_db_t *db = calloc(1, sizeof(*db));
    if (!db)
        return NULL;

    db->mode = mode;
    db->data_limit = data_limit;

    int flags = 0;

    switch (db->mode) {
    case SENSORS_DB_CONSUMER: {
        /* Consumer must NOT create DB */
        if (access(db_file, F_OK) != 0) {
            fprintf(stderr, "Database does not exist: %s\n", db_file);
            goto error_db;
        }

        flags = SQLITE_OPEN_READONLY;

        if (sqlite3_open_v2(db_file, &db->db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
            fprintf(stderr, "Cannot open DB: %s\n", sqlite3_errmsg(db->db));
            goto error_db;
        }

        break;
    }
    case SENSORS_DB_PRODUCER: {

        flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

        if (sqlite3_open_v2(db_file, &db->db, flags, NULL) != SQLITE_OK) {
            fprintf(stderr, "Cannot open DB: %s\n", sqlite3_errmsg(db->db));
            goto error_db;
        }

        /* Only producer creates table */
        char *err = NULL;
        if (sqlite3_exec(db->db, CREATE_TABLE_SQL, NULL, NULL, &err) != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", err);
            sqlite3_free(err);
            goto error_db;
        }
        break;
    }
    } /* switch (db->mode) */

    return db;

error_db:
    sqlite3_close(db->db);
    free(db);
    return NULL;
}

int sensors_db_store_data(sensors_db_t *self, float bmp280_temp, float bmp280_pressure,
                          float htu21d_temp, float htu21d_humidity)
{
    if (self->mode != SENSORS_DB_PRODUCER) {
        fprintf(stderr, "DB is read-only\n");
        return -1;
    }

    const char *sql = "INSERT INTO SensorData "
                      "(bmp280_temperature, bmp280_pressure, "
                      " htu21d_temperature, htu21d_humidity) "
                      "VALUES (?, ?, ?, ?);";

    /* To execute an SQL statement,
       it must first be compiled into a byte-code program
       https://sqlite.org/c3ref/prepare.html
    */
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(self->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return -1;

    sqlite3_bind_double(stmt, 1, bmp280_temp);
    sqlite3_bind_double(stmt, 2, bmp280_pressure);
    sqlite3_bind_double(stmt, 3, htu21d_temp);
    sqlite3_bind_double(stmt, 4, htu21d_humidity);

    /* evaluate the statement */
    sqlite3_step(stmt);

    /* destroy stmt and get evaluation result
       https://sqlite.org/c3ref/finalize.html */
    int rc = sqlite3_finalize(stmt);

    if (rc != SQLITE_OK)
        return -1;

    /* Trim old data */
    if (self->data_limit > 0) {
        char sql_trim[256];
        snprintf(sql_trim, sizeof(sql_trim),
                 "DELETE FROM SensorData WHERE id NOT IN ("
                 "SELECT id FROM SensorData ORDER BY id DESC LIMIT %d);",
                 self->data_limit);

        sqlite3_exec(self->db, sql_trim, NULL, NULL, NULL);
    }

    return 0;
}

int sensors_db_read_latest(sensors_db_t *self, sensors_sample_t *out)
{
    const char *sql = "SELECT id, timestamp, bmp280_temperature, "
                      "bmp280_pressure, htu21d_temperature, htu21d_humidity "
                      "FROM SensorData ORDER BY id DESC LIMIT 1;";

    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(self->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return -1;

    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return -1;
    }

    /* https://sqlite.org/c3ref/column_blob.html */
    out->id = sqlite3_column_int(stmt, 0);
    strncpy(out->timestamp, (const char *)sqlite3_column_text(stmt, 1), sizeof(out->timestamp));
    out->bmp280_temperature = sqlite3_column_double(stmt, 2);
    out->bmp280_pressure = sqlite3_column_double(stmt, 3);
    out->htu21d_temperature = sqlite3_column_double(stmt, 4);
    out->htu21d_humidity = sqlite3_column_double(stmt, 5);

    return sqlite3_finalize(stmt);
}

int sensors_db_read_n(sensors_db_t *self, sensors_sample_t *out_array, int max_samples)
{
    const char *sql = "SELECT id, timestamp, bmp280_temperature, "
                      "bmp280_pressure, htu21d_temperature, htu21d_humidity "
                      "FROM SensorData ORDER BY id DESC LIMIT ?;";

    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(self->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return -1;

    sqlite3_bind_int(stmt, 1, max_samples);

    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        sensors_sample_t *out = &out_array[count++];

        out->id = sqlite3_column_int(stmt, 0);
        strncpy(out->timestamp, (const char *)sqlite3_column_text(stmt, 1), sizeof(out->timestamp));
        out->bmp280_temperature = sqlite3_column_double(stmt, 2);
        out->bmp280_pressure = sqlite3_column_double(stmt, 3);
        out->htu21d_temperature = sqlite3_column_double(stmt, 4);
        out->htu21d_humidity = sqlite3_column_double(stmt, 5);

        if (count >= max_samples)
            break;
    }

    sqlite3_finalize(stmt);
    return count;
}

void sensors_db_close(sensors_db_t *self)
{
    if (!self)
        return;
    sqlite3_close(self->db);
    free(self);
}
