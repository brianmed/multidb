#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <libgen.h>

#include <errno.h>

#include "libmultidb.h"

void mdb_init(void)
{
    GSList* list = NULL, *iterator = NULL;

    gchar *MULTIDB_PREFIX = getenv("MULTIDB_PREFIX");
    if (NULL == MULTIDB_PREFIX) {
        MULTIDB_PREFIX = g_strdup(".");
    }

    MULTIDB_BASEDIR = g_strconcat(MULTIDB_PREFIX, "/", "multidb", NULL);
    MULTIDB_DATADIR = g_strconcat(MULTIDB_BASEDIR, "/", "data", NULL);
    MULTIDB_SCHEMADIR = g_strconcat(MULTIDB_DATADIR, "/", "schema", NULL);
    MULTIDB_TABLESDIR = g_strconcat(MULTIDB_DATADIR, "/", "tables", NULL);
    MULTIDB_PURGATORYDIR = g_strconcat(MULTIDB_DATADIR, "/", "purgatory", NULL);
    
    list = g_slist_append(list, MULTIDB_BASEDIR);
    list = g_slist_append(list, MULTIDB_DATADIR);
    list = g_slist_append(list, MULTIDB_SCHEMADIR);
    list = g_slist_append(list, MULTIDB_TABLESDIR);

    for (iterator = list; iterator; iterator = iterator->next) {
        if (0 != g_mkdir_with_parents(iterator->data, 0775)) {
            fprintf(stderr, "error: g_mkdir_with_parents: %s: %s\n", iterator->data, g_strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    g_slist_free(list);
}

enum {
    STATE_START,
    STATE_TABLENAME,
    STATE_FROM,
    STATE_INNER_JOIN,
    STATE_SET,
    STATE_UPDATE,
    STATE_START_COLS,
    STATE_PROCESS_COLS,
    STATE_END_COLS,

    STATE_WHERE,

    STATE_VALUES_CONSTANT,
    STATE_START_VALUES,
    STATE_PROCESS_VALUES,
    STATE_END_VALUES,
    STATE_END
};

char *tickGTokenType(GTokenType token);

/*
 *  CREATE TABLE site_key (
 *      id serial,
 *      site_key VARCHAR(512),
 *      updated timestamp,
 *      inserted timestamp
 *  );
 */

struct ddl_parsed parse_create(const gchar *text)
{
    GScanner *scanner;
    
    scanner = g_scanner_new(NULL);
    
    /* feed in the text */
    g_scanner_input_text(scanner, text, strlen(text));
    
    /* give the error handler an idea on how the input is named */
    scanner->input_name = "CREATE TABLE";

    int state = STATE_START;
    gchar *_buf = NULL;

    struct ddl_parsed ddl_create = {NULL, NULL};

    while (!g_scanner_eof(scanner))
    {
        static int nested = 0;
        GTokenType tokenType = g_scanner_get_next_token(scanner);

        gchar *converted = NULL;
        gchar *t = NULL;

        switch (state) {
            case STATE_START: 
                if (G_TOKEN_IDENTIFIER != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }

                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("CREATE", scanner->value.v_identifier, strlen("CREATE"))) {
                        if (NULL == _buf) {
                            _buf = g_strdup("CREATE ");
                        }
                        else {
                            g_scanner_error(scanner, "Unexpected CREATE TABLE preamble: %s\n", _buf);
                            exit(EXIT_FAILURE);
                        }
                    }
                    else if (0 == g_ascii_strncasecmp("TABLE", scanner->value.v_identifier, strlen("TABLE"))) {
                        gchar *t = _buf;
                        _buf = g_strconcat(_buf, scanner->value.v_identifier, NULL);
                        g_free(t);
                            
                        if (0 != g_ascii_strncasecmp("CREATE TABLE", _buf, strlen("CREATE TABLE"))) {
                            g_scanner_error(scanner, "Unexpected CREATE TABLE preamble: %s\n", _buf);
                            exit(EXIT_FAILURE);
                        }

                        g_free(_buf);
                        _buf = NULL;

                        state = STATE_TABLENAME;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
            break;

            case STATE_TABLENAME: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    state = STATE_START_COLS;

                    ddl_create.tbl_name = g_strdup(scanner->value.v_identifier);
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_START_COLS: 
                if (G_TOKEN_LEFT_PAREN == tokenType) {
                    state = STATE_PROCESS_COLS;
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_PROCESS_COLS: 
                if (G_TOKEN_COMMA == tokenType) {
                    ddl_create.row = g_slist_append(ddl_create.row, g_strdup(_buf));

                    g_free(_buf);
                    _buf = NULL;
                }
                else if (G_TOKEN_RIGHT_PAREN == tokenType && 0 == nested) {
                    GTokenType nextToken = g_scanner_peek_next_token(scanner);

                    if (';' == nextToken) {
                        state = STATE_END_COLS;

                        ddl_create.row = g_slist_append(ddl_create.row, g_strdup(_buf));
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else if (G_TOKEN_IDENTIFIER == tokenType || G_TOKEN_LEFT_PAREN == tokenType || (G_TOKEN_RIGHT_PAREN == tokenType && 1 == nested) || G_TOKEN_INT == tokenType) {
                    switch ((int)tokenType) {
                        case G_TOKEN_IDENTIFIER: 
                            if (NULL == _buf) {
                                _buf = g_strconcat(scanner->value.v_identifier, " ", NULL);
                            }
                            else {
                                t = _buf;
                                _buf = g_strconcat(_buf, scanner->value.v_identifier, NULL);
                                g_free(t);
                            }
                        break;

                        case G_TOKEN_LEFT_PAREN: 
                            t = _buf;
                            _buf = g_strconcat(_buf, "(", NULL);
                            g_free(t);
                            nested = 1;
                        break;

                        case G_TOKEN_RIGHT_PAREN: 
                            t = _buf;
                            _buf = g_strconcat(_buf, ")", NULL);
                            g_free(t);
                            nested = 0;
                        break;

                        case G_TOKEN_INT: 
                            converted = g_strdup_printf("%li", scanner->value.v_int);
                            t = _buf;
                            _buf = g_strconcat(_buf, converted, NULL);
                            g_free(t);
                            g_free(converted);
                        break;
                    }
                }
            break;

            case STATE_END_COLS:
                if (';' != tokenType && G_TOKEN_EOF != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                }
            break;
        }
    }

    g_free(_buf);

    return(ddl_create);
}

char *tickGTokenType(GTokenType token)
{
    static gchar _hack[2] = "!";

    if (G_TOKEN_EOF == token) {
        return "G_TOKEN_EOF";
    }
    if (G_TOKEN_LEFT_PAREN == token) {
        return "G_TOKEN_LEFT_PAREN";
    }
    if (G_TOKEN_RIGHT_PAREN == token) {
        return "G_TOKEN_RIGHT_PAREN";
    }
    if (G_TOKEN_LEFT_CURLY == token) {
        return "G_TOKEN_LEFT_CURLY";
    }
    if (G_TOKEN_RIGHT_CURLY == token) {
        return "G_TOKEN_RIGHT_CURLY";
    }
    if (G_TOKEN_LEFT_BRACE == token) {
        return "G_TOKEN_LEFT_BRACE";
    }
    if (G_TOKEN_RIGHT_BRACE == token) {
        return "G_TOKEN_RIGHT_BRACE";
    }
    if (G_TOKEN_EQUAL_SIGN == token) {
        return "G_TOKEN_EQUAL_SIGN";
    }
    if (G_TOKEN_COMMA == token) {
        return "G_TOKEN_COMMA";
    }
    if (G_TOKEN_NONE == token) {
        return "G_TOKEN_NONE";
    }
    if (G_TOKEN_ERROR == token) {
        return "G_TOKEN_ERROR";
    }
    if (G_TOKEN_CHAR == token) {
        return "G_TOKEN_CHAR";
    }
    if (G_TOKEN_BINARY == token) {
        return "G_TOKEN_BINARY";
    }
    if (G_TOKEN_OCTAL == token) {
        return "G_TOKEN_OCTAL";
    }
    if (G_TOKEN_INT == token) {
        return "G_TOKEN_INT";
    }
    if (G_TOKEN_HEX == token) {
        return "G_TOKEN_HEX";
    }
    if (G_TOKEN_FLOAT == token) {
        return "G_TOKEN_FLOAT";
    }
    if (G_TOKEN_STRING == token) {
        return "G_TOKEN_STRING";
    }
    if (G_TOKEN_SYMBOL == token) {
        return "G_TOKEN_SYMBOL";
    }
    if (G_TOKEN_IDENTIFIER == token) {
        return "G_TOKEN_IDENTIFIER";
    }
    if (G_TOKEN_IDENTIFIER_NULL == token) {
        return "G_TOKEN_IDENTIFIER_NULL";
    }
    if (G_TOKEN_COMMENT_SINGLE == token) {
        return "G_TOKEN_COMMENT_SINGLE";
    }
    if (G_TOKEN_COMMENT_MULTI == token) {
        return "G_TOKEN_COMMENT_MULTI";
    }

    _hack[0] = token;
    return(_hack);

    // g_print("error: %d: not found in GTokenType\n", token);
    // exit(EXIT_FAILURE);
}

void write_fd(int fd, gchar *buf, size_t nbyte)
{
    ssize_t wrote = 0;

    for (int written = 0; written != nbyte; written += wrote) {
        wrote = write(fd, buf, nbyte - written);
        if (-1 == wrote) {
            if (EINTR == errno) {
                continue;
            }
            else {
                fprintf(stderr, "error: write_fd(%d): [%d] %s\n", fd, errno, g_strerror(errno));
                exit(EXIT_FAILURE);
            }
        }
    }
}

void write_file(gchar *path, gchar *buf)
{
    GError *error = NULL;
    GIOChannel *file = g_io_channel_new_file(path, "w", &error);
    if (error) {
        fprintf(stderr, "error: g_io_channel_new_file: [%s] %s\n", path, error->message);
        exit(EXIT_FAILURE);
    }

    g_io_channel_write_chars(file, buf, -1, NULL, &error);
    if (error) {
        fprintf(stderr, "error: g_io_channel_write_chars: %s\n", error->message);
        exit(EXIT_FAILURE);
    }
    
    g_io_channel_shutdown(file, TRUE, &error);
    if (error) {
        fprintf(stderr, "error: g_io_channel_shutdown %s\n", error->message);
        exit(EXIT_FAILURE);
    } 
    
    g_io_channel_unref(file);
}

void read_first_line(const gchar *path, gchar **buf)
{
    GError *error = NULL;
    GIOChannel *file = g_io_channel_new_file(path, "r", &error);
    if (error) {
        *buf = NULL;
        return;
        // fprintf(stderr, "error: g_io_channel_new_file: [%s] %s\n", path, error->message);
        // exit(EXIT_FAILURE);
    }

    g_io_channel_read_line(file, buf, NULL, NULL, &error);
    if (error) {
        *buf = NULL;
        return;
        // fprintf(stderr, "error: g_io_channel_read_line: %s\n", error->message);
        // exit(EXIT_FAILURE);
    }
    // g_print ("Read %u bytes: %s\n", len, *buf);
    
    g_io_channel_shutdown(file, TRUE, &error);
    if (error) {
        fprintf(stderr, "error: g_io_channel_shutdown %s\n", error->message);
        exit(EXIT_FAILURE);
    } 
    
    g_io_channel_unref(file);
}

void execute_ddl_create(gchar *sql)
{
    struct ddl_parsed ddl_create = parse_create(sql);

    gchar *schema_path = g_strconcat(MULTIDB_SCHEMADIR, "/", ddl_create.tbl_name, NULL);
    if (g_file_test(schema_path, G_FILE_TEST_IS_DIR)) {
        fprintf(stderr, "error: schema: %s: already exists: %s\n", ddl_create.tbl_name, schema_path);
        exit(EXIT_FAILURE);
    }
    if (0 != g_mkdir_with_parents(schema_path, 0775)) {
        fprintf(stderr, "error: g_mkdir_with_parents: %s: %s\n", schema_path, g_strerror(errno));
        exit(EXIT_FAILURE);
    }

    gchar *table_path = g_strconcat(MULTIDB_TABLESDIR, "/", ddl_create.tbl_name, NULL);
    if (g_file_test(table_path, G_FILE_TEST_IS_DIR)) {
        fprintf(stderr, "error: table: %s: already exists: %s\n", ddl_create.tbl_name, table_path);
        exit(EXIT_FAILURE);
    }
    if (0 != g_mkdir_with_parents(table_path, 0775)) {
        fprintf(stderr, "error: g_mkdir_with_parents: %s: %s\n", table_path, g_strerror(errno));
        exit(EXIT_FAILURE);
    }

    GSList* paths = NULL, *iterator = NULL;
    paths = g_slist_append(paths, g_strconcat(table_path, "/", "rows", NULL));
    paths = g_slist_append(paths, g_strconcat(table_path, "/", "metadata", NULL));
    paths = g_slist_append(paths, g_strconcat(table_path, "/", "metadata", "/", "columns", NULL));
    paths = g_slist_append(paths, g_strconcat(table_path, "/", "metadata", "/", "serial", NULL));
    for (iterator = paths; iterator; iterator = iterator->next) {
        if (0 != g_mkdir_with_parents(iterator->data, 0775)) {
            fprintf(stderr, "error: g_mkdir_with_parents: %s: %s\n", iterator->data, g_strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    g_slist_free_full(paths, g_free);

    for (int i = 0; i < 4096; ++i) {
        gchar *converted = g_strdup_printf("%04d", i);
        gchar *path = g_strconcat(table_path, "/", "rows", "/", converted, NULL);
        if (0 != g_mkdir_with_parents(path, 0775)) {
            fprintf(stderr, "error: g_mkdir_with_parents: %s: %s\n", iterator->data, g_strerror(errno));
            exit(EXIT_FAILURE);
        }
        g_free(converted);
        g_free(path);
    }

    gchar *path = g_strconcat(table_path, "/", "metadata", "/", "version", NULL);
    write_file(path, "v1");
    g_free(path);

    path = g_strconcat(table_path, "/", "metadata", "/", "roid", NULL);
    write_file(path, "0");
    g_free(path);

    for (iterator = ddl_create.row; iterator; iterator = iterator->next) {
        gchar **items = g_strsplit(iterator->data, " ", 2);

        gchar *path = g_strconcat(schema_path, "/", items[0], NULL);
        // g_print("%s :: %s\n", path, items[1]);
		write_file(path, items[1]);
        g_free(path);

        if (0 == g_ascii_strncasecmp("serial", items[1], strlen("serial"))) {
            gchar *path = g_strconcat(table_path, "/", "metadata", "/", "serial", "/", items[0], NULL);
            write_file(path, "0");
            g_free(path);
        }

        g_strfreev(items);
    }

    g_slist_free_full(ddl_create.row, g_free);

    g_free(schema_path);
    g_free(table_path);
}

void execute_ddl_insert(gchar *sql)
{
    struct ddl_parsed ddl_insert = parse_insert(sql);
    GSList *cols = NULL;
    GSList *values = NULL;

    gchar *schema_path = g_strconcat(MULTIDB_SCHEMADIR, "/", ddl_insert.tbl_name, NULL);
    if (!g_file_test(schema_path, G_FILE_TEST_IS_DIR)) {
        fprintf(stderr, "error: schema: %s: does not already exist: %s\n", ddl_insert.tbl_name, schema_path);
        exit(EXIT_FAILURE);
    }

    gchar *table_path = g_strconcat(MULTIDB_TABLESDIR, "/", ddl_insert.tbl_name, NULL);
    if (!g_file_test(table_path, G_FILE_TEST_IS_DIR)) {
        fprintf(stderr, "error: table: %s: does not already exist: %s\n", ddl_insert.tbl_name, table_path);
        exit(EXIT_FAILURE);
    }

    for (cols = ddl_insert.cols; cols; cols = cols->next) {
        gchar *path = g_strconcat(schema_path, "/", cols->data, NULL);
        if (!g_file_test(path, G_FILE_TEST_IS_REGULAR)) {
            fprintf(stderr, "error: schema: [%s]::[%s]: not found: %s\n", ddl_insert.tbl_name, cols->data, path);
            exit(EXIT_FAILURE);
        }
        g_free(path);
    }

    gchar *bucket = next_row_bucket(table_path);

    cols = ddl_insert.cols;
    values = ddl_insert.values;
    while (cols && values) {
        struct stat st;
        gchar *bucket_file = g_strconcat(bucket, "/", cols->data, NULL);
        gchar *serial_file = g_strconcat(table_path, "/", "metadata", "/", "serial", "/", cols->data, NULL);

        // g_print("[%s] -> [%s]\n", cols->data, bucket_file);

        if (0 == stat(serial_file, &st) && 0 == g_ascii_strncasecmp("0", values->data, strlen("0"))) {
            gint serial = next_serial(table_path, serial_file);
            gchar *buf = g_strdup_printf("%i", serial);
            write_file(bucket_file, buf);
            g_free(buf);
        }
        else {
            write_file(bucket_file, values->data);
        }

        g_free(bucket_file);

        cols = cols->next;
        values = values->next;
    }

    g_slist_free_full(ddl_insert.cols, g_free);
    g_slist_free_full(ddl_insert.values, g_free);

    g_free(bucket);
}

gint next_serial(gchar *table_path, gchar *serial_file) 
{
    get_table_lock(table_path);

    gchar *buf;

    read_first_line(serial_file, &buf);

    gint64 serial = g_ascii_strtoll(buf, NULL, 10);
    ++serial;
    g_free(buf);

    buf = g_strdup_printf("%li", serial);
    write_file(serial_file, buf);
    g_free(buf);

    free_table_lock(table_path);

    return(serial);
}

gchar * next_row_bucket(gchar *table_path)
{
    gint roid = next_roid(table_path);

    gchar *roid_file = g_strdup_printf("%i", roid);
    gchar *bucket_dir = g_strdup_printf("%04i", roid % 4096);
    gchar *bucket_path = g_strconcat(table_path, "/", "rows", "/", bucket_dir, "/", roid_file, NULL);

    g_free(roid_file);
    g_free(bucket_dir);

    if (0 != g_mkdir_with_parents(bucket_path, 0775)) {
        fprintf(stderr, "error: g_mkdir_with_parents: %s: %s\n", bucket_path, g_strerror(errno));
        exit(EXIT_FAILURE);
    }

    return(bucket_path);
}

gint next_roid(gchar *table_path)
{
    gchar *roid_file = g_strconcat(table_path, "/", "metadata", "/", "roid", NULL);

    get_table_lock(table_path);

    gchar *buf;

    read_first_line(roid_file, &buf);

    // g_print("cur roid: %s\n", buf);
    gint64 roid = g_ascii_strtoll(buf, NULL, 10);
    ++roid;
    g_free(buf);

    buf = g_strdup_printf("%li", roid);
    write_file(roid_file, buf);
    g_free(buf);

    free_table_lock(table_path);

    return(roid);
}

void get_table_lock(gchar *table_path)
{
    gchar *lock_file = g_strconcat(table_path, "/", "tbl_lock", NULL);
    struct stat st;
    int fd = 0;

    if (0 == stat(lock_file, &st)) {
        fprintf(stderr, "lock file (%s) already exists (%s)\n", lock_file, g_strerror(errno));
        exit(EXIT_FAILURE);
    }

    while ((fd = open(lock_file, O_CREAT|O_RDWR|O_EXCL, 0644)) < 0) {
        if (errno != EEXIST) {
            fprintf(stderr, "error: open(%s): %s\n", lock_file, g_strerror(errno));
            exit(EXIT_FAILURE);
        }

        if (stat(lock_file, &st) == 0 && time(NULL) - st.st_mtime > 120) {
            fprintf(stderr, "error: open(%s): waiting for over 120 seconds\n", lock_file);
            exit(EXIT_FAILURE);
        }

        sleep(1);
    }

    pid_t pid = getpid();

    gchar *converted = g_strdup_printf("%i", pid);
    write_fd(fd, converted, strlen(converted));
    g_free(converted);

    close(fd);

    g_free(lock_file);
}

void free_table_lock(gchar *table_path)
{
    gchar *lock_file = g_strconcat(table_path, "/", "tbl_lock", NULL);

    if (-1 == unlink(lock_file)) {
        fprintf(stderr, "error: unlink(%s): %s\n", lock_file, g_strerror(errno));
        exit(EXIT_FAILURE);
    }

    g_free(lock_file);
}

/*
 * INSERT INTO album (id, name, year) VALUES (0, 'Vacation', 2014);
 */

struct ddl_parsed parse_insert(const gchar *text)
{
    GScanner *scanner;
    
    scanner = g_scanner_new(NULL);
    
    /* feed in the text */
    g_scanner_input_text(scanner, text, strlen(text));
    
    /* give the error handler an idea on how the input is named */
    scanner->input_name = "INSERT INTO";

    int state = STATE_START;
    gchar *_buf = NULL;

    struct ddl_parsed ddl_create = {NULL, NULL, NULL, NULL};

    while (!g_scanner_eof(scanner))
    {
        static int nested = 0;
        GTokenType tokenType = g_scanner_get_next_token(scanner);

        switch (state) {
            case STATE_START: 
                if (G_TOKEN_IDENTIFIER != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }

                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("INSERT", scanner->value.v_identifier, strlen("INSERT"))) {
                        if (NULL == _buf) {
                            _buf = g_strdup("INSERT ");
                        }
                        else {
                            g_scanner_error(scanner, "Unexpected INSERT INTO preamble: %s\n", _buf);
                            exit(EXIT_FAILURE);
                        }
                    }
                    else if (0 == g_ascii_strncasecmp("INTO", scanner->value.v_identifier, strlen("INTO"))) {
                        gchar *t = _buf;
                        _buf = g_strconcat(_buf, scanner->value.v_identifier, NULL);
                        g_free(t);
                            
                        if (0 != g_ascii_strncasecmp("INSERT INTO", _buf, strlen("INSERT INTO"))) {
                            g_scanner_error(scanner, "Unexpected INSERT INTO preamble: %s\n", _buf);
                            exit(EXIT_FAILURE);
                        }

                        g_free(_buf);
                        _buf = NULL;

                        state = STATE_TABLENAME;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
            break;

            case STATE_TABLENAME: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    state = STATE_START_COLS;

                    ddl_create.tbl_name = g_strdup(scanner->value.v_identifier);
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_START_COLS: 
                if (G_TOKEN_LEFT_PAREN == tokenType) {
                    state = STATE_PROCESS_COLS;
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_PROCESS_COLS: 
                if (G_TOKEN_COMMA == tokenType) {
                    /* nothing to do here */
                }
                else if (G_TOKEN_RIGHT_PAREN == tokenType) {
                    state = STATE_VALUES_CONSTANT;
                }
                else if (G_TOKEN_IDENTIFIER == tokenType) {
                    ddl_create.cols = g_slist_append(ddl_create.cols, g_strdup(scanner->value.v_identifier));
                }
            break;

            case STATE_VALUES_CONSTANT: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("VALUES", scanner->value.v_identifier, strlen("VALUES"))) {
                        state = STATE_START_VALUES;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_START_VALUES: 
                if (G_TOKEN_LEFT_PAREN == tokenType) {
                    state = STATE_PROCESS_VALUES;
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_PROCESS_VALUES: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    // g_print("value: %s\n", scanner->value.v_identifier);
                    if (_buf) {
                        gchar *t = _buf;
                        _buf = g_strconcat(_buf, scanner->value.v_identifier, NULL);
                        g_free(t);
                    }
                    else {
                        _buf = g_strdup(scanner->value.v_identifier);
                    }
                }
                else if ('\'' == tokenType) {
                    if (_buf) {
                        gchar *t = _buf;
                        _buf = g_strconcat(_buf, "'", NULL);
                        g_free(t);
                    }
                    else {
                        _buf = g_strdup("'");
                    }
                }
                else if (G_TOKEN_COMMA == tokenType) {
                    ddl_create.values = g_slist_append(ddl_create.values, g_strdup(_buf));
                    g_free(_buf);
                    _buf = NULL;
                }
                else if (G_TOKEN_RIGHT_PAREN == tokenType) {
                    GTokenType nextToken = g_scanner_peek_next_token(scanner);

                    if (';' == nextToken) {
                        state = STATE_END_VALUES;

                        ddl_create.values = g_slist_append(ddl_create.values, g_strdup(_buf));
                        g_free(_buf);
                        _buf = NULL;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else if (G_TOKEN_INT == tokenType) {
                    if (_buf) {
                        gchar *t = _buf;
                        gchar *converted = g_strdup_printf("%li", scanner->value.v_int);
                        _buf = g_strconcat(_buf, converted, NULL);
                        g_free(t);
                        g_free(converted);
                    }
                    else {
                        gchar *converted = g_strdup_printf("%li", scanner->value.v_int);
                        _buf = g_strdup(converted);
                        g_free(converted);
                    }
                }
                else if (G_TOKEN_STRING == tokenType) {
                    if (_buf) {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                    else {
                        _buf = g_strconcat("'", scanner->value.v_string, "'", NULL);
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;
        }
    }

    g_free(_buf);

    return(ddl_create);
}

GDir * dir_open(gchar *path)
{
    GError *error = NULL;
    GDir *dir = g_dir_open(path, 0, &error);
    if (error) {
        return(NULL);
        // fprintf(stderr, "error: g_dir_open [%s]: %s\n", path, error->message);
        // exit(EXIT_FAILURE);
    }

    return(dir);
}

int gslist_tbl_injoin(gconstpointer data, gconstpointer str)
{
    struct ddl_join *join = (struct ddl_join *) data;
    // g_print("{%s} %s: %d\n", join->tbl_name, str, g_ascii_strncasecmp(data, str, strlen(data)));
    return (g_ascii_strncasecmp(join->tbl_name, str, strlen(join->tbl_name)));
}

int gslist_cmp_string(gconstpointer data, gconstpointer str)
{
    // g_print("[%s] %s: %d\n", data, str, g_ascii_strncasecmp(data, str, strlen(data)));
    return (g_ascii_strncasecmp(data, str, strlen(data)));
}

void execute_ddl_select(gchar *sql)
{
    struct ddl_parsed ddl_select = parse_select(sql);
    GSList *cols = NULL;
    GSList *table = NULL;
    GSList *asterisk = NULL;

    /*
    g_print("WHERE [SELECT]: %s\n", ddl_select.where);
    for (GSList *iter = ddl_select.cols; iter; iter = iter->next) {
        g_print("\t[COLS] %s\n", iter->data);
    }
    for (GSList *iter = ddl_select.joins; iter; iter = iter->next) {
        struct ddl_join *join = iter->data;
        g_print("\t[JOIN] %s ON [%s] [%s]\n", join->tbl_name, join->on_left, join->on_right);
    }
    */

    /* Handle the '*' in SELECT */
    while ((asterisk = g_slist_find_custom(ddl_select.cols, "*", gslist_cmp_string))) {
        table = ddl_select.tables;
        while (table) {
            gchar *cols_path = g_strconcat(MULTIDB_SCHEMADIR, "/", table->data, NULL);
            GDir *cols_dir = dir_open(cols_path);
            const gchar *col = g_dir_read_name(cols_dir);

            while (col) {
                ddl_select.cols = g_slist_insert_before(ddl_select.cols, asterisk, g_strconcat(table->data, ".", col, NULL));
                col = g_dir_read_name(cols_dir);
            }

            table = table->next;
            g_free(cols_path);
            g_dir_close(cols_dir);
        }

        gpointer data = asterisk->data;
        ddl_select.cols = g_slist_remove(ddl_select.cols, data);
        g_free(data);
    }

    /* Verify table is in SELECT stmt */
    cols = ddl_select.cols;
    while (cols) {
        if (g_strstr_len(cols->data, strlen(cols->data), ".")) {
            gchar *tbl = g_strdup(cols->data);
            gchar *dot = g_strstr_len(tbl, strlen(tbl), ".");

            tbl[dot - tbl] = '\0';

            if (NULL == g_slist_find_custom(ddl_select.tables, tbl, gslist_cmp_string) &&
                NULL == g_slist_find_custom(ddl_select.joins, tbl, gslist_tbl_injoin)
            ) {
                fprintf(stderr, "error: table [%s] not in SELECT statement\n", tbl);
                exit(EXIT_FAILURE);
            }

            g_free(tbl);
        }
        cols = cols->next;
    }

    /* Print the headers */
    cols = ddl_select.cols;
    while (cols) {
        g_print("%s%s", cols->data, cols->next ? "\t" : "\n");
        cols = cols->next;
    }

    /* Print the rows */
    table = ddl_select.tables;
    while (table) {
        gchar *rows_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", NULL);
        GDir *rows;
        const gchar *bucket;
        if (!g_file_test(rows_path, G_FILE_TEST_IS_DIR)) {
            fprintf(stderr, "error: table: %s: does not exist: %s\n", table->data, rows_path);
            exit(EXIT_FAILURE);
        }

        rows = dir_open(rows_path);
        bucket = g_dir_read_name(rows);

        while (bucket) {
            gchar *row_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", "/", bucket, NULL);
            gchar *schema_path = g_strconcat(MULTIDB_SCHEMADIR, "/", table->data, NULL);
            GDir *row = dir_open(row_path);
            gchar *entry_path;

            const gchar *entry = g_dir_read_name(row);
            while (entry) {
                entry_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", "/", bucket, "/", entry, NULL);

                GHashTable *join_entry_paths = NULL;
                
                if (NULL == ddl_select.joins) {
                     // g_print("[DEFAULT] included_in_join\n");
                    join_entry_paths = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
                    g_hash_table_insert(join_entry_paths, g_strdup(table->data), g_strdup(entry_path));
                }
                else {
                    join_entry_paths = included_in_join(entry_path, ddl_select.joins);
                    guint size = g_hash_table_size(join_entry_paths);
                    if (0 == size) {
                        entry = g_dir_read_name(row);
                        g_free(entry_path);
                        // g_print("join_entry_paths: %p\n", join_entry_paths);
                        g_hash_table_destroy(join_entry_paths);
                        // g_print("[FALSE] included_in_join\n");
                        continue;
                    }
                    else {
                        g_hash_table_insert(join_entry_paths, g_strdup(table->data), g_strdup(entry_path));
                    }
                }

                if (FALSE == included_in_where(join_entry_paths, table->data, ddl_select.where)) {
                    entry = g_dir_read_name(row);
                    g_free(entry_path);
                    // g_print("[FALSE] included_in_where\n");
                    continue;
                }
                else {
                    // g_print("[TRUE] included_in_where\n");
                }

                GDir *entry_dir = dir_open(entry_path);
                if (NULL != entry_dir) {
                    GSList *col = ddl_select.cols;
                    while (col) {
                        struct mdb_col *mdb_col;

                        if (g_strstr_len(col->data, strlen(col->data), ".")) {
                            gchar *filename = g_strdup(col->data);
                            gchar *dot = g_strstr_len(filename, strlen(filename), ".");

                            filename[dot - filename] = '\0';
                            gchar *left = filename;
                            gchar *right = &filename[dot - filename + 1];

                            /* 
                             * Is this col for the current table?
                             */

                            if (NULL == g_strstr_len(left, strlen(left), table->data)) {
                                GHashTable *schema = load_schema(left);
                                const gchar *path = g_hash_table_lookup(join_entry_paths, left);
                                mdb_col = load_mdb_col(left, right, schema, path); /* needs to be a hash lookup */
                                g_hash_table_destroy(schema);
                            }
                            else {
                                GHashTable *schema = load_schema(left);
                                mdb_col = load_mdb_col(left, right, schema, entry_path);
                                g_hash_table_destroy(schema);
                            }

                            g_free(filename);
                        }
                        else {
                            GHashTable *schema = load_schema(table->data);
                            mdb_col = load_mdb_col(table->data, col->data, schema, entry_path);
                            g_hash_table_destroy(schema);
                        }

                        if (MDB_COL_TEXT == mdb_col->col_type) {
                            g_print("%s", mdb_col->v_text);
                        }
                        else if (MDB_COL_INT64 == mdb_col->col_type) {
                            g_print("%ld", mdb_col->v_int64);
                        }

                        free_mdb_col(&mdb_col);

                        col = col->next;
                        if (col) {
                            g_print("\t");
                        }
                        else {
                            g_print("\n");
                        }
                    }

                    g_dir_close(entry_dir);
                }

                if (NULL != join_entry_paths) {
                    g_hash_table_destroy(join_entry_paths);
                }
                g_free(entry_path);
                entry = g_dir_read_name(row);
            }

            g_dir_close(row);
            g_free(row_path);
            g_free(schema_path);

            bucket = g_dir_read_name(rows);
        }

        g_dir_close(rows);
        g_free(rows_path);

        table = table->next;
    }

    g_slist_free_full(ddl_select.cols, g_free);
    g_slist_free_full(ddl_select.tables, g_free);
}

/*
 * SELECT * FROM site_key;
 * SELECT id, site_key, updated FROM site_key;
 * SELECT site_key FROM site_key;
 */

struct ddl_parsed parse_select(const gchar *text)
{
    GScanner *scanner;

    scanner = g_scanner_new(NULL);

    scanner->config->scan_identifier_1char = TRUE;
    scanner->config->cset_identifier_nth = G_CSET_a_2_z "_0123456789." G_CSET_A_2_Z G_CSET_LATINS G_CSET_LATINC;

    /* feed in the text */
    g_scanner_input_text(scanner, text, strlen(text));
    
    /* give the error handler an idea on how the input is named */
    scanner->input_name = "SELECT";

    int state = STATE_START;
    gchar *_buf = NULL;

    struct ddl_parsed ddl_select = {NULL, NULL, NULL, NULL, NULL, NULL, NULL};

    while (!g_scanner_eof(scanner))
    {
        static int nested = 0;
        GTokenType tokenType = g_scanner_get_next_token(scanner);
        GTokenType nextToken;

        gchar *converted = NULL;
        gchar *t = NULL;

        // g_print("%s %s [%d]\n", tickGTokenType(tokenType), G_TOKEN_IDENTIFIER == tokenType ? scanner->value.v_identifier : "", state);

        if (G_TOKEN_ERROR == tokenType) {
            g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
            exit(EXIT_FAILURE);
        }

        switch (state) {
            case STATE_START: 
                if (G_TOKEN_IDENTIFIER != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }

                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 != g_ascii_strncasecmp("SELECT", scanner->value.v_identifier, strlen("SELECT"))) {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }

                    nextToken = g_scanner_peek_next_token(scanner);

                    if ('*' == nextToken || G_TOKEN_IDENTIFIER == nextToken) {
                        state = STATE_PROCESS_COLS;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
            break;

            case STATE_PROCESS_COLS: 
                if ('*' == tokenType) {
                    ddl_select.cols = g_slist_append(ddl_select.cols, g_strdup("*"));
                }
                else if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (NULL == _buf) {
                        _buf = g_strdup(scanner->value.v_identifier);
                    }
                } 
                else if (G_TOKEN_COMMA == tokenType) {
                    if (_buf) {
                        ddl_select.cols = g_slist_append(ddl_select.cols, g_strdup(_buf));
                        g_free(_buf);
                        _buf = NULL;
                    }
                }
                else if ('.' == tokenType) {
                    if (_buf) {
                        gchar *t = _buf;
                        _buf = g_strconcat(_buf, ".", NULL);
                        g_free(t);
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }

                nextToken = g_scanner_peek_next_token(scanner);
                if (G_TOKEN_IDENTIFIER == nextToken) {
                    if (0 == g_ascii_strncasecmp("FROM", scanner->next_value.v_identifier, strlen("FROM"))) {
                        if (_buf) {
                            ddl_select.cols = g_slist_append(ddl_select.cols, g_strdup(_buf));
                            g_free(_buf);
                            _buf = NULL;
                        }

                        state = STATE_FROM;
                    }
                }
            break;

            case STATE_FROM: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("FROM", scanner->next_value.v_identifier, strlen("FROM"))) {
                        state = STATE_TABLENAME;
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_TABLENAME: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    ddl_select.tables = g_slist_append(ddl_select.tables, g_strdup(scanner->value.v_identifier));

                    nextToken = g_scanner_peek_next_token(scanner);

                    if (';' == nextToken) {
                        state = STATE_END;
                    }
                    else if (G_TOKEN_IDENTIFIER == nextToken) {
                        if (0 == g_ascii_strncasecmp("WHERE", scanner->next_value.v_identifier, strlen("WHERE"))) {
                            state = STATE_WHERE;
                        }
                        else if (0 == g_ascii_strncasecmp("INNER", scanner->next_value.v_identifier, strlen("INNER"))) {
                            state = STATE_INNER_JOIN;
                        }
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_INNER_JOIN:
                if (NULL == _buf && G_TOKEN_IDENTIFIER == tokenType && 0 == g_ascii_strncasecmp("INNER", scanner->value.v_identifier, strlen("INNER"))) {
                    continue;
                }
                else if (NULL == _buf && G_TOKEN_IDENTIFIER == tokenType && 0 == g_ascii_strncasecmp("JOIN", scanner->value.v_identifier, strlen("JOIN"))) {
                    continue;
                }

                nextToken = g_scanner_peek_next_token(scanner);

                if (';' == nextToken) {
                    state = STATE_END;
                }

                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("WHERE", scanner->value.v_identifier, strlen("WHERE"))) {
                        state = STATE_WHERE;
                        continue;
                    }
                }

                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 != g_ascii_strncasecmp("ON", scanner->value.v_identifier, strlen("ON"))) {
                        GSList *cur_join = g_slist_last(ddl_select.joins);

                        if (NULL == ddl_select.joins) {
                            ddl_select.joins = g_slist_append(ddl_select.joins, g_malloc0(sizeof(struct ddl_join)));

                            ((struct ddl_join *) ddl_select.joins->data)->join_type = MDB_JOIN_INNER;

                            cur_join = g_slist_last(ddl_select.joins);
                        }
                        else if (NULL != ((struct ddl_join *) cur_join->data)->on_right) {
                            ddl_select.joins = g_slist_append(ddl_select.joins, g_malloc0(sizeof(struct ddl_join)));

                            ((struct ddl_join *) ddl_select.joins->data)->join_type = MDB_JOIN_INNER;

                            cur_join = g_slist_last(ddl_select.joins);
                        }

                        struct ddl_join *join = cur_join->data;

                        if (NULL == join->tbl_name) {
                            join->tbl_name = g_strdup(scanner->value.v_identifier);
                        }
                        else if (NULL == join->on_left) {
                            join->on_left = g_strdup(scanner->value.v_identifier);
                        }
                        else if (NULL == join->on_right) {
                            join->on_right = g_strdup(scanner->value.v_identifier);
                        }
                    }
                }
            break;

            case STATE_WHERE:
                extract_where(scanner, tokenType, &_buf, &state);
            break;

            case STATE_END:
                if (';' == tokenType) {
                    if (_buf) {
                        ddl_select.where = g_strdup(_buf);
                        g_free(_buf);
                        _buf = NULL;
                    }
                }

                if (';' != tokenType && G_TOKEN_EOF != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;
        }
    }

    g_free(_buf);

    return(ddl_select);
}

gboolean is_precedence_less_or_equal(gchar *a, GList *elem)
{
    gchar *b = elem->data;
    // "not" always comes first
    if (0 == g_ascii_strncasecmp("not", b, strlen("not"))) {
        return TRUE;
    }

    if (0 == g_ascii_strncasecmp("not", a, strlen("not"))) {
        return FALSE;
    }

    if (0 == g_ascii_strncasecmp("or", a, strlen("or")) && 
        0 == g_ascii_strncasecmp("and", b, strlen("and"))) {
        return TRUE;
    }

    if (0 == g_ascii_strncasecmp("and", a, strlen("and")) && 
        0 == g_ascii_strncasecmp("or", b, strlen("or"))) {
        return FALSE;
    }

    // otherwise they're equal
    return TRUE;
}

gboolean is_operator(gchar *cmp)
{
    if (0 == g_ascii_strncasecmp("not", cmp, strlen("not"))) {
        return TRUE;
    }

    if (0 == g_ascii_strncasecmp("and", cmp, strlen("and"))) {
        return TRUE;
    }

    if (0 == g_ascii_strncasecmp("or", cmp, strlen("or"))) {
        return TRUE;
    }

    return FALSE;
}

void iterator(gpointer key, gpointer value, gpointer user_data)
{
    g_print(user_data, key, value);
}

gboolean eval_where_expression(GHashTable *paths, const gchar *def_tbl, const gchar *ex, gboolean *stale)
{
    gchar *entry_path = g_hash_table_lookup(paths, def_tbl);

    // g_print("ex: %s: entry_path: %s\n", ex, entry_path);

    if (0 == g_ascii_strncasecmp("0", ex, strlen("0"))) {
        return FALSE;
    }
    else if (0 == g_ascii_strncasecmp("1", ex, strlen("1"))) {
        return TRUE;
    }

    const gchar *table = NULL;
    gchar *dot = g_strstr_len(ex, strlen(ex), ".");
    gchar *t;
    if (dot) {
        t = g_strdup(&ex[dot - ex + 1]);
        table = g_strndup(ex, dot - ex);
        // g_print("table: %s\n", table);
    }
    else {
        table = def_tbl;
        t = g_strdup(ex);
    }
    gchar *not_equals = g_strstr_len(t, strlen(t), "!=");
    gchar *less_than_equals = g_strstr_len(t, strlen(t), "<=");
    gchar *greater_than_equals = g_strstr_len(t, strlen(t), ">=");
    gchar *equals = g_strstr_len(t, strlen(t), "=");
    gchar *greater_than = g_strstr_len(t, strlen(t), ">");
    gchar *less_than = g_strstr_len(t, strlen(t), "<");
    gchar *is_null = g_strstr_len(t, strlen(t), "IS NULL");

    gboolean ret = FALSE;

    *stale = FALSE;

    /*
    gchar *col = NULL;
    if (g_strstr_len(col_name, strlen(col_name), ".")) {
        gchar *dot = g_strstr_len(col_name, strlen(col_name), ".");
        col = g_strdup(&col_name[dot - col_name + 1]);
    }
    else {
        col = g_strdup(col_name);
    }
    */

    /* REFACTOR - consolidate */

    if (less_than_equals) {
        t[less_than_equals - t] = '\0';
        gchar *left = t;
        gchar *right = &t[less_than_equals - t + 2];
        
        // g_print("left[%s]: %s: right: %s\n", g_hash_table_lookup(schema, left), left, right);

        gchar *buf;
        gchar *file = g_strconcat(entry_path, "/", left, NULL);

        read_first_line(file, &buf);
        if (NULL == buf) {
            *stale = TRUE;
            g_free(t);
            return(FALSE);
        }

        if (0 == g_ascii_strncasecmp("'", right, strlen("'"))) { /* string */
            if (0 >= g_ascii_strncasecmp(right, buf, strlen(left))) {
                ret = TRUE;
            }
        }
        else {  /* number */
            gint64 r_number = g_ascii_strtoll(right, NULL, 10);
            gint64 db_number = g_ascii_strtoll(buf, NULL, 10);

            if (r_number >= db_number) {
                ret = TRUE;
            }
        }
    }
    else if (greater_than_equals) {
        t[greater_than_equals - t] = '\0';
        gchar *left = t;
        gchar *right = &t[greater_than_equals - t + 2];
        
        // g_print("left[%s]: %s: right: %s\n", g_hash_table_lookup(schema, left), left, right);

        gchar *buf;
        gchar *file = g_strconcat(entry_path, "/", left, NULL);

        read_first_line(file, &buf);
        if (NULL == buf) {
            *stale = TRUE;
            g_free(t);
            return(FALSE);
        }

        if (0 == g_ascii_strncasecmp("'", right, strlen("'"))) { /* string */
            if (0 <= g_ascii_strncasecmp(right, buf, strlen(left))) {
                ret = TRUE;
            }
        }
        else {  /* number */
            gint64 r_number = g_ascii_strtoll(right, NULL, 10);
            gint64 db_number = g_ascii_strtoll(buf, NULL, 10);

            if (r_number <= db_number) {
                ret = TRUE;
            }
        }
    }
    else if (not_equals) {
        t[not_equals - t] = '\0';
        gchar *left = t;
        gchar *right = &t[not_equals - t + 2];
        
        // g_print("left[%s]: %s: right: %s\n", g_hash_table_lookup(schema, left), left, right);

        gchar *buf;
        gchar *file = g_strconcat(entry_path, "/", left, NULL);

        read_first_line(file, &buf);
        if (NULL == buf) {
            *stale = TRUE;
            g_free(t);
            return(FALSE);
        }

        if (0 == g_ascii_strncasecmp("'", right, strlen("'"))) { /* string */
            if (0 != g_ascii_strncasecmp(right, buf, strlen(left))) {
                ret = TRUE;
            }
        }
        else {  /* number */
            gint64 r_number = g_ascii_strtoll(right, NULL, 10);
            gint64 db_number = g_ascii_strtoll(buf, NULL, 10);

            if (r_number != db_number) {
                ret = TRUE;
            }
        }
    }
    else if (greater_than) {
        t[greater_than - t] = '\0';
        gchar *left = t;
        gchar *right = &t[greater_than - t + 1];
        
        // g_print("left[%s]: %s: right: %s\n", g_hash_table_lookup(schema, left), left, right);

        gchar *buf;
        gchar *file = g_strconcat(entry_path, "/", left, NULL);

        read_first_line(file, &buf);
        if (NULL == buf) {
            *stale = TRUE;
            g_free(t);
            return(FALSE);
        }

        if (0 == g_ascii_strncasecmp("'", right, strlen("'"))) { /* string */
            if (0 < g_ascii_strncasecmp(right, buf, strlen(left))) {
                ret = TRUE;
            }
        }
        else {  /* number */
            gint64 r_number = g_ascii_strtoll(right, NULL, 10);
            gint64 db_number = g_ascii_strtoll(buf, NULL, 10);

            if (r_number < db_number) {
                ret = TRUE;
            }
        }
    }
    else if (less_than) {
        t[less_than - t] = '\0';
        gchar *left = t;
        gchar *right = &t[less_than - t + 1];
        
        // g_print("left[%s]: %s: right: %s\n", g_hash_table_lookup(schema, left), left, right);

        gchar *buf;
        gchar *file = g_strconcat(entry_path, "/", left, NULL);

        read_first_line(file, &buf);
        if (NULL == buf) {
            *stale = TRUE;
            g_free(t);
            return(FALSE);
        }

        if (0 == g_ascii_strncasecmp("'", right, strlen("'"))) { /* string */
            if (0 > g_ascii_strncasecmp(right, buf, strlen(left))) {
                ret = TRUE;
            }
        }
        else {  /* number */
            gint64 r_number = g_ascii_strtoll(right, NULL, 10);
            gint64 db_number = g_ascii_strtoll(buf, NULL, 10);

            if (r_number > db_number) {
                ret = TRUE;
            }
        }
    }
    else if (equals) {
        t[equals - t] = '\0';
        gchar *left = t;
        gchar *right = &t[equals - t + 1];
        
        // g_print("left[%s]: %s: right: %s\n", g_hash_table_lookup(schema, left), left, right);

        gchar *buf;
        gchar *file = g_strconcat(entry_path, "/", left, NULL);

        read_first_line(file, &buf);
        if (NULL == buf) {
            *stale = TRUE;
            g_free(t);
            return(FALSE);
        }

        if (0 == g_ascii_strncasecmp("'", right, strlen("'"))) { /* string */
            if (0 == g_ascii_strncasecmp(right, buf, strlen(left))) {
                ret = TRUE;
            }
        }
        else {  /* number */
            gint64 r_number = g_ascii_strtoll(right, NULL, 10);
            gint64 db_number = g_ascii_strtoll(buf, NULL, 10);

            if (r_number == db_number) {
                ret = TRUE;
            }
        }
    }
    else if (is_null) {
        t[(is_null - 1) - t] = '\0';  /* Just the col that is null */


        gchar *buf;
        gchar *file = g_strconcat(g_hash_table_lookup(paths, table), "/", t, NULL);
        read_first_line(file, &buf);

        if (NULL == buf) {
            *stale = TRUE;
            g_free(t);
            return(FALSE);
        }

        if (0 == g_ascii_strncasecmp(buf, "NULL", strlen(buf))) { /* string */
            ret = TRUE;
        }
    }
    else {
        fprintf(stderr, "error: invalid expresstion: %s\n", ex);
        exit(EXIT_FAILURE);
    }

    g_free(t);

    return(ret);
}

GHashTable * load_schema(gchar *table)
{
    gchar *schema_path = g_strconcat(MULTIDB_SCHEMADIR, "/", table, NULL);

    if (!g_file_test(schema_path, G_FILE_TEST_IS_DIR)) {
        fprintf(stderr, "error: schema: %s: does not exist\n", schema_path);
        exit(EXIT_FAILURE);
    }

    GDir *schema_dir = dir_open(schema_path);
    const gchar *schema_entry = g_dir_read_name(schema_dir);

    GHashTable *schema = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);

    while (schema_entry) {
        gchar *buf;
        gchar *file = g_strconcat(schema_path, "/", schema_entry, NULL);

        read_first_line(file, &buf);
        g_hash_table_insert(schema, g_strdup(schema_entry), g_strdup(buf));

        g_free(file);
        g_free(buf);

        schema_entry = g_dir_read_name(schema_dir);
    }

    g_dir_close(schema_dir);

    g_free(schema_path);

    return(schema);
}

struct mdb_col *load_mdb_col(gchar *table, gchar *col_name, GHashTable *schema, const gchar *entry_path)
{
    struct mdb_col *mdb_col = g_malloc0(sizeof(struct mdb_col));
    
    /* 
     * Support col and table.col
     */

    gchar *col = NULL;
    if (g_strstr_len(col_name, strlen(col_name), ".")) {
        gchar *dot = g_strstr_len(col_name, strlen(col_name), ".");
        col = g_strdup(&col_name[dot - col_name + 1]);
    }
    else {
        col = g_strdup(col_name);
    }

    /* 
     * Get the value
     */

    gchar *buf;
    gchar *file = g_strconcat(entry_path, "/", col, NULL);

    read_first_line(file, &buf);
    if (NULL == buf) {
        mdb_col->stale = TRUE;
        g_free(col);

        return(mdb_col);
    }
    else {
        mdb_col->stale = FALSE;
    }

    // g_print("[%s]: %s: %s\n", g_hash_table_lookup(schema, col), col, buf);
    const gchar *type = g_hash_table_lookup(schema, col);

    if (0 == g_ascii_strncasecmp("serial", type, strlen("serial"))) {
        gint64 serial = g_ascii_strtoll(buf, NULL, 10);
        mdb_col->v_int64 = serial;
        mdb_col->col_type = MDB_COL_INT64;
    }
    else if (0 == g_ascii_strncasecmp("integer", type, strlen("integer"))) {
        gint64 integer = g_ascii_strtoll(buf, NULL, 10);
        mdb_col->v_int64 = integer;
        mdb_col->col_type = MDB_COL_INT64;
    }
    else if (0 == g_ascii_strncasecmp("text", type, strlen("text"))) {
        mdb_col->v_text = g_strdup(buf);
        mdb_col->col_type = MDB_COL_TEXT;
    }

    if (buf) {
        g_free(buf);
    }
    g_free(col);

    return(mdb_col);
}

void free_mdb_col(struct mdb_col **mdb_col)
{
    if ((*mdb_col)->stale) {
        g_free(*mdb_col);
        *mdb_col = NULL;
        return;
    }

    if (MDB_COL_TEXT == (*mdb_col)->col_type) {
        g_free((*mdb_col)->v_text);
        g_free(*mdb_col);
        *mdb_col = NULL;
        return;
    }

    if (MDB_COL_INT64 == (*mdb_col)->col_type) {
        g_free(*mdb_col);
        *mdb_col = NULL;
        return;
    }
}

gchar * sequential_scan(struct ddl_join *join, gchar *entry_path)
{
    gchar *left_table = NULL;
    gchar *right_table = NULL;

    gchar *dot;

    dot = g_strstr_len(join->on_left, strlen(join->on_left), ".");
    left_table = g_strndup(join->on_left, dot - join->on_left);
    
    dot = g_strstr_len(join->on_right, strlen(join->on_right), ".");
    right_table = g_strndup(join->on_right, dot - join->on_right);

    GHashTable *left_schema = load_schema(left_table);
    GHashTable *right_schema = load_schema(right_table);
    // g_print("right_schema: %s\n", right_schema_path);

    struct mdb_col *left_col = load_mdb_col(left_table, join->on_left, left_schema, entry_path);

    struct mdb_tbl_scanner *right = NULL;
    init_scan_table(&right, join->tbl_name);

    gchar *ret = NULL;

    while (scan_table(right)) {
        for (GSList *iter = right->cols; iter; iter = iter->next) {
            gchar *right_name = g_strconcat(right_table, ".", basename(iter->data), NULL);
            
            if (0 == g_ascii_strncasecmp(right_name, join->on_right, strlen(right_name))) {
                struct mdb_col *right_col = load_mdb_col(right_table, basename(iter->data), right_schema, dirname(iter->data));

                // g_print("scan_table: %s: %s\n", right_name, join->on_right);

                if (MDB_COL_TEXT == left_col->col_type) {
                    // g_print("[%s]=[%s] {%s}\n", left_col->v_text, right_col->v_text, right->entry_path);
                    if (0 == g_ascii_strncasecmp(left_col->v_text, right_col->v_text, strlen(left_col->v_text))) {
                        ret = g_strdup(right->entry_path);
                        free_mdb_col(&right_col);
                        g_free(right_name);

                        break;
                    }
                }
                else if (MDB_COL_INT64 == left_col->col_type) {
                    // g_print("[%ld]=[%ld] {%s}\n", left_col->v_int64, right_col->v_int64, right->entry_path);
                    if (left_col->v_int64 == right_col->v_int64) {
                        ret = g_strdup(right->entry_path);
                        free_mdb_col(&right_col);
                        g_free(right_name);

                        break;
                    }
                }

                free_mdb_col(&right_col);
            }

            g_free(right_name);
        }

        if (right->cols) {
            g_slist_free_full(right->cols, g_free);
            right->cols = NULL;
        }

        if (right->entry_path) {
            g_free(right->entry_path);
            right->entry_path = NULL;
        }

        if (ret) {
            break;
        }
    }

    free_mdb_col(&left_col);
    final_scan_table(&right);

    g_hash_table_destroy(left_schema);
    g_hash_table_destroy(right_schema);

    return ret;
}

void init_scan_table(struct mdb_tbl_scanner **scan, gchar *table)
{
    if (NULL == *scan) {
        *scan = g_malloc0(sizeof(struct mdb_tbl_scanner));

        (*scan)->table = g_strdup(table);

        gchar *rows_path = g_strconcat(MULTIDB_TABLESDIR, "/", table, "/", "rows", NULL);

        if (!g_file_test(rows_path, G_FILE_TEST_IS_DIR)) {
            fprintf(stderr, "error: table: %s: does not exist: %s\n", table, rows_path);
            exit(EXIT_FAILURE);
        }

        (*scan)->rows = dir_open(rows_path);
        (*scan)->cols = NULL;
        (*scan)->bucket_dir = NULL;
        (*scan)->entry_path = NULL;
        
        g_free(rows_path);
    }
    else {
        fprintf(stderr, "error: init_scan_table called on already initialized scanner\n");
        exit(EXIT_FAILURE);
    }
}

void final_scan_table(struct mdb_tbl_scanner **scan)
{
    g_free((*scan)->table);
    if ((*scan)->entry_path) {
        g_free((*scan)->entry_path);
    }
    g_dir_close((*scan)->rows);
    g_free(*scan);

    *scan = NULL;
}

/*
 * One row at a time
 */

gboolean scan_table(struct mdb_tbl_scanner *scan)
{
    /* 
     * Remeber, a bucket can have multiple entries (rows)
     */

    const gchar *bucket = NULL;
    if (NULL == scan->bucket_dir) {
        bucket = g_dir_read_name(scan->rows);

        if (NULL == bucket) {
            return FALSE;
        }

        gchar *bucket_path = g_strconcat(MULTIDB_TABLESDIR, "/", scan->table, "/", "rows", "/", bucket, NULL);
        scan->bucket_dir = dir_open(bucket_path);
        g_free(bucket_path);

        if (NULL == scan->bucket_dir) {
            fprintf(stderr, "error: dir_open: %s: %d\n", bucket_path, __LINE__);
            exit(EXIT_FAILURE);
        }
    }

    const gchar *entry = g_dir_read_name(scan->bucket_dir);
    if (entry) {
        scan->entry_path = g_strconcat(MULTIDB_TABLESDIR, "/", scan->table, "/", "rows", "/", bucket, "/", entry, NULL);

        GDir *entry_dir = dir_open(scan->entry_path);
        if (NULL != entry_dir) { /* could have been deleted */
            const gchar *col;

            while ((col = g_dir_read_name(entry_dir))) {
                scan->cols = g_slist_append(scan->cols, g_strconcat(MULTIDB_TABLESDIR, "/", scan->table, "/", "rows", "/", bucket, "/", entry, "/", col, NULL));
            }

            g_dir_close(entry_dir);
        }
    }
    else {
        g_dir_close(scan->bucket_dir);
        scan->bucket_dir = NULL;
    }

    return TRUE;
}

GHashTable * included_in_join(gchar *entry_path, GSList *joins)
{
    if (NULL == joins) {
        return(NULL);
    }

    // gchar *ret = NULL;
    GHashTable *paths = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);

    for (GSList *iter = joins; iter; iter = iter->next) {
        struct ddl_join *join = iter->data;
        gchar *ret;

        if ((ret = sequential_scan(join, entry_path))) {
            // g_print("\t%s ON [%s]=[%s] [%s]\n", join->tbl_name, join->on_left, join->on_right, ret);
            // g_print("paths: %s: %s [%s]\n", join->tbl_name, ret, entry_path);
            g_hash_table_insert(paths, g_strdup(join->tbl_name), g_strdup(ret));
            g_free(ret);
        }
    }

    return(paths);
}

gboolean included_in_where(GHashTable *paths, gchar *table, gchar *where_clause)
{
    if (NULL == where_clause) {
        return TRUE;
    }

    GSList *rpn = parse_where(where_clause);

    // g_print("schema_path: %s\n", schema_path);
    // g_print("entry_path: %s\n", g_hash_table_lookup(paths, table));
    // g_print("where_clause: %s\n", where_clause);
    // g_hash_table_foreach(paths, (GHFunc)iterator, "%s -> %s\n");

    // GHashTable *schema = load_schema(table);

    /* 
     * Eval the RPN from db values
     */

    GList *stack = NULL;

    gboolean stale;

    // No AND, OR, or ?NOT? in the where clause
    if (1 == g_slist_length(rpn)) {
        gboolean ans = eval_where_expression(paths, table, rpn->data, &stale);
        if (stale) {
            g_slist_free_full(rpn, g_free);

            return(FALSE);
        }

        rpn = g_slist_remove(rpn, rpn->data);

        if (ans) {
            stack = g_list_append(stack, g_strdup("1"));
        }
        else {
            stack = g_list_append(stack, g_strdup("0"));
        }
    }

    GSList *iter_slist;
    for (iter_slist = rpn; iter_slist; iter_slist = iter_slist->next) {
        if (0 == g_ascii_strncasecmp("AND", iter_slist->data, strlen("AND")) ||
            0 == g_ascii_strncasecmp("OR", iter_slist->data, strlen("OR"))
        ) {
            GList *elem = NULL;
            gchar *left;
            gchar *right;

            if (NULL == stack) {
                fprintf(stderr, "error: Incomplete AND or OR expression\n");
                exit(EXIT_FAILURE);
            }
            elem = g_list_last(stack);
            left = g_strdup(elem->data);
            stack = g_list_remove(stack, elem->data);

            if (NULL == stack) {
                fprintf(stderr, "error: Incomplete AND or OR expression\n");
                exit(EXIT_FAILURE);
            }
            elem = g_list_last(stack);
            right = g_strdup(elem->data);
            stack = g_list_remove(stack, elem->data);

            gboolean ans_left = eval_where_expression(paths, table, left, &stale);
            if (stale) {
                g_slist_free_full(rpn, g_free);

                return(FALSE);
            }

            gboolean ans_right = eval_where_expression(paths, table, right, &stale);
            if (stale) {
                g_slist_free_full(rpn, g_free);

                return(FALSE);
            }

            if (0 == g_ascii_strncasecmp("AND", iter_slist->data, strlen("AND"))) {
                if (ans_left && ans_right) {
                    stack = g_list_append(stack, g_strdup("1"));
                }
                else {
                    stack = g_list_append(stack, g_strdup("0"));
                }
            }
            else if (0 == g_ascii_strncasecmp("OR", iter_slist->data, strlen("OR"))) {
                if (ans_left || ans_right) {
                    stack = g_list_append(stack, g_strdup("1"));
                }
                else {
                    stack = g_list_append(stack, g_strdup("0"));
                }
            }
        }
        else if (0 == g_ascii_strncasecmp("NOT", iter_slist->data, strlen("NOT"))) {
            GSList *right = NULL;
            fprintf(stderr, "error: NOT isn't implemented\n");
            exit(EXIT_FAILURE);
        }
        else {
            stack = g_list_append(stack, g_strdup(iter_slist->data));
        }
        // g_print("%s\n", iter_slist->data);
    }

    if (1 != g_list_length(stack)) {
        fprintf(stderr, "error: RPN not evaluated correctly: %s\n", g_hash_table_lookup(paths, table));
        exit(EXIT_FAILURE);
    }

    gboolean ret = FALSE;
    if (0 == g_ascii_strncasecmp("1", stack->data, strlen("1"))) {
        ret = TRUE;
    }

    // g_hash_table_foreach(schema, (GHFunc)iterator, "%s is a %s\n");

    // g_print("\n");
    // g_print("%s\n", g_hash_table_lookup(schema, "id"));

    g_slist_free_full(rpn, g_free);
    // g_hash_table_destroy(schema);

    return(ret);
}

GSList * parse_where(gchar *where_clause)
{
    GScanner *scanner;
    
    scanner = g_scanner_new(NULL);
    scanner->config->scan_identifier_1char = TRUE;
    scanner->config->cset_identifier_nth = G_CSET_a_2_z "_0123456789." G_CSET_A_2_Z G_CSET_LATINS G_CSET_LATINC;

    // g_print("%s\n", where_clause);
    
    /* feed in the text */
    g_scanner_input_text(scanner, where_clause, strlen(where_clause));
    
    /* give the error handler an idea on how the input is named */
    scanner->input_name = "WHERE";

    gchar *_buf = NULL;

    struct ddl_parsed ddl_select = {NULL, NULL, NULL, NULL};

    // http://en.wikipedia.org/wiki/Shunting-yard_algorithm (sort of)

    GSList *output = NULL;
    GList *stack = NULL;

    gchar *converted = NULL;

    while (!g_scanner_eof(scanner))
    {
        static int nested = 0;
        GTokenType tokenType = g_scanner_get_next_token(scanner);
        GTokenType nextToken;

        gchar *t = NULL;

        // g_print("%s %s\n", tickGTokenType(tokenType), G_TOKEN_IDENTIFIER == tokenType ? scanner->value.v_identifier : "");

        switch ((int)tokenType) {
            case G_TOKEN_IDENTIFIER: 
                if (NULL == _buf && !is_operator(scanner->value.v_identifier)) {
                    _buf = g_strconcat(scanner->value.v_identifier, NULL);
                }
                else if (is_operator(scanner->value.v_identifier)) {
                    if (_buf) {
                        output = g_slist_append(output, g_strdup(_buf));
                        g_free(_buf);
                        _buf = NULL;
                    }

                    GList *elem = g_list_last(stack);
                    while (elem && is_operator(elem->data) && 
                        is_precedence_less_or_equal(scanner->value.v_identifier, elem))
                    {
                        output = g_slist_append(output, g_strdup(elem->data));
 
                        gpointer data = elem->data;
                        stack = g_list_remove(stack, elem->data);
                        g_free(data);

                        elem = g_list_last(stack);
                    }

                    stack = g_list_append(stack, g_strdup(scanner->value.v_identifier));
                }
                else {
                    if (0 == g_ascii_strncasecmp("IS", scanner->value.v_identifier, strlen("IS")) ||
                        0 == g_ascii_strncasecmp("NULL", scanner->value.v_identifier, strlen("NULL")))
                    {
                        t = _buf;
                        _buf = g_strconcat(_buf, " ", scanner->value.v_identifier, NULL);
                        g_free(t);
                    }
                    else {
                        t = _buf;
                        _buf = g_strconcat(_buf, scanner->value.v_identifier, NULL);
                        g_free(t);
                    }
                }
            break;

            case G_TOKEN_LEFT_PAREN: 
                if (NULL == converted) {
                    stack = g_list_append(stack, g_strdup("("));
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case G_TOKEN_RIGHT_PAREN: 
                {
                    if (_buf) {
                        output = g_slist_append(output, g_strdup(_buf));
                        g_free(_buf);
                        _buf = NULL;
                    }

                    GList *elem = g_list_last(stack);
                    while (elem && 0 != g_ascii_strncasecmp("(", elem->data, strlen("("))) {
                        output = g_slist_append(output, g_strdup(elem->data));

                        gpointer data = elem->data;
                        stack = g_list_remove(stack, elem->data);
                        g_free(data);

                        elem = g_list_last(stack);
                    }

                    if (NULL == elem) {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                    else if (0 == g_ascii_strncasecmp("(", elem->data, strlen("("))) {
                        gpointer data = elem->data;
                        stack = g_list_remove(stack, elem->data);
                        g_free(data);
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
            break;

            case G_TOKEN_STRING: 
                if (NULL == converted) {
                    converted = g_strdup_printf("'%s'", scanner->value.v_string);
                }
                else {
                    gchar *t = converted;
                    converted = g_strdup_printf("%s'%s'", t, scanner->value.v_string);
                    g_free(t);
                }

                if (NULL == _buf) {
                    _buf = g_strconcat(converted, NULL);
                }
                else {
                    t = _buf;
                    _buf = g_strconcat(_buf, converted, NULL);
                    g_free(t);
                }
                g_free(converted);
                converted = NULL;
            break;

            case G_TOKEN_EQUAL_SIGN: 
                if (NULL == converted) {
                    converted = g_strdup_printf("%s", "=");
                }
                else {
                    gchar *t = converted;
                    converted = g_strdup_printf("%s%s", t, "=");
                    g_free(t);
                }

                if (NULL == _buf) {
                    _buf = g_strconcat(converted, NULL);
                }
                else {
                    t = _buf;
                    _buf = g_strconcat(_buf, converted, NULL);
                    g_free(t);
                }
                g_free(converted);
                converted = NULL;
            break;

            case '>': 
                if (NULL == converted) {
                    converted = g_strdup_printf("%s", ">");
                }

                nextToken = g_scanner_peek_next_token(scanner);
                if (G_TOKEN_EQUAL_SIGN != nextToken) {
                    if (NULL == _buf) {
                        _buf = g_strconcat(converted, NULL);
                    }
                    else {
                        t = _buf;
                        _buf = g_strconcat(_buf, converted, NULL);
                        g_free(t);
                    }
                    g_free(converted);
                    converted = NULL;
                }
            break;

            case '<': 
                if (NULL == converted) {
                    converted = g_strdup_printf("%s", "<");
                }

                nextToken = g_scanner_peek_next_token(scanner);
                if (G_TOKEN_EQUAL_SIGN != nextToken) {
                    if (NULL == _buf) {
                        _buf = g_strconcat(converted, NULL);
                    }
                    else {
                        t = _buf;
                        _buf = g_strconcat(_buf, converted, NULL);
                        g_free(t);
                    }
                    g_free(converted);
                    converted = NULL;
                }
            break;

            case '!': 
                if (NULL == converted) {
                    converted = g_strdup_printf("%s", "!");
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case G_TOKEN_INT: 
                converted = g_strdup_printf("%li", scanner->value.v_int);
                t = _buf;
                _buf = g_strconcat(_buf, converted, NULL);
                g_free(t);
                g_free(converted);
                converted = NULL;
            break;
        }
    }

    if (_buf) {
        output = g_slist_append(output, g_strdup(_buf));
        g_free(_buf);
        _buf = NULL;
    }

    GList *iter_list;
    for (iter_list = g_list_last(stack); iter_list; iter_list = iter_list->prev) {
        if (0 == g_ascii_strncasecmp("(", iter_list->data, strlen("("))) {
            g_scanner_unexp_token(scanner, G_TOKEN_LEFT_PAREN, NULL, "symbol", NULL, NULL, TRUE);
            exit(EXIT_FAILURE);
        }
        output = g_slist_append(output, g_strdup(iter_list->data));
    }

    g_list_free_full(stack, g_free);

    /*
    GSList *iter_slist;
    for (iter_slist = output; iter_slist; iter_slist = iter_slist->next) {
        g_print("output: %s\n", iter_slist->data);
    }
    */

    return(output);
}

/*
 * DELETE FROM site_key WHERE id = 3;
 */

struct ddl_parsed parse_delete(const gchar *text)
{
    GScanner *scanner;
    
    scanner = g_scanner_new(NULL);
    
    /* feed in the text */
    g_scanner_input_text(scanner, text, strlen(text));
    
    /* give the error handler an idea on how the input is named */
    scanner->input_name = "DELETE";

    int state = STATE_START;
    gchar *_buf = NULL;

    struct ddl_parsed ddl_delete = {NULL, NULL, NULL, NULL};

    while (!g_scanner_eof(scanner))
    {
        GTokenType tokenType = g_scanner_get_next_token(scanner);
        GTokenType nextToken;

        gchar *converted = NULL;
        gchar *t = NULL;

        // g_print("%s %s\n", tickGTokenType(tokenType), G_TOKEN_IDENTIFIER == tokenType ? scanner->value.v_identifier : "");

        switch (state) {
            case STATE_START: 
                if (G_TOKEN_IDENTIFIER != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }

                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 != g_ascii_strncasecmp("DELETE", scanner->value.v_identifier, strlen("DELETE"))) {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }

                    nextToken = g_scanner_peek_next_token(scanner);

                    if (G_TOKEN_IDENTIFIER == nextToken) {
                        state = STATE_FROM;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
            break;

            case STATE_FROM: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("FROM", scanner->next_value.v_identifier, strlen("FROM"))) {
                        state = STATE_TABLENAME;
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_TABLENAME: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    ddl_delete.tables = g_slist_append(ddl_delete.tables, g_strdup(scanner->value.v_identifier));

                    nextToken = g_scanner_peek_next_token(scanner);

                    if (';' == nextToken) {
                        state = STATE_END;
                    }
                    else if (G_TOKEN_IDENTIFIER == nextToken) {
                        if (0 == g_ascii_strncasecmp("WHERE", scanner->next_value.v_identifier, strlen("WHERE"))) {
                            state = STATE_WHERE;
                        }
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_WHERE:
                extract_where(scanner, tokenType, &_buf, &state);
            break;

            case STATE_END:
                if (';' == tokenType) {
                    if (_buf) {
                        ddl_delete.where = g_strdup(_buf);
                        g_free(_buf);
                        _buf = NULL;
                    }
                }

                if (';' != tokenType && G_TOKEN_EOF != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;
        }
    }

    g_free(_buf);

    return(ddl_delete);
}

void execute_ddl_delete(gchar *sql)
{
    struct ddl_parsed ddl_delete = parse_delete(sql);

    GSList *table = NULL;
    GSList *purgatory = NULL;

    // g_print("WHERE [DELETE]: %s\n", ddl_delete.where);

    /* Delete the rows */
    table = ddl_delete.tables;
    
    gchar *rows_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", NULL);
    if (!g_file_test(rows_path, G_FILE_TEST_IS_DIR)) {
        fprintf(stderr, "error: table: %s: does not exist: %s\n", table->data, rows_path);
        exit(EXIT_FAILURE);
    }

    GDir *rows = dir_open(rows_path);
    const gchar *bucket = g_dir_read_name(rows);

    gboolean found_one_entry = FALSE;

    gchar *schema_path = g_strconcat(MULTIDB_SCHEMADIR, "/", table->data, NULL);

    while (bucket) {
        gchar *row_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", "/", bucket, NULL);
        GDir *row = dir_open(row_path);

        gchar *purgatory_path = g_strconcat(MULTIDB_PURGATORYDIR, "/", table->data, NULL);

        const gchar *entry = g_dir_read_name(row);

        while (entry) {
            gchar *entry_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", "/", bucket, "/", entry, NULL);
            gchar *purgatory_entry = g_strconcat(purgatory_path, "/", entry, NULL);

            GHashTable *paths = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
            g_hash_table_insert(paths, g_strdup(table->data), g_strdup(entry_path));

            if (FALSE == included_in_where(paths, table->data, ddl_delete.where)) {
                entry = g_dir_read_name(row);
                g_free(entry_path);
                g_hash_table_destroy(paths);
                continue;
            }
            g_hash_table_destroy(paths);

            found_one_entry = TRUE;
            // g_print("DELETE: %s [%s]\n", entry_path, purgatory_path);

            if (0 != g_mkdir_with_parents(purgatory_path, 0775)) {
                fprintf(stderr, "error: g_mkdir_with_parents: %s: %s\n", purgatory_path, g_strerror(errno));
                exit(EXIT_FAILURE);
            }

            if (0 != g_rename(entry_path, purgatory_entry)) {
                fprintf(stderr, "error: g_rename: %s -> %s: %s\n", entry_path, purgatory_path, g_strerror(errno));
                exit(EXIT_FAILURE);
            }

            purgatory = g_slist_append(purgatory, g_strdup(purgatory_entry));

            g_free(entry_path);
            g_free(purgatory_entry);
            entry = g_dir_read_name(row);
        }

        g_dir_close(row);
        g_free(row_path);

        bucket = g_dir_read_name(rows);
    }

    if (found_one_entry) {
        purgatory = g_slist_append(purgatory, g_strconcat(MULTIDB_PURGATORYDIR, "/", table->data, NULL));
    }

    GSList *iterator = NULL;
    for (iterator = purgatory; iterator; iterator = iterator->next) {

        /* 
         * Remove the files in the directory
         */

        GDir *schema_dir = dir_open(schema_path);
        const gchar *schema_entry = g_dir_read_name(schema_dir);
        while (schema_entry) {
            gchar *file = g_strconcat(iterator->data, "/", schema_entry, NULL);

            if (g_file_test(file, G_FILE_TEST_IS_REGULAR)) {
                if (0 != g_remove(file)) {
                    fprintf(stderr, "error: g_remove: %s: %s\n", file, g_strerror(errno));
                    exit(EXIT_FAILURE);
                }
            }

            g_free(file);
            schema_entry = g_dir_read_name(schema_dir);
        }

        /* 
         * Remove the directory
         */
        if (0 != g_remove(iterator->data)) {
            fprintf(stderr, "error: g_remove: %s: %s\n", iterator->data, g_strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    g_free(schema_path);
    g_slist_free_full(purgatory, g_free);

    g_dir_close(rows);
    g_free(rows_path);
}

/*
 * UPDATE site_key SET id = 4 WHERE id = 3;
 */

struct ddl_parsed parse_update(const gchar *text)
{
    GScanner *scanner;
    
    scanner = g_scanner_new(NULL);
    
    /* feed in the text */
    g_scanner_input_text(scanner, text, strlen(text));
    
    /* give the error handler an idea on how the input is named */
    scanner->input_name = "UPDATE";

    int state = STATE_START;
    gchar *_buf = NULL;

    struct ddl_parsed ddl_update = {NULL, NULL, NULL, NULL};

    while (!g_scanner_eof(scanner))
    {
        GTokenType tokenType = g_scanner_get_next_token(scanner);
        GTokenType nextToken = g_scanner_peek_next_token(scanner);

        gchar *converted = NULL;
        gchar *t = NULL;

        // g_print("%s %s [%d]\n", tickGTokenType(tokenType), G_TOKEN_IDENTIFIER == tokenType ? scanner->value.v_identifier : "", state);

        switch (state) {
            case STATE_START: 
                if (G_TOKEN_IDENTIFIER != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }

                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 != g_ascii_strncasecmp("UPDATE", scanner->value.v_identifier, strlen("UPDATE"))) {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }

                    if (G_TOKEN_IDENTIFIER == nextToken) {
                        state = STATE_TABLENAME;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
            break;

            case STATE_TABLENAME: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    ddl_update.tables = g_slist_append(ddl_update.tables, g_strdup(scanner->value.v_identifier));

                    if (G_TOKEN_IDENTIFIER == nextToken) {
                        state = STATE_SET;
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_SET: 
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("SET", scanner->value.v_identifier, strlen("SET"))) {
                        state = STATE_UPDATE;
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_UPDATE:
                if (G_TOKEN_IDENTIFIER == tokenType) {
                    if (0 == g_ascii_strncasecmp("WHERE", scanner->value.v_identifier, strlen("WHERE"))) {
                        if (_buf) {
                            ddl_update.cols = g_slist_append(ddl_update.cols, g_strdup(_buf));
                            g_free(_buf);
                            _buf = NULL;
                        }

                        state = STATE_WHERE;
                    }
                    else {
                        if (0 == g_ascii_strncasecmp("null", scanner->value.v_identifier, strlen("null"))) {
                            if (_buf) {
                                gchar *t = _buf;
                                _buf = g_strconcat(_buf, g_strdup("NULL"), NULL);
                                g_free(t);
                            }
                            else {
                                _buf = g_strdup("NULL");
                            }
                        }
                        else {
                            if (_buf) {
                                gchar *t = _buf;
                                _buf = g_strconcat(_buf, scanner->value.v_identifier, NULL);
                                g_free(t);
                            }
                            else {
                                _buf = g_strdup(scanner->value.v_identifier);
                            }
                        }
                    }
                }
                else if (G_TOKEN_COMMA == tokenType || ';' == tokenType) {
                    ddl_update.cols = g_slist_append(ddl_update.cols, g_strdup(_buf));
                    g_free(_buf);
                    _buf = NULL;
                }
                else if (G_TOKEN_EQUAL_SIGN == tokenType) {
                    if (_buf) {
                        gchar *t = _buf;
                        _buf = g_strconcat(_buf, "=", NULL);
                        g_free(t);
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else if (G_TOKEN_INT == tokenType) {
                    if (_buf) {
                        gchar *t = _buf;
                        gchar *converted = g_strdup_printf("%li", scanner->value.v_int);
                        _buf = g_strconcat(_buf, converted, NULL);
                        g_free(t);
                        g_free(converted);
                    }
                    else {
                        gchar *converted = g_strdup_printf("%li", scanner->value.v_int);
                        _buf = g_strdup(converted);
                        g_free(converted);
                    }
                }
                else if (G_TOKEN_STRING == tokenType) {
                    if (_buf) {
                        gchar *t = _buf;
                        _buf = g_strconcat(_buf, "'", scanner->value.v_string, "'", NULL);
                        g_free(t);
                    }
                    else {
                        g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                        exit(EXIT_FAILURE);
                    }
                }
                else {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;

            case STATE_WHERE:
                extract_where(scanner, tokenType, &_buf, &state);
            break;

            case STATE_END:
                if (';' == tokenType) {
                    if (_buf) {
                        ddl_update.where = g_strdup(_buf);
                        g_free(_buf);
                        _buf = NULL;
                    }
                }

                if (';' != tokenType && G_TOKEN_EOF != tokenType) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
            break;
        }
    }

    g_free(_buf);

    return(ddl_update);
}

void execute_ddl_update(gchar *sql)
{
    struct ddl_parsed ddl_update = parse_update(sql);

    GSList *table = NULL;
    GSList *purgatory = NULL;

    // g_print("WHERE [UPDATE]: %s\n", ddl_update.where);
    /*
    for (GSList *iterator = ddl_update.cols; iterator; iterator = iterator->next) {
        g_print("\t%s\n", iterator->data);
    }
    */

    /* Update the rows */
    table = ddl_update.tables;
    
    gchar *rows_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", NULL);
    if (!g_file_test(rows_path, G_FILE_TEST_IS_DIR)) {
        fprintf(stderr, "error: table: %s: does not exist: %s\n", table->data, rows_path);
        exit(EXIT_FAILURE);
    }

    GDir *rows = dir_open(rows_path);
    const gchar *bucket = g_dir_read_name(rows);

    gchar *schema_path = g_strconcat(MULTIDB_SCHEMADIR, "/", table->data, NULL);

    while (bucket) {
        gchar *row_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", "/", bucket, NULL);
        GDir *row = dir_open(row_path);

        const gchar *entry = g_dir_read_name(row);

        while (entry) {
            gchar *entry_path = g_strconcat(MULTIDB_TABLESDIR, "/", table->data, "/", "rows", "/", bucket, "/", entry, NULL);
            GHashTable *paths = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
            g_hash_table_insert(paths, g_strdup(table->data), g_strdup(entry_path));

            if (FALSE == included_in_where(paths, table->data, ddl_update.where)) {
                entry = g_dir_read_name(row);
                g_free(entry_path);
                g_hash_table_destroy(paths);
                continue;
            }
            g_hash_table_destroy(paths);

            for (GSList *iterator = ddl_update.cols; iterator; iterator = iterator->next) {
                gchar *t = g_strdup(iterator->data);

                gchar *equals = g_strstr_len(t, strlen(t), "=");
                t[equals - t] = '\0';
                gchar *left = t;
                gchar *right = &t[equals - t + 1];

                gchar *path = g_strconcat(entry_path, "/", left, NULL);

                if (!g_file_test(path, G_FILE_TEST_IS_REGULAR)) {
                    fprintf(stderr, "error: table: [%s]::[%s]: not found: %s\n", table->data, left, path);
                    exit(EXIT_FAILURE);
                }

                write_file(path, right);
            }
            

            g_free(entry_path);
            entry = g_dir_read_name(row);
        }

        g_dir_close(row);
        g_free(row_path);

        bucket = g_dir_read_name(rows);
    }

    g_free(schema_path);
    g_slist_free_full(purgatory, g_free);

    g_dir_close(rows);
    g_free(rows_path);
}

void extract_where(GScanner *scanner, GTokenType tokenType, gchar **_buf, int *state)
{
    GTokenType nextToken;

    gchar *converted = NULL;
    gchar *t = NULL;

    nextToken = g_scanner_peek_next_token(scanner);

    if (';' == nextToken) {
        *state = STATE_END;
    }

    if (NULL == *_buf && G_TOKEN_IDENTIFIER == tokenType && 0 == g_ascii_strncasecmp("WHERE", scanner->value.v_identifier, strlen("WHERE"))) {
        return;
    }

    // g_print("%s %s [%d]\n", tickGTokenType(tokenType), G_TOKEN_IDENTIFIER == tokenType ? scanner->value.v_identifier : "", *state);

    switch ((int)tokenType) {
        case G_TOKEN_IDENTIFIER: 
            if (NULL == *_buf) {
                *_buf = g_strconcat(scanner->value.v_identifier, NULL);
            }
            else if (
                    0 == g_ascii_strncasecmp("AND", scanner->value.v_identifier, strlen("AND")) ||
                    0 == g_ascii_strncasecmp("OR", scanner->value.v_identifier, strlen("OR")) ||
                    0 == g_ascii_strncasecmp("NOT", scanner->value.v_identifier, strlen("NOT"))
            ) {
                t = *_buf;
                *_buf = g_strconcat(*_buf, " ", scanner->value.v_identifier, " ", NULL);
                g_free(t);
            }
            else if (
                    0 == g_ascii_strncasecmp("IS", scanner->value.v_identifier, strlen("IS")) ||
                    0 == g_ascii_strncasecmp("NULL", scanner->value.v_identifier, strlen("NULL"))
            ) {
                if (NULL == *_buf) {
                    g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                    exit(EXIT_FAILURE);
                }
                gchar *identifier = g_ascii_strup(scanner->value.v_identifier, -1);
                *_buf = g_strconcat(*_buf, " ", identifier, NULL);
                g_free(identifier);
            }
            else {
                t = *_buf;
                *_buf = g_strconcat(*_buf, scanner->value.v_identifier, NULL);
                g_free(t);
            }
        break;

        case G_TOKEN_LEFT_PAREN: 
            if (NULL == converted) {
                converted = g_strdup_printf("%s", "(");
            }

            if (NULL == *_buf) {
                *_buf = g_strconcat(converted, NULL);
            }
            else {
                t = *_buf;
                *_buf = g_strconcat(*_buf, converted, NULL);
                g_free(t);
            }
            g_free(converted);
            converted = NULL;
        break;

        case G_TOKEN_RIGHT_PAREN: 
            if (NULL == converted) {
                converted = g_strdup_printf("%s", ")");
            }

            if (NULL == *_buf) {
                *_buf = g_strconcat(converted, NULL);
            }
            else {
                t = *_buf;
                *_buf = g_strconcat(*_buf, converted, NULL);
                g_free(t);
            }
            g_free(converted);
            converted = NULL;
        break;

        case G_TOKEN_EQUAL_SIGN: 
            if (NULL == converted) {
                converted = g_strdup_printf("%s", "=");
            }
            else {
                gchar *t = converted;
                converted = g_strdup_printf("%s%s", t, "=");
                g_free(t);
            }

            if (NULL == *_buf) {
                *_buf = g_strconcat(converted, NULL);
            }
            else {
                t = *_buf;
                *_buf = g_strconcat(*_buf, converted, NULL);
                g_free(t);
            }
            g_free(converted);
            converted = NULL;
        break;

        case '>': 
            if (NULL == converted) {
                converted = g_strdup_printf("%s", ">");
            }

            nextToken = g_scanner_peek_next_token(scanner);
            if (G_TOKEN_EQUAL_SIGN != nextToken) {
                if (NULL == *_buf) {
                    *_buf = g_strconcat(converted, NULL);
                }
                else {
                    t = *_buf;
                    *_buf = g_strconcat(*_buf, converted, NULL);
                    g_free(t);
                }
                g_free(converted);
                converted = NULL;
            }
        break;

        case '<': 
            if (NULL == converted) {
                converted = g_strdup_printf("%s", "<");
            }

            nextToken = g_scanner_peek_next_token(scanner);
            if (G_TOKEN_EQUAL_SIGN != nextToken) {
                if (NULL == *_buf) {
                    *_buf = g_strconcat(converted, NULL);
                }
                else {
                    t = *_buf;
                    *_buf = g_strconcat(*_buf, converted, NULL);
                    g_free(t);
                }
                g_free(converted);
                converted = NULL;
            }
        break;

        case '!': 
            if (NULL == converted) {
                converted = g_strdup_printf("%s", "!");
            }
            else {
                g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
                exit(EXIT_FAILURE);
            }
        break;

        case G_TOKEN_INT: 
            converted = g_strdup_printf("%li", scanner->value.v_int);
            t = *_buf;
            *_buf = g_strconcat(*_buf, converted, NULL);
            g_free(t);
            g_free(converted);
            converted = NULL;
        break;

        case G_TOKEN_STRING: 
            converted = g_strdup_printf("'%s'", scanner->value.v_string);
            t = *_buf;
            *_buf = g_strconcat(*_buf, converted, NULL);
            g_free(t);
            g_free(converted);
            converted = NULL;
        break;

        default:
            g_scanner_unexp_token(scanner, tokenType, NULL, "symbol", NULL, g_strdup_printf("Line: %d", __LINE__), TRUE);
            exit(EXIT_FAILURE);
        break;
    }
}
