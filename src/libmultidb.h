#ifndef __MULTIDB_H
#define __MULTIDB_H

gchar *MULTIDB_PREFIX;
gchar *MULTIDB_BASEDIR;

gchar *MULTIDB_DATADIR;
gchar *MULTIDB_SCHEMADIR;
gchar *MULTIDB_TABLESDIR;
gchar *MULTIDB_PURGATORYDIR;

// #define MULTIDB_BASEDIR "multidb"

// #define MULTIDB_DATADIR MULTIDB_BASEDIR"/data"
// #define MULTIDB_SCHEMADIR MULTIDB_BASEDIR"/data/schema"
// #define MULTIDB_TABLESDIR MULTIDB_BASEDIR"/data/tables"
// #define MULTIDB_PURGATORYDIR MULTIDB_BASEDIR"/data/purgatory"

typedef enum {
    MDB_JOIN_INNER
} MdbJoinType;

struct ddl_join {
    MdbJoinType join_type;
    gchar *tbl_name;
    gchar *on_left;
    gchar *on_right;
};

struct ddl_parsed {
    gchar *tbl_name;
    GSList *row;
    GSList *cols;
    GSList *values;
    GSList *tables;
    GSList *joins;
    gchar *where;
};

typedef enum {
    MDB_COL_TEXT,
    MDB_COL_INT64,
} MdbColumnType;

struct mdb_col {
    MdbColumnType col_type;
    gboolean stale;
    gint64 v_int64;
    gchar *v_text;
};

struct mdb_tbl_scanner {
    GDir *rows;
    GDir *bucket_dir;
    gchar *table;
    gchar *entry_path;
    GSList *cols;
};

void mdb_init(void);
struct ddl_parsed parse_create(const gchar *text);
struct ddl_parsed parse_insert(const gchar *text);
struct ddl_parsed parse_select(const gchar *text);
void execute_ddl_create(gchar *sql);
void execute_ddl_insert(gchar *sql);
void execute_ddl_select(gchar *sql);
void get_table_lock(gchar *table_path);
void free_table_lock(gchar *table_path);
gchar * next_row_bucket(gchar *table_path);
gint next_roid(gchar *table_path);
void read_first_line(const gchar *path, gchar **buf);
gint next_serial(gchar *table_path, gchar *serial_file);
gboolean included_in_where(GHashTable *paths, gchar *table, gchar *where_clause);
GSList * parse_where(gchar *where_clause);
void execute_ddl_delete(gchar *sql);
void execute_ddl_update(gchar *sql);
GHashTable * included_in_join(gchar *entry_path, GSList *joins);
void init_scan_table(struct mdb_tbl_scanner **scan, gchar *table);
void final_scan_table(struct mdb_tbl_scanner **scan);
gboolean scan_table(struct mdb_tbl_scanner *scan);
GHashTable * load_schema(gchar *table);
struct mdb_col *load_mdb_col(gchar *table, gchar *col_name, GHashTable *schema, const gchar *entry_path);
void free_mdb_col(struct mdb_col **mdb_col);
void extract_where(GScanner *scanner, GTokenType tokenType, gchar **_buf, int *state);

#endif
