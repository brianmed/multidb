#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <glib.h>
#include <errno.h>

#include "libmultidb.h"

static gchar *sql_create = NULL;
static gchar *sql_insert = NULL;
static gchar *sql_select = NULL;
static gchar *sql_delete = NULL;
static gchar *sql_update = NULL;
// static gint max_size = 8;
// static gboolean verbose = FALSE;
// static gboolean beep = FALSE;
// static gboolean randomize = FALSE;

static GOptionEntry entries[] = {
  { "sql_create", 0, 0, G_OPTION_ARG_STRING, &sql_create, "A CREATE statement", NULL },
  { "sql_insert", 0, 0, G_OPTION_ARG_STRING, &sql_insert, "An INSERT statement", NULL },
  { "sql_select", 0, 0, G_OPTION_ARG_STRING, &sql_select, "A SELECT statement", NULL },
  { "sql_delete", 0, 0, G_OPTION_ARG_STRING, &sql_delete, "A DELETE statement", NULL },
  { "sql_update", 0, 0, G_OPTION_ARG_STRING, &sql_update, "An UPDATE statement", NULL },
  // { "max-size", 0, 0, G_OPTION_ARG_INT, &max_size, "Test up to 2^M items", "M" },
  // { "verbose", 0, 0, G_OPTION_ARG_NONE, &verbose, "Be verbose", NULL },
  // { "beep", 0, 0, G_OPTION_ARG_NONE, &beep, "Beep when done", NULL },
  // { "rand", 0, 0, G_OPTION_ARG_NONE, &randomize, "Randomize the data", NULL },
  { NULL }
};

int main(int argc, char *argv[])
{
    GError *error = NULL;
    GOptionContext *context;
    
    context = g_option_context_new("- multidb command-line helper");
    g_option_context_add_main_entries(context, entries, NULL);

    if (!g_option_context_parse(context, &argc, &argv, &error)) {
        g_print("option parsing failed: %s\n", error->message);
        exit(EXIT_FAILURE);
    }

    mdb_init();

    if (sql_create) {
        execute_ddl_create(sql_create);
    }
    else if (sql_insert) {
        execute_ddl_insert(sql_insert);
    }
    else if (sql_select) {
        execute_ddl_select(sql_select);
    }
    else if (sql_delete) {
        execute_ddl_delete(sql_delete);
    }
    else if (sql_update) {
        execute_ddl_update(sql_update);
    }

    return(EXIT_SUCCESS);
}
