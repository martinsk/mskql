#ifndef CATALOG_H
#define CATALOG_H

#include "database.h"

/* Refresh all pg_catalog and information_schema virtual tables.
 * Creates/replaces __pg_class, __pg_namespace, __pg_attribute, __pg_type,
 * __pg_index, __pg_attrdef, __pg_constraint, __pg_database, __pg_roles,
 * __pg_settings, __information_schema_tables, __information_schema_columns
 * as real struct table entries in db->tables.
 * Should be called before executing any query that references catalog tables. */
void catalog_refresh(struct database *db);

/* Remove all catalog tables from the database */
void catalog_cleanup(struct database *db);

/* Check if a table name is a catalog table (starts with __pg_ or __information_schema_) */
int catalog_is_catalog_table(const char *name);

/* Map a schema-qualified name to internal catalog table name.
 * e.g. "pg_catalog.pg_class" -> "pg_class", "pg_class" -> "pg_class"
 * Returns the bare table name (pointing into the input or a static string),
 * or NULL if this is not a catalog table reference.
 * If schema is "pg_catalog", strips it. If schema is "information_schema",
 * returns "__information_schema_<table>". Bare pg_* names are passed through. */
const char *catalog_resolve_name(const char *schema, size_t schema_len,
                                  const char *table, size_t table_len,
                                  char *buf, size_t bufsz);

#endif
