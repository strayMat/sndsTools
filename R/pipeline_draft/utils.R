initialize_connection <- function() {
  drv <- dbDriver("Oracle")
  conn <- dbConnect(drv, dbname = "IPIAMPR2.WORLD")
  Sys.setenv(TZ = "Europe/Paris")
  Sys.setenv(ORA_SDTZ = "Europe/Paris")
  return(conn)
}

create_table_from_query <- function(
    conn = NULL,
    output_table_name = NULL,
    query = NULL,
    overwrite = TRUE) {
  query <- sql_render(query)
  if (overwrite & dbExistsTable(conn, output_table_name)) {
    dbRemoveTable(conn, output_table_name)
  }
  dbExecute(conn, glue("CREATE TABLE {output_table_name} AS {query}"))
}

