#' Initialisation de la connexion à la base de données.
#' @return Connexion à la base de données
#'
#' @export
initialize_connection <- function() {
  Sys.setenv(TZ = "Europe/Paris")
  Sys.setenv(ORA_SDTZ = "Europe/Paris")
  drv <- dbDriver("Oracle")
  conn <- dbConnect(drv, dbname = "IPIAMPR2.WORLD")
  return(conn)
}

#' Création d'une table à partir d'une requête SQL.
#' @param conn Connexion à la base de données
#' @param output_table_name Nom de la table de sortie
#' @param query Requête SQL
#' @return NULL
#'
#' @export
create_table_from_query <- function(
    conn = NULL,
    output_table_name = NULL,
    query = NULL) {
  query <- sql_render(query)
  temp_table_name <- paste0(output_table_name, "_TMP")
  dbExecute(conn, glue("CREATE TABLE {temp_table_name} AS {query}"))
  if (dbExistsTable(conn, output_table_name)) {
    dbRemoveTable(conn, output_table_name)
  }
  dbExecute(conn, glue("CREATE TABLE {output_table_name} AS SELECT * FROM {temp_table_name}"))
  dbRemoveTable(conn, temp_table_name)
}

#' Création d'une table à partir d'une requête SQL ou insertion des résultats dans une table existante.
#' @param conn Connexion à la base de données
#' @param output_table_name Nom de la table de sortie
#' @param query Requête SQL
#' @return NULL
#'
#' @export
create_table_or_insert_from_query <- function(
    conn = NULL,
    output_table_name = NULL,
    query = NULL) {
  query <- sql_render(query)
  if (dbExistsTable(conn, output_table_name)) {
    dbExecute(conn, glue("INSERT INTO {output_table_name} {query}"))
  } else {
    dbExecute(conn, glue("CREATE TABLE {output_table_name} AS {query}"))
  }
}

#' Récupération de l'année non archivée la plus ancienne.
#' @param conn Connexion à la base de données
#' @return Année non archivée la plus ancienne
#'
#' @export
get_first_non_archived_year <- function(conn) {
  tables_names <- dbGetQuery(conn, "SELECT object_name FROM all_objects WHERE object_name LIKE 'ER_PRS_F_%'")

  archived_years <- tables_names %>%
    mutate(year = substr(OBJECT_NAME, 10, 13)) %>%
    filter(year != "TEST") %>%
    pull(year)

  first_non_archived_year <- as.character(max(as.numeric(archived_years)) + 1)
  return(first_non_archived_year)
}
