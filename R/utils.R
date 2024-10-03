#' Initialisation de la connexion à la base de données.
#'
#' @return dbConnection Connexion à la base de données oracle
#'
#' @export
connect_oracle <- function() {
  require(ROracle)
  Sys.setenv(TZ = "Europe/Paris")
  Sys.setenv(ORA_SDTZ = "Europe/Paris")
  drv <- DBI::dbDriver("Oracle")
  conn <- DBI::dbConnect(drv, dbname = "IPIAMPR2.WORLD")

  return(conn)
}

#' Initialisation de la connexion à la base de données duckdb.
#'
#' Utilisation pour le testing uniquement. Si le code s'exécute en dehors du portail, il faut initier une connexion duckdb pour
#' effectuer les tests.
#'
#' @return dbConnection Connexion à la base de données duckdb
#'
#' @export
connect_duckdb <- function() {
  print(
    "Le code ne s'exécute pas sur le portail CNAM. Initialisation d'une connexion duckdb en mémoire."
  )
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  return(conn)
}

#' Création d'une table à partir d'une requête SQL.
#' @details
#' La fonction crée une table sous Oracle à partir d'une requête SQL.
#' Si la table `output_table_name` existe déjà, elle est écrasée si
#' le paramètre `overwrite` est TRUE.
#' @param conn Connexion à la base de données
#' @param output_table_name Nom de la table de sortie
#' @param query Requête SQL
#' @param overwrite Logical. Indique si la table `output_table_name`
#' doit être écrasée dans le cas où elle existe déjà.
#' @return NULL
#'
#' @export
create_table_from_query <- function(conn = NULL,
                                    output_table_name = NULL,
                                    query = NULL,
                                    overwrite = FALSE) {
  stopifnot(
    !DBI::dbExistsTable(conn, output_table_name) || (DBI::dbExistsTable(conn, output_table_name) && overwrite)
  )
  if (DBI::dbExistsTable(conn, output_table_name) && overwrite) {
    DBI::dbRemoveTable(conn, output_table_name)
  }
  query <- dbplyr::sql_render(query)
  DBI::dbExecute(
    conn,
    glue::glue(
      "CREATE TABLE {output_table_name} AS {query}"
    )
  )
}

#' Insertion des résultats d'une requête SQL dans une table existante.
#' @param conn Connexion à la base de données
#' @param output_table_name Nom de la table de sortie
#' @param query Requête SQL
#' @return NULL
#'
#' @export
insert_into_table_from_query <- function(
    conn = NULL,
    output_table_name = NULL,
    query = NULL) {
  stopifnot(DBI::dbExistsTable(conn, output_table_name))
  query <- dbplyr::sql_render(query)
  DBI::dbExecute(
    conn,
    glue::glue("INSERT INTO {output_table_name} {query}")
  )
}

#' Création d'une table à partir d'une requête SQL ou insertion des résultats dans une table existante.
#' @param conn Connexion à la base de données
#' @param output_table_name Nom de la table de sortie
#' @param query Requête SQL
#' @return NULL
#'
#' @export
create_table_or_insert_from_query <- function(conn = NULL,
                                              output_table_name = NULL,
                                              query = NULL,
                                              append = FALSE) {
  query <- dbplyr::sql_render(query)
  if (DBI::dbExistsTable(conn, output_table_name)) {
    if (append) {
      DBI::dbExecute(
        conn,
        glue::glue("INSERT INTO {output_table_name} {query}")
      )
    } else {
      stop(glue::glue("La table {output_table_name} existe déjà et le paramètre append est FALSE."))
    }
  } else {
    DBI::dbExecute(
      conn,
      glue::glue("CREATE TABLE {output_table_name} AS {query}")
    )
  }
}

#' Récupération de l'année non archivée la plus ancienne de la table ER_PRS_F.
#' @param conn Connexion à la base de données
#' @return Année non archivée la plus ancienne
#'
#' @export
get_first_non_archived_year <- function(conn) {
  tables_names <-
    dbGetQuery(
      conn,
      "SELECT object_name FROM all_objects WHERE object_name LIKE 'ER_PRS_F_%'"
    )
  dbGetQuery(
    conn,
    "SELECT object_name FROM all_objects WHERE object_name LIKE 'ER_PRS_F_%'"
  )

  archived_years <- tables_names |>
    dplyr::mutate(year = substr(OBJECT_NAME, 10, 13)) |>
    filter(year != "TEST") |>
    dplyr::pull(year)

  first_non_archived_year <-
    as.character(max(as.numeric(archived_years)) + 1)
  return(first_non_archived_year)
}

#' Récupération des statistiques des tables
#' @param conn Connexion à la base de données
#' @param table Chaine de caractère indiquant le nom d'une table
#' @references https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_STATS.html#GUID-CA6A56B9-0540-45E9-B1D7-D78769B7714C
gather_table_stats <- function(conn, table) {
  user <- dbGetQuery(con, "SELECT user FROM dual;")
  user <- dbQuoteIdentifier(con, user$USER)
  dbExecute(
    conn,
    "BEGIN DBMS_STATS.GATHER_TABLE_STATS(?, ?); END;",
    params = list(user, table)
  )
}
