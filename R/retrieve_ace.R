# library(ROracle)
# library(dplyr)
# library(dbplyr)
# library(DBI)
# library(glue)
# library(lubridate)
# source("~/sasdata1/park/retrieve/utils.R")



#' Extrait les consultations externes à l'hôpital.
#'
#' - Sélectionne les codes prestations (ACT_COD) parmi : C et CS.
#' - TODO: Implémente des filtres qualités sur les codes retours
#'
#' @param start_date Date de début de la période
#' @param end_date Date de fin de la période
#' @param spe_codes Codes spécialités
#' @param ben_table_name Nom de la table BEN
#' @param output_table_name Nom de la table de sortie
#' @param r_output_path Chemin de sauvegarde des données R
#'
#' @return Tables MCO_FCSTC||MCO_CSC pour une année donnée
#'
#' @export
extract_hospital_consultations <- function(
    start_date = NULL,
    end_date = NULL,
    spe_codes = NULL,
    ben_table_name = NULL,
    output_table_name = NULL,
    r_output_path = NULL) {
  conn <- initialize_connection() # Connect to database

  consultation_act_codes <- c("C", "CS")

  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(end_date)
  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")

  for (year in start_year:end_year) {
    start_time <- Sys.time()
    print(glue("Processing year: {year}"))
    formatted_year <- sprintf("%02d", year %% 100)

    ben <- tbl(conn, ben_table_name)
    cstc <- tbl(conn, glue("T_MCO{formatted_year}CSTC"))
    fcstc <- tbl(conn, glue("T_MCO{formatted_year}FCSTC"))

    fcstc <- fcstc %>%
      select(ETA_NUM, SEQ_NUM, ACT_COD, EXE_SPE) %>%
      distinct()

    ace <- cstc %>%
      select(ETA_NUM, SEQ_NUM, NIR_ANO_17, EXE_SOI_DTD) %>%
      distinct() %>%
      filter(EXE_SOI_DTD >= TO_DATE(formatted_start_date, "YYYY-MM-DD") &
        EXE_SOI_DTD <= TO_DATE(formatted_end_date, "YYYY-MM-DD")) %>%
      left_join(fcstc, by = c("ETA_NUM", "SEQ_NUM")) %>%
      select(NIR_ANO_17, EXE_SOI_DTD, ACT_COD, EXE_SPE) %>%
      filter(ACT_COD %in% consultation_act_codes) %>%
      distinct()

    if (!is.null(spe_codes)) {
      ace <- ace %>%
        filter(EXE_SPE %in% spe_codes)
    }

    ben <- ben %>%
      select(BEN_IDT_ANO, BEN_NIR_PSA) %>%
      distinct()

    query <- ben %>%
      inner_join(ace, by = c("BEN_NIR_PSA" = "NIR_ANO_17")) %>%
      select(BEN_IDT_ANO, EXE_SOI_DTD, ACT_COD, EXE_SPE) %>%
      distinct()

    create_table_or_insert_from_query(conn = conn, output_table_name = output_table_name, query = query)

    end_time <- Sys.time()
    print(glue("Time taken for year {year}: {round(difftime(end_time, start_time, units='mins'),1)} mins."))
  }

  if (!is.null(r_output_path)) {
    # Save the table to a R data file
    query <- tbl(conn, output_table_name)
    data <- collect(query)
    saveRDS(data, glue("{r_output_path}/{tolower(output_table_name)}.RDS"))
  }

  dbDisconnect(conn)
}
