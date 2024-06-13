# library(ROracle)
# library(dplyr)
# library(dbplyr)
# library(DBI)
# library(glue)
# library(lubridate)
# source("/sasdata/prd/users/44a001280010899/park/retrieve/utils.R")


extract_consultations <- function(
    start_date = NULL,
    end_date = NULL,
    pse_spe_codes = NULL,
    prs_nat_ref_codes = NULL,
    ben_table_name = NULL,
    output_table_name = NULL,
    r_output_path = NULL) {
  conn <- initialize_connection() # Connect to database

  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(end_date) + 1
  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")

  start_time <- Sys.time()

  first_non_archived_year <- get_first_non_archived_year(conn)

  for (year in start_year:end_year) {
    start_time <- Sys.time()
    print(glue("Processing year: {year}"))
    formatted_year_start <- glue("{year}-01-01")
    formatted_year_end <- glue("{year}-12-31")

    arc_suffix <- ifelse(year < first_non_archived_year, paste0("_", year), "")
    prs_table_name <- glue("ER_PRS_F{arc_suffix}")

    prs <- tbl(conn, prs_table_name)
    ben <- tbl(conn, ben_table_name)


    prs_clean <- prs %>%
      filter(
        EXE_SOI_DTD >= TO_DATE(formatted_start_date, "YYYY-MM-DD"),
        EXE_SOI_DTD <= TO_DATE(formatted_end_date, "YYYY-MM-DD"),
        FLX_DIS_DTD >= TO_DATE(formatted_year_start, "YYYY-MM-DD"),
        FLX_DIS_DTD <= TO_DATE(formatted_year_end, "YYYY-MM-DD"),
        FLX_DIS_DTD - EXE_SOI_DTD < 183 & # pour être à flux constant sur la durée pour les remontées de données
          (DPN_QLF != 71 | is.na(DPN_QLF)), # Suppression de l'activité des actes et consultations externes (ACE) rémontée pour information, cette activité est mesurée par ailleurs pour les établissements de santé dans le champ de la SAE
        (PRS_DPN_QLP != 71 | is.na(PRS_DPN_QLP)), # Suppression des ACE pour information
        (CPL_MAJ_TOP < 2), # Suppression des majorations
        (CPL_AFF_COD != 16), # Suppression des participations forfaitaires
        !(PSE_STJ_COD %in% c(61, 62, 63, 64, 69)) # Suppression des prestations de professionneles exécutants salariés (impact négligeable)
      ) %>%
      select(
        BEN_NIR_PSA, EXE_SOI_DTD, PSE_SPE_COD, PFS_EXE_NUM, PRS_NAT_REF, PRS_ACT_QTE
      )
    prs_clean <- prs_clean %>%
      filter(
        PRS_ACT_QTE > 0,
        PRS_NAT_REF %in% prs_nat_ref_codes,
        PSE_SPE_COD %in% pse_spe_codes
      )

    ben <- ben %>%
      select(BEN_IDT_ANO, BEN_NIR_PSA) %>%
      distinct()

    query <- ben %>%
      inner_join(prs_clean, by = "BEN_NIR_PSA") %>%
      select(BEN_IDT_ANO, EXE_SOI_DTD, PSE_SPE_COD, PFS_EXE_NUM, PRS_NAT_REF, PRS_ACT_QTE) %>%
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
