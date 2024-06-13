# library(ROracle)
# library(dplyr)
# library(dbplyr)
# library(DBI)
# library(glue)
# library(lubridate)
# source("~/sasdata1/park/retrieve/utils.R")



extract_dispenses <- function(
    start_date = NULL,
    end_date = NULL,
    starts_with_codes = NULL,
    is_exactly_codes = NULL,
    ben_table_name = NULL,
    output_table_name = NULL,
    r_output_path = TRUE) {
  conn <- initialize_connection() # Connect to database

  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(end_date) + 1

  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")

  first_non_archived_year <- get_first_non_archived_year(conn)

  for (year in start_year:end_year) {
    print(glue("Processing year: {year}"))
    start_time <- Sys.time()
    arc_suffix <- ifelse(year < first_non_archived_year, paste0("_", year), "")
    prs_table_name <- glue("ER_PRS_F{arc_suffix}")
    pha_table_name <- glue("ER_PHA_F{arc_suffix}")

    prs_tbl <- tbl(conn, prs_table_name)
    pha_tbl <- tbl(conn, pha_table_name)
    atc_tbl <- tbl(conn, "IR_PHA_R")

    # Construct conditions for "starts with" and "is exactly"
    if (is.null(starts_with_codes) & is.null(is_exactly_codes)) {
      stop("'starts_with_codes' and 'is_exactly_codes' cannot be NULL at the same time")
    }
    starts_with_conditions <- sapply(starts_with_codes, function(code) paste0("PHA_ATC_CLA LIKE '", code, "%'"))
    is_exactly_conditions <- sapply(is_exactly_codes, function(code) paste0("PHA_ATC_CLA = '", code, "'"))
    combined_conditions <- paste(c(starts_with_conditions, is_exactly_conditions), collapse = " OR ")
    combined_conditions <- glue("({combined_conditions})")

    # Construct date conditions
    dis_dtd_condition <- glue("FLX_DIS_DTD >= TO_DATE('{year}-01-01', 'YYYY-MM-DD') AND FLX_DIS_DTD <= TO_DATE('{year}-12-31', 'YYYY-MM-DD')")
    soi_dtd_condition <- glue("EXE_SOI_DTD >= TO_DATE('{formatted_start_date}', 'YYYY-MM-DD') AND EXE_SOI_DTD <= TO_DATE('{formatted_end_date}', 'YYYY-MM-DD')")

    # Prepare the dbplyr query
    query <- prs_tbl %>%
      inner_join(pha_tbl, by = c(
        "DCT_ORD_NUM", "FLX_DIS_DTD", "FLX_EMT_ORD", "FLX_EMT_NUM",
        "FLX_EMT_TYP", "FLX_TRT_DTD", "ORG_CLE_NUM", "PRS_ORD_NUM", "REM_TYP_AFF"
      )) %>%
      inner_join(atc_tbl, by = c("PHA_PRS_C13" = "PHA_CIP_C13")) %>%
      filter(
        sql(dis_dtd_condition),
        sql(soi_dtd_condition),
        sql(combined_conditions)
      ) %>%
      mutate(PHA_CIP_C13 = PHA_PRS_C13) %>%
      select(BEN_NIR_PSA, EXE_SOI_DTD, PHA_ACT_QSN, PHA_ATC_CLA, PHA_CIP_C13, PSP_SPE_COD) %>%
      distinct()

    if (!is.null(ben_table_name)) {
      ben <- tbl(conn, ben_table_name)
      ben <- ben %>%
        select(BEN_IDT_ANO, BEN_NIR_PSA) %>%
        distinct()
      query <- query %>%
        inner_join(ben, by = "BEN_NIR_PSA") %>%
        select(BEN_IDT_ANO, EXE_SOI_DTD, PHA_ACT_QSN, PHA_ATC_CLA, PHA_CIP_C13, PSP_SPE_COD) %>%
        distinct()
    }

    # Execute the query to create or insert into the output table
    create_table_or_insert_from_query(conn = conn, output_table_name = output_table_name, query = query)

    end_time <- Sys.time()
    print(glue("Time taken for year {year}: {round(difftime(end_time, start_time, units='mins'),1)} mins."))
  }

  query <- tbl(conn, output_table_name)
  dispenses <- collect(query)
  if (!is.null(r_output_path)) {
    saveRDS(dispenses, file = glue("{r_output_path}/{tolower(output_table_name)}.RDS"))
  }

  dbDisconnect(conn)
  return(dispenses)
}
