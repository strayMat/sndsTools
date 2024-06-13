# library(ROracle)
# library(dplyr)
# library(dbplyr)
# library(DBI)
# library(glue)
# library(lubridate)


extract_ald <- function(start_date = NULL,
                        end_date = NULL,
                        icd_cod_starts_with = NULL,
                        ald_numbers = NULL,
                        excl_atm_nat = NULL,
                        ben_table_name = NULL,
                        output_table_name = NULL,
                        save_to_sas = NULL,
                        r_output_path = NULL) {
  conn <- initialize_connection()
  formatted_start_date <- format(start_date, "%d/%m/%Y")
  formatted_end_date <- format(end_date, "%d/%m/%Y")

  conditions <- list()


  if (!is.null(icd_cod_starts_with) && length(icd_cod_starts_with) > 0) {
    starts_with_conditions <- sapply(icd_cod_starts_with, function(code) glue("MED_MTF_COD LIKE '{code}%'"))
    conditions <- c(conditions, paste(starts_with_conditions, collapse = " OR "))
  }

  if (!is.null(ald_numbers) && length(ald_numbers) > 0) {
    conditions <- c(conditions, glue("IMB_ALD_NUM IN ({paste(ald_numbers, collapse = ',')})"))
  }

  combined_conditions <- paste(conditions, collapse = " OR ")

  imb_r <- tbl(conn, "IR_IMB_R")

  query <- imb_r %>%
    filter(
      IMB_ALD_DTD <= TO_DATE(formatted_end_date, "DD/MM/YYYY"),
      IMB_ALD_DTF >= TO_DATE(formatted_start_date, "DD/MM/YYYY"),
      !(IMB_ETM_NAT %in% excl_atm_nat)
    ) %>%
    filter(
      sql(combined_conditions)
    ) %>%
    select(
      BEN_NIR_PSA,
      IMB_ALD_NUM,
      IMB_ALD_DTD,
      IMB_ALD_DTF,
      IMB_ETM_NAT,
      MED_MTF_COD
    ) %>%
    distinct()


  if (!is.null(ben_table_name)) {
    ben <- tbl(conn, ben_table_name)
    ben <- ben %>%
      select(BEN_IDT_ANO, BEN_NIR_PSA) %>%
      distinct()
    query <- query %>%
      inner_join(ben, by = "BEN_NIR_PSA") %>%
      select(
        BEN_IDT_ANO,
        IMB_ALD_NUM,
        IMB_ALD_DTD,
        IMB_ALD_DTF,
        IMB_ETM_NAT,
        MED_MTF_COD
      ) %>%
      distinct()
  }

  if (save_to_sas) {
    create_table_or_insert_from_query(
      conn = conn,
      output_table_name = output_table_name,
      query = query
    )
  }

  ald <- collect(query)

  if (!is.null(r_output_path)) {
    saveRDS(ald, file = glue("{r_output_path}/{tolower(output_table_name)}.RDS"))
  }

  dbDisconnect(conn)

  return(ald)
}
