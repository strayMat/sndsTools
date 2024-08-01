library(ROracle)
library(dbplyr)
library(DBI)
library(glue)
source("utils.R")


create_ref_nir_dc <- function(
    aliases_table_name = NULL,
    demographics_table_name = NULL) {
  conn <- initialize_connection()
  ref_ir_ben <- tbl(conn, "REF_IR_BEN")

  ref_nir_dc_1_11 <- ref_ir_ben %>%
    filter(BEN_DCD_DTE != TO_DATE("1600-01-01", "yyyy-MM-dd"), !is.na(BEN_IDT_ANO)) %>%
    select(BEN_IDT_ANO, BEN_DCD_DTE) %>%
    distinct()

  ref_nir_dc_1_11 <- ref_nir_dc_1_11 %>%
    group_by(BEN_IDT_ANO) %>%
    summarize(BEN_DCD_DTE = min(BEN_DCD_DTE)) %>%
    ungroup()

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DC_1_11",
    query = ref_nir_dc_1_11
  )

  ki_cci_r <- tbl(conn, "KI_CCI_R")

  ref_nir_dc_1_12 <- ki_cci_r %>%
    filter(!is.na(BEN_DCD_DTE), !is.na(BEN_IDT_ANO)) %>%
    select(BEN_IDT_ANO, BEN_DCD_DTE, DCD_CIM_COD) %>%
    distinct()

  ref_nir_dc_1_12 <- ref_nir_dc_1_12 %>%
    group_by(BEN_IDT_ANO) %>%
    mutate(
      BEN_DCD_DTE = min(BEN_DCD_DTE),
      DCD_CIM_COD = DCD_CIM_COD[BEN_DCD_DTE == min(BEN_DCD_DTE)]
    ) %>%
    distinct() %>%
    ungroup()

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DC_1_12",
    query = ref_nir_dc_1_12
  )

  ref_nir_dem <- tbl(conn, demographics_table_name)
  number_demographics <- ref_nir_dem %>%
    select(BEN_IDT_ANO) %>%
    distinct() %>%
    summarise(N = n()) %>%
    collect()
  print(glue("Number of patients in demographics table before adding death: {number_demographics$N}"))
  ref_nir_dc_1_11 <- tbl(conn, "REF_NIR_DC_1_11")
  ref_nir_dc_1_12 <- tbl(conn, "REF_NIR_DC_1_12")

  ref_nir_dc_1_3 <- ref_nir_dem %>%
    select(BEN_IDT_ANO) %>%
    distinct() %>%
    left_join(
      ref_nir_dc_1_11 %>% select(BEN_IDT_ANO, BEN_DCD_DTE_ir = BEN_DCD_DTE),
      by = "BEN_IDT_ANO"
    ) %>%
    left_join(
      ref_nir_dc_1_12 %>% select(BEN_IDT_ANO, BEN_DCD_DTE_cepi = BEN_DCD_DTE, DCD_CIM_COD),
      by = "BEN_IDT_ANO"
    ) %>%
    mutate(
      BEN_DCD_DTE = if_else(!is.na(BEN_DCD_DTE_cepi), BEN_DCD_DTE_cepi, BEN_DCD_DTE_ir)
    ) %>%
    select(BEN_IDT_ANO, BEN_DCD_DTE, DCD_CIM_COD) %>%
    distinct()

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DC_1_3",
    query = ref_nir_dc_1_3
  )

  # We do not filter with filter(!is.na(BEN_DCD_DTE))
  # And do not create ref_nir_dc_1_4

  ref_nir_dem <- tbl(conn, demographics_table_name)

  ref_nir_dc_2_0 <- ref_nir_dem %>%
    left_join(ref_nir_dc_1_3, by = "BEN_IDT_ANO")

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DC_2_0",
    query = ref_nir_dc_2_0
  )
  ref_nir_dc_2_0 <- tbl(conn, "REF_NIR_DC_2_0")

  current_year <- as.integer(format(Sys.time(), "%Y"))
  current_month <- as.integer(format(Sys.time(), "%m"))

  ref_nir_dc_2_1 <- ref_nir_dc_2_0 %>%
    mutate(
      ANO_1 = if_else(
        !is.na(BEN_DCD_DTE) & year(BEN_DCD_DTE) < BEN_NAI_ANN,
        1,
        0
      ),
      ANO_2 = if_else(
        !is.na(BEN_DCD_DTE) & (year(BEN_DCD_DTE) == BEN_NAI_ANN & month(BEN_DCD_DTE) < BEN_NAI_MOI),
        1,
        0
      ),
      ANO_3 = if_else(
        !is.na(BEN_DCD_DTE) & (year(BEN_DCD_DTE) > !!current_year),
        1,
        0
      ),
      ANO_4 = if_else(
        !is.na(BEN_DCD_DTE) & (year(BEN_DCD_DTE) == !!current_year & month(BEN_DCD_DTE) > !!current_month), 1,
        0
      ),
      ANO_DC = if_else(ANO_1 == 1 | ANO_2 == 1 | ANO_3 == 1 | ANO_4 == 1, 1, 0)
    )

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DC_2_1",
    query = ref_nir_dc_2_1
  )

  ref_nir_dc_2_1_table <- collect(ref_nir_dc_2_1)

  # Creating RES_DEM table
  flowchart <- bind_rows(
    data.frame(
      description = "Anomalies de date de décès type 1",
      N = n_distinct(filter(ref_nir_dc_2_1_table, ANO_1 == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Anomalies de date de décès type 2",
      N = n_distinct(filter(ref_nir_dc_2_1_table, ANO_2 == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Anomalies de date de décès type 3",
      N = n_distinct(filter(ref_nir_dc_2_1_table, ANO_3 == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Anomalies de date de décès type 4",
      N = n_distinct(filter(ref_nir_dc_2_1_table, ANO_4 == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Anomalies de date de décès totales",
      N = n_distinct(filter(ref_nir_dc_2_1_table, ANO_DC == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Patients sans aucune anomalie de date de décès",
      N = n_distinct(filter(ref_nir_dc_2_1_table, ANO_DC == 0)$BEN_IDT_ANO)
    )
  )

  # Filter demographics table based on ANO_DC
  ref_nir_dem <- ref_nir_dc_2_1 %>%
    filter(ANO_DC == 0) %>%
    select(-c(ANO_1, ANO_2, ANO_3, ANO_4, ANO_DC)) %>%
    distinct()

  create_table_from_query(
    conn = conn,
    output_table_name = demographics_table_name,
    query = ref_nir_dem
  )

  # Update aliases table based on the filter on ANO_DC
  ref_nir_dem <- tbl(conn, demographics_table_name)
  ref_nir_cle <- tbl(conn, aliases_table_name)

  ref_nir_dc_aliases <- ref_nir_cle %>%
    inner_join(
      ref_nir_dem %>%
        select(BEN_IDT_ANO) %>%
        distinct(),
      by = "BEN_IDT_ANO"
    ) %>%
    distinct()

  # create_table_from_query cannot be used directly here
  # because the table already exists
  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DC_ALIASES",
    query = ref_nir_dc_aliases
  )

  ref_nir_dc_aliases <- tbl(conn, "REF_NIR_DC_ALIASES")
  create_table_from_query(
    conn = conn,
    output_table_name = aliases_table_name,
    query = ref_nir_dc_aliases
  )

  tables_to_remove <- c(
    "REF_NIR_DC_1_11",
    "REF_NIR_DC_1_12",
    "REF_NIR_DC_1_3",
    "REF_NIR_DC_2_0",
    "REF_NIR_DC_2_1",
    "REF_NIR_DC_ALIASES"
  )

  for (table_name in tables_to_remove) {
    dbRemoveTable(conn, table_name)
  }

  dbDisconnect(conn)

  return(flowchart)
}
