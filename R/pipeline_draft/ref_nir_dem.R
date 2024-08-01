library(ROracle)
library(dbplyr)
library(DBI)
library(glue)
source("utils.R")

create_ref_nir_dem <- function(aliases_table_name = NULL, demographics_table_name = NULL) {
  conn <- initialize_connection()

  ref_nir_cle <- tbl(conn, aliases_table_name)
  ref_ir_ben <- tbl(conn, "REF_IR_BEN")

  ref_nir_dem_0 <- ref_nir_cle %>%
    select("BEN_IDT_ANO") %>%
    distinct() %>%
    inner_join(
      ref_ir_ben,
      by = "BEN_IDT_ANO"
    ) %>%
    select(BEN_IDT_ANO, BEN_SEX_COD, BEN_NAI_ANN, BEN_NAI_MOI) %>%
    distinct()

  ref_nir_dem_0 <- ref_nir_dem_0 %>%
    mutate(BEN_NAI_MOI = case_when(
      BEN_NAI_MOI %in% c("01", "1") ~ "1",
      BEN_NAI_MOI %in% c("02", "2") ~ "2",
      BEN_NAI_MOI %in% c("03", "3") ~ "3",
      BEN_NAI_MOI %in% c("04", "4") ~ "4",
      BEN_NAI_MOI %in% c("05", "5") ~ "5",
      BEN_NAI_MOI %in% c("06", "6") ~ "6",
      BEN_NAI_MOI %in% c("07", "7") ~ "7",
      BEN_NAI_MOI %in% c("08", "8") ~ "8",
      BEN_NAI_MOI %in% c("09", "9") ~ "9",
      BEN_NAI_MOI == "10" ~ "10",
      BEN_NAI_MOI == "11" ~ "11",
      BEN_NAI_MOI == "12" ~ "12",
      TRUE ~ as.character(BEN_NAI_MOI) # Default case
    ))

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DEM_0",
    query = ref_nir_dem_0
  )

  ref_nir_dem_0 <- tbl(conn, "REF_NIR_DEM_0")

  table_n_idt <- ref_nir_dem_0 %>%
    distinct() %>%
    group_by(BEN_IDT_ANO) %>%
    summarize(n_idt = n())

  create_table_from_query(
    conn = conn,
    output_table_name = "TABLE_N_IDT",
    query = table_n_idt
  )

  table_n_idt <- tbl(conn, "TABLE_N_IDT")

  ref_nir_dem_1 <- ref_nir_dem_0 %>%
    left_join(table_n_idt, by = "BEN_IDT_ANO")

  # Create new variables based on conditions
  ref_nir_dem_1 <- ref_nir_dem_1 %>%
    mutate(
      doubl_excl = ifelse(n_idt == 1, 0, 1),
      BEN_SEX_COD_excl = ifelse(BEN_SEX_COD %in% c(1, 2), 0, 1),
      BEN_NAI_ANN_excl = ifelse(BEN_NAI_ANN >= 1900 & BEN_NAI_ANN <= !!format(Sys.time(), "%Y"), 0, 1),
      BEN_NAI_MOI_excl = ifelse(BEN_NAI_MOI %in% 1:12, 0, 1),
      dem_excl = ifelse(doubl_excl == 1 | BEN_SEX_COD_excl == 1 | BEN_NAI_ANN_excl == 1 | BEN_NAI_MOI_excl == 1, 1, 0)
    )

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_DEM_1",
    query = ref_nir_dem_1
  )

  ref_nir_dem_1 <- tbl(conn, "REF_NIR_DEM_1")
  ref_nir_dem_table <- collect(ref_nir_dem_1)

  # Frequency tables
  table(
    ref_nir_dem_table$dem_excl,
    ref_nir_dem_table$doubl_excl,
    ref_nir_dem_table$BEN_SEX_COD_excl,
    ref_nir_dem_table$BEN_NAI_ANN_excl,
    ref_nir_dem_table$BEN_NAI_MOI_excl
  )

  # Creating RES_DEM table
  flowchart <- bind_rows(
    data.frame(
      description = "Doublons contradictoires",
      N = n_distinct(filter(ref_nir_dem_table, doubl_excl == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Sexe",
      N = n_distinct(filter(ref_nir_dem_table, BEN_SEX_COD_excl == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Annee Naissance",
      N = n_distinct(filter(ref_nir_dem_table, BEN_NAI_ANN_excl == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Mois Naissance",
      N = n_distinct(filter(ref_nir_dem_table, BEN_NAI_MOI_excl == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Total exclusion",
      N = n_distinct(filter(ref_nir_dem_table, dem_excl == 1)$BEN_IDT_ANO)
    ),
    data.frame(
      description = "Patients avec donnees de sexe et naissance valides",
      N = n_distinct(filter(ref_nir_dem_table, dem_excl == 0)$BEN_IDT_ANO)
    )
  )
  # TO DO :
  # - Translate column names
  # - Integrate this exclusion policy into a global flow chart
  # (exclusion based on BEN_CDI_NIR, based on twins, ... etc.)

  # Create the final REF_NIR_DEM table with applied exclusions
  ref_nir_dem <- ref_nir_dem_1 %>%
    filter(dem_excl == 0) %>%
    select(BEN_IDT_ANO, BEN_SEX_COD, BEN_NAI_ANN, BEN_NAI_MOI) %>%
    distinct()

  create_table_from_query(
    conn = conn,
    output_table_name = demographics_table_name,
    query = ref_nir_dem
  )

  tables_to_remove <- c("REF_NIR_DEM_0", "TABLE_N_IDT", "REF_NIR_DEM_1")

  for (table_name in tables_to_remove) {
    dbRemoveTable(conn, table_name)
  }

  dbDisconnect(conn)

  return(flowchart)
}
