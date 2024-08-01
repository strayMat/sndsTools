library(ROracle)
library(dbplyr)
library(DBI) # For database operations
library(glue)
# setwd("/sasdata/prd/users/44a001280010899/snds_extraction_pipeline/referentiel")
source("utils.R")

create_ref_ir_ben <- function(output_table_name = "REF_IR_BEN") {
  conn <- initialize_connection()
  # Read the tables
  ir_ben_r <- tbl(conn, "IR_BEN_R")
  ir_ben_r_arc <- tbl(conn, "IR_BEN_R_ARC")

  # Define the columns for selection
  cols <- c(
    "BEN_IDT_ANO", "BEN_NIR_PSA", "BEN_RNG_GEM", "BEN_NIR_ANO", "ASS_NIR_ANO",
    "BEN_SEX_COD", "BEN_NAI_ANN", "BEN_NAI_MOI", "BEN_CDI_NIR", "BEN_DCD_DTE"
  )

  ir_ben_r <- ir_ben_r %>%
    select(all_of(cols)) %>%
    distinct() %>%
    mutate(SOURCE = 1)

  ir_ben_r_arc <- ir_ben_r_arc %>%
    select(all_of(cols)) %>%
    distinct() %>%
    mutate(SOURCE = 2) %>%
    anti_join(ir_ben_r, by = "BEN_NIR_PSA")

  # Combine the two queries with a union
  ref_ir_ben <- union(ir_ben_r, ir_ben_r_arc)

  create_table_from_query(
    conn = conn,
    output_table_name = output_table_name,
    query = ref_ir_ben
  )
  # A corriger : creer une table intermediaire avant le filtre sur ben_cdi_nir

  # Close the connection
  dbDisconnect(conn)
}
