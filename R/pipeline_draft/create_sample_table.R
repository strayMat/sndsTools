library(ROracle)
library(dbplyr)
library(DBI)
library(glue)
source("utils.R")


create_sample <- function() {
  conn <- initialize_connection()
  ref_ir_ben <- tbl(conn, "REF_IR_BEN")
  ref_ir_ben_sample <- ref_ir_ben %>%
    select(BEN_NIR_PSA) %>%
    distinct() %>%
    head(1000)


  create_table_from_query(
    conn = conn,
    output_table_name = "TEST_SAMPLE_PSA",
    query = ref_ir_ben_sample
  )
  dbDisconnect(conn)
}

start_time <- Sys.time()

create_sample()

end_time <- Sys.time()
print(glue("Time taken : {round(difftime(end_time, start_time, units='mins'),1)} mins."))
