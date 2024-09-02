library(ROracle)
library(dplyr)
library(dbplyr)
library(DBI)
library(glue)
library(lubridate)
library(progress)
# Source the following functions to run the code below:
# - initialize_connection
# - get_first_non_archived_year
# - create_table_or_insert_from_query
# - extract_drug_dispenses

# Retrieve all N04A drug dispenses over three days
# The output will contain ben_nir_psa as the patient
# identifier, since no patients_ids are provided
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-01-03")
starts_with_codes <- c("N04A")

dispenses <- extract_drug_dispenses(
  start_date = start_date,
  end_date = end_date,
  starts_with_codes = starts_with_codes
)
head(dispenses)

# Same as above, but for N04A and N04B drug dispenses
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-01-03")
starts_with_codes <- c(
  "N04A",
  "N04B"
)

dispenses <- extract_drug_dispenses(
  start_date = start_date,
  end_date = end_date,
  starts_with_codes = starts_with_codes
)
head(dispenses)

# You can also provide your own database connection
# to the extract_drug_dispenses function. This is useful
# to avoid opening and closing a connection for each
# extraction function call.
conn <- initialize_connection()
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-01-03")
starts_with_codes <- c("N04A")

dispenses <- extract_drug_dispenses(
  start_date = start_date,
  end_date = end_date,
  starts_with_codes = starts_with_codes,
  conn = conn
)
head(dispenses)

# Create a sample of patients
conn <- initialize_connection()
ref_ir_ben <- tbl(conn, "IR_BEN_R")
patients_ids_sample <- ref_ir_ben %>%
  select(BEN_IDT_ANO, BEN_NIR_PSA) %>%
  distinct() %>%
  head(10000) %>%
  collect()

# Retrieve antidiabetic drug dispenses for
# the given sample of patients over one month.
# The output will contain ben_idt_ano as the patient
# identifier, since patients_ids are provided
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-01-31")
starts_with_codes <- c("A10")

dispenses <- extract_drug_dispenses(
  start_date = start_date,
  end_date = end_date,
  starts_with_codes = starts_with_codes,
  patients_ids = patients_ids_sample
)
head(dispenses)

# Retrieve all dispenses for the sample
# of patients over one week. The fact that
# no starts_with_codes are provided allows
# all dispenses to be retrieved.
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-01-07")

dispenses <- extract_drug_dispenses(
  start_date = start_date,
  end_date = end_date,
  patients_ids = patients_ids_sample
)
head(dispenses)

# If the output_table_name argument is provided,
# the output will be stored in a table with the
# given name. This is especially useful when the
# output table is too large to be stored in memory.
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-01-15")
output_table_name <- "TMP_DISPENSES"
print(dbExistsTable(conn, output_table_name))

extract_drug_dispenses(
  start_date = start_date,
  end_date = end_date,
  patients_ids = patients_ids_sample,
  output_table_name = output_table_name
)

print(dbExistsTable(conn, output_table_name))
query <- glue("SELECT COUNT(*) FROM {output_table_name}")
result <- dbGetQuery(conn, query)
print(result)