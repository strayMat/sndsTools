library(ROracle)
library(dplyr)
library(dbplyr)
library(DBI)
library(glue)
library(lubridate)
# Source the following functions to run the code below:
# - initialize_connection
# - extract_long_term_disease

# Retrieve all active long-term diseases in 2010
# with ICD 10 code corresponding to Parkinson's
# disease (G20)
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-12-31")
icd_cod_starts_with <- c("G20")

ald <- extract_long_term_disease(
    start_date = start_date,
    end_date = end_date,
    icd_cod_starts_with = icd_cod_starts_with
)
head(ald)

# Retrieve all active long-term diseases in 2010
# with ALD number corresponding to diabetes (8).
# Gives roughly 6.5 million rows.
start_date <- as.Date("2010-01-01")
end_date <- as.Date("2010-12-31")
ald_numbers <- c(8)

ald <- extract_long_term_disease(
    start_date = start_date,
    end_date = end_date,
    ald_numbers = ald_numbers
)
head(ald)
