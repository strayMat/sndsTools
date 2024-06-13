start_date <- as.Date("01/01/2009", format = "%d/%m/%Y")
end_date <- as.Date("31/12/2022", format = "%d/%m/%Y")

icd_cod_starts_with <- NULL
ald_numbers <- as.character(1:30)
excl_atm_nat <- c("11", "12", "13")

ben_table_name <- "PD_10_22_IDS"
output_table_name <- "ALD_TEST"
save_to_sas <- FALSE
r_output_path <- "~/sasdata1/park/visualize/data/raw"

extract_ald(
    start_date = start_date,
    end_date = end_date,
    icd_cod_starts_with = icd_cod_starts_with,
    ald_numbers = ald_numbers,
    excl_atm_nat = excl_atm_nat,
    ben_table_name = ben_table_name,
    output_table_name = output_table_name,
    save_to_sas = save_to_sas,
    r_output_path = r_output_path
)
