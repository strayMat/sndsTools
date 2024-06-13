start_date <- as.Date("01/01/2009", format = "%d/%m/%Y")
end_date <- as.Date("31/12/2022", format = "%d/%m/%Y")

starts_with_codes <- c("N04A", "N04B")
is_exactly_codes <- NULL

extract_dispenses(
    start_date = start_date,
    end_date = end_date,
    starts_with_codes = starts_with_codes,
    is_exactly_codes = is_exactly_codes,
    ben_table_name = "PD_10_22_IDS",
    output_table_name = "PD_ATC_09_22",
    r_output_path = "~/sasdata1/park/visualize/data/raw"
)
