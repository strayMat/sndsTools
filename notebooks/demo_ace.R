start_date <- as.Date("01/01/2019", format = "%d/%m/%Y")
end_date <- as.Date("31/12/2022", format = "%d/%m/%Y")
spe_codes <- c("01", "22", "32", "34")

extract_hospital_consultations(
    start_date = start_date,
    end_date = end_date,
    spe_codes = spe_codes,
    ben_table_name = "PD_10_22_IDS",
    output_table_name = "PD_ACE_19_22",
    r_output_path = "~/sasdata1/park/visualize/data/raw"
)
