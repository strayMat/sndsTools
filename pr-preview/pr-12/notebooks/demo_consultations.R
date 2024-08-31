start_date <- as.Date("01/01/2009", format = "%d/%m/%Y")
end_date <- as.Date("31/12/2022", format = "%d/%m/%Y")

pse_spe_codes <- c("01", "22", "32", "34")

# Reference for consultation codes: https://documentation-snds.health-data-hub.fr/snds/fiches/activite_medecins.html#contexte
consultation_codes <- c(1089, 1090, 1091, 1092, 1093, 1094, 1098, 1099, 1101, 1102, 1103, 1104, 1105, 1107, 1109, 1110, 1111, 1112, 1113, 1114, 1115, 1117, 1118, 1122, 1123, 1140, 1168, 1434, 1929, 2414, 2426, 4316, 9421)
visits_codes <- c(1209, 1210, 1211, 1212, 1213, 1214, 1215, 1216, 1221, 1222)
teleconsultation_codes <- c(1096, 1157, 1164, 1191, 1192)

prs_nat_ref_codes <- c(consultation_codes, visits_codes, teleconsultation_codes)


extract_consultations(
    start_date = start_date,
    end_date = end_date,
    pse_spe_codes = pse_spe_codes,
    prs_nat_ref_codes = prs_nat_ref_codes,
    ben_table_name = "PD_10_22_IDS",
    output_table_name = "PD_CONS_09_22",
    r_output_path = "~/sasdata1/park/visualize/data/raw"
)
