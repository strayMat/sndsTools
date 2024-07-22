#' Extraction des Affections Longue Durée (ALD)
#'
#' Cette fonction permet d'extraire des ALD actives au
#' moins un jour sur une période donnée.
#' Les ALD dont l'intersection [IMB_ALD_DTD, IMB_ALD_DTF]
#' avec la période [start_date, end_date] n'est pas vide
#' sont extraites.
#' Si des codes ICD 10 ou des numéros d'ALD sont fournis,
#' seules les ALD associées à ces codes ICD 10 ou numéros
#' d'ALD sont extraites.
#' Si des identifiants de patients sont fournis, seules
#' les ALD associées à ces patients sont extraites.
#'
#' @param start_date Date La date de début de la période
#' sur laquelle extraire les ALD actives.
#' @param end_date Date La date de fin de la période
#' sur laquelle extraire les ALD actives.
#' @param icd_cod_starts_with character vector Un vecteur de codes
#' ICD 10. Si `icd_cod_starts_with` ou `ald_numbers` sont fournis,
#' seules les ALD associées à ces codes ICD 10 ou numéros d'ALD
#' sont extraites. Sinon, toutes les ALD actives sur la période
#' [start_date, end_date] sont extraites.
#' @param ald_numbers numeric vector Un vecteur de numéros d'ALD.
#' Si `icd_cod_starts_with` ou `ald_numbers` sont fournis,
#' seules les ALD associées à ces codes ICD 10 ou numéros d'ALD
#' sont extraites. Sinon, toutes les ALD actives sur la période
#' [start_date, end_date] sont extraites.
#' @param excl_atm_nat character vector Un vecteur de codes
#' IMB_ATM_NAT à exclure. Par défaut, les ALD de nature
#' 11, 12 et 13 sont exclues.
#' @param patients_ids data.frame Un data.frame contenant les
#' paires d'identifiants des patients pour lesquels les ALD
#' doivent être extraites. Les colonnes de ce data.frame
#' doivent être "ben_idt_ano" et "ben_nir_psa" (en minuscules). Les
#' "ben_nir_psa" doivent être tous les "ben_nir_psa" associés aux
#' "ben_idt_ano" fournis. Si `patients_ids` n'est pas fourni,
#' les ALD de tous les patients sont extraites.
#' @return Un data.frame contenant les ALDs actives sur la période.
#' La première colonne est BEN_NIR_PSA si aucun identifiant de
#' patient n'est fourni. Si  la première colonne est BEN_IDT_ANO.
#' Les autres colonnes par défaut sont les suivantes :
#' - IMB_ALD_NUM : Le numéro de l'ALD
#' - IMB_ALD_DTD : La date de début de l'ALD
#' - IMB_ALD_DTF : La date de fin de l'ALD
#' - IMB_ETM_NAT : La nature de l'ALD
#' - MED_MTF_COD : Le code ICD 10 de la pathologie associée à l'ALD
#'
#' @examples
#' start_date <- as.Date("2010-01-01")
#' end_date <- as.Date("2010-01-03")
#' starts_with_codes <- c("N04A")
#'
#' dispenses <- extract_drug_dispenses(
#'   start_date = start_date,
#'   end_date = end_date,
#'   starts_with_codes = starts_with_codes
#' )
#' @export
extract_long_term_disease <- function(
    start_date = NULL,
    end_date = NULL,
    icd_cod_starts_with = NULL,
    ald_numbers = NULL,
    excl_atm_nat = c("11", "12", "13"),
    patients_ids = NULL) {
  conn <- initialize_connection()
  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")

  if (!is.null(icd_cod_starts_with)) {
    print(glue("Extracting LTD status for ICD 10 codes starting \
    with {paste(icd_cod_starts_with, collapse = ' or ')}..."))
  } 
  if (!is.null(ald_numbers)){
    print(glue("Extracting LTD status for ALD numbers \
    {paste(ald_numbers, collapse = ',')}..."))
  }
  if (is.null(icd_cod_starts_with) & is.null(ald_numbers)) {
    print(glue("Extracting LTD status for all ICD 10 codes..."))
  }

  if (!is.null(patients_ids)) {
    patients_ids <- patients_ids %>%
      rename(
        BEN_IDT_ANO = ben_idt_ano,
        BEN_NIR_PSA = ben_nir_psa
      )
    patients_ids_table_name <- "TMP_PATIENTS_IDS"
    try(
      DBI::dbRemoveTable(conn, patients_ids_table_name),
      silent = TRUE
    )
    DBI::dbWriteTable(conn, patients_ids_table_name, patients_ids)
  }

  codes_conditions <- list()
  if (!is.null(icd_cod_starts_with)) {
    starts_with_conditions <- sapply(
      icd_cod_starts_with,
      function(code) glue("MED_MTF_COD LIKE '{code}%'")
    )
    codes_conditions <- c(
      codes_conditions,
      paste(starts_with_conditions, collapse = " OR ")
    )
  }
  if (!is.null(ald_numbers)) {
    codes_conditions <- c(
      codes_conditions,
      glue("IMB_ALD_NUM IN ({paste(ald_numbers, collapse = ',')})")
    )
  }

  codes_conditions <- paste(codes_conditions, collapse = " OR ")

  imb_r <- dplyr::tbl(conn, "IR_IMB_R")

  date_condition <- glue(
    "IMB_ALD_DTD <= TO_DATE('{formatted_end_date}', 'YYYY-MM-DD') \
    AND IMB_ALD_DTF >= TO_DATE('{formatted_start_date}', 'YYYY-MM-DD')"
  )

  query <- imb_r %>%
    filter(
      sql(date_condition),
      !(IMB_ETM_NAT %in% excl_atm_nat)
    )

  if (!is.null(icd_cod_starts_with) | !is.null(ald_numbers)) {
    query <- query %>%
      filter(
        sql(codes_conditions)
      )
  }

  cols_to_select <- c(
    "IMB_ALD_NUM",
    "IMB_ALD_DTD",
    "IMB_ALD_DTF",
    "IMB_ETM_NAT",
    "MED_MTF_COD"
  )

  query <- query %>%
    select(
      BEN_NIR_PSA,
      all_of(cols_to_select)
    ) %>%
    distinct()

  if (!is.null(patients_ids)) {
    patients_ids_table <- dplyr::tbl(conn, patients_ids_table_name)
    patients_ids_table <- patients_ids_table %>%
      select(BEN_IDT_ANO, BEN_NIR_PSA) %>%
      distinct()
    query <- query %>%
      inner_join(patients_ids_table, by = "BEN_NIR_PSA") %>%
      select(
        BEN_IDT_ANO,
        all_of(cols_to_select)
      ) %>%
      distinct()
  }

  ald <- collect(query)
  DBI::dbDisconnect(conn)
  return(ald)
}
