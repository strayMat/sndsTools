#' Extraction des délivrances de médicaments.
#'
#' Cette fonction permet d'extraire les délivrances de médicaments.
#' Les délivrances dont les dates EXE_SOI_DTD sont comprises
#' entre start_date et end_date (incluses) sont extraites.
#' Le décalage de remontée des données est pris en compte
#' en récupérant également les délivrances dont les dates
#' FLX_DIS_DTD sont comprises dans l'année calendaire qui
#' suit l'année calendaire de end_date.
#' Si starts_with_codes est fourni, seules les délivrances
#' de médicaments dont le code ATC commence par l'un des
#' éléments de starts_with_codes sont extraites.
#' Si patients_ids est fourni, seules les délivrances
#' de médicaments pour les patients dont les identifiants
#' sont dans patients_ids sont extraites.
#'
#' @param start_date Date La date de début de la période
#' des délivrances des médicaments à extraire.
#' @param end_date Date La date de fin de la période
#' des délivrances des médicaments à extraire.
#' @param starts_with_codes Character vector Les codes ATC
#' par lesquels les délivrances de médicaments à extraire
#' doivent commencer.
#' @param patients_ids data.frame Un data.frame contenant les
#' paires d'identifiants des patients pour lesquels les délivrances de
#' médicaments doivent être extraites. Les colonnes de ce data.frame
#' doivent être "ben_idt_ano" et "ben_nir_psa" (en minuscules). Les
#' "ben_nir_psa" doivent être tous les "ben_nir_psa" associés aux
#' "ben_idt_ano" fournis.
#' @return Un data.frame contenant les délivrances de médicaments.
#' La première colonne est BEN_NIR_PSA si aucun identifiant de
#' patient n'est fourni. Si  la première colonne est BEN_IDT_ANO.
#' Les autres colonnes par défaut sont les suivantes :
#' - EXE_SOI_DTD : Date de la délivrance
#' - PHA_ACT_QSN : Quantité délivrée
#' - PHA_ATC_CLA : Code ATC du médicament délivré
#' - PHA_CIP_C13 : Code CIP du médicament délivré
#' - PSP_SPE_COD : Code de spécialité du professionnel de soin prescripteur
#'
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
extract_drug_dispenses <- function(
    start_date = NULL,
    end_date = NULL,
    starts_with_codes = NULL,
    patients_ids = NULL) {
  conn <- initialize_connection()
  temp_table_name <-
    paste0("TMP_DISP_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  try(
    DBI::dbRemoveTable(conn, database_output_table_name),
    silent = TRUE
  )
  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(end_date) + 1

  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")

  first_non_archived_year <- get_first_non_archived_year(conn)

  if (!is.null(starts_with_codes)) {
    print(
      glue(
        "Extracting drug dispenses with ATC codes starting with \
        {paste(starts_with_codes, collapse = ' or ')}..."
      )
    )
  } else {
    print(
      glue(
        "Extracting drug dispenses for all ATC codes..."
      )
    )
  }

  if (!is.null(patients_ids)) {
    patients_ids <- patients_ids %>%
      rename(
        BEN_IDT_ANO = ben_idt_ano,
        BEN_NIR_PSA = ben_nir_psa
      )
    patients_ids_table_name <- "TMP_PATIENTS_IDS"
    try(dbRemoveTable(conn, patients_ids_table_name), silent = T)
    dbWriteTable(conn, patients_ids_table_name, patients_ids)
  }
  pb <- progress_bar$new(
    format = "Extracting :year1 (going from :year2 to :year3) \
    [:bar] :percent in :elapsed (eta: :eta)",
    total = (end_year - start_year + 1), clear = FALSE, width = 80
  )
  pb$tick(0)
  for (year in start_year:end_year) {
    pb$tick(tokens = list(year1 = year, year2 = start_year, year3 = end_year))

    arc_suffix <- ifelse(year < first_non_archived_year, paste0("_", year), "")
    er_prs_f <- dplyr::tbl(conn, glue("ER_PRS_F{arc_suffix}"))
    er_pha_f <- dplyr::tbl(conn, glue("ER_PHA_F{arc_suffix}"))
    ir_pha_r <- dplyr::tbl(conn, "IR_PHA_R")

    starts_with_conditions <- sapply(
      starts_with_codes,
      function(code) glue("PHA_ATC_CLA LIKE '{code}%'")
    )
    atc_conditions <- paste(starts_with_conditions, collapse = " OR ")
    atc_conditions <- glue("({atc_conditions})")


    dis_dtd_condition <- glue(
      "FLX_DIS_DTD >= TO_DATE('{year}-01-01', 'YYYY-MM-DD') \
      AND FLX_DIS_DTD <= TO_DATE('{year}-12-31', 'YYYY-MM-DD')"
    )
    soi_dtd_condition <- glue(
      "EXE_SOI_DTD >= TO_DATE('{formatted_start_date}', 'YYYY-MM-DD') \
      AND EXE_SOI_DTD <= TO_DATE('{formatted_end_date}', 'YYYY-MM-DD')"
    )

    dcir_join_keys <- c(
      "DCT_ORD_NUM",
      "FLX_DIS_DTD",
      "FLX_EMT_ORD",
      "FLX_EMT_NUM",
      "FLX_EMT_TYP",
      "FLX_TRT_DTD",
      "ORG_CLE_NUM",
      "PRS_ORD_NUM",
      "REM_TYP_AFF"
    )

    query <- er_prs_f %>%
      inner_join(er_pha_f, by = dcir_join_keys) %>%
      inner_join(ir_pha_r, by = c("PHA_PRS_C13" = "PHA_CIP_C13")) %>%
      filter(
        sql(dis_dtd_condition),
        sql(soi_dtd_condition)
      )

    if (!is.null(starts_with_codes)) {
      query <- query %>%
        filter(sql(atc_conditions))
    }

    cols_to_select <- c(
      "EXE_SOI_DTD",
      "PHA_ACT_QSN",
      "PHA_ATC_CLA",
      "PHA_CIP_C13",
      "PSP_SPE_COD"
    )

    query <- query %>%
      mutate(PHA_CIP_C13 = PHA_PRS_C13) %>%
      select(
        BEN_NIR_PSA,
        all_of(cols_to_select)
      ) %>%
      distinct()

    if (!is.null(patients_ids)) {
      patients_ids_table <- tbl(conn, patients_ids_table_name)
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

    create_table_or_insert_from_query(
      conn = conn,
      output_table_name = temp_table_name,
      query = query
    )
  }

  query <- dplyr::tbl(conn, temp_table_name)
  dispenses <- dplyr::collect(query)

  try(
    DBI::dbRemoveTable(conn, temp_table_name),
    silent = TRUE
  )

  DBI::dbDisconnect(conn)
  return(dispenses)
}
