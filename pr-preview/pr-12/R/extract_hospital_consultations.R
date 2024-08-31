#' Extraction des consultations externes à l'hôpital (MCO).
#'
#' Cette fonction permet d'extraire les consultations à l'hôpital en MCO. Les consultations dont les dates
#' EXE_SOI_DTD sont comprises entre start_date et end_date sont extraites.
#'
#' Si spe_codes est renseigné, seules les consultations des spécialités
#' correspondantes sont extraites.
#'
#' Si prestation_codes est renseigné, seules les consultations des prestations correspondantes sont extraites.
#'
#' Si patients_ids est fourni, seules les délivrances
#' de médicaments pour les patients dont les identifiants
#' sont dans patients_ids sont extraites.
#'
#' @param start_date Date La date de début de la période sur laquelle extraire les consultations.
#' @param end_date Date La date de fin de la période sur laquelle extraire les consultations.
#' @param spe_codes character vector Les codes spécialités des médecins effectuant les consultations à extraire. Si `spe_codes` n'est pas fourni, les consultations de tous les spécialités sont extraites.
#' @param prestation_codes character vector Les codes des prestations à extraire. Si `prestation_codes` n'est pas fourni, les consultations de tous les prestations sont extraites. Les codes des prestations sont disponibles sur la page [actes et consultations externes de la documentation SNDS](https://documentation-snds.health-data-hub.fr/snds/fiches/actes_consult_externes.html#exemple-de-requetes-pour-analyse).
#' @param patient_ids data.frame Un data.frame contenant les paires
#' d'identifiants des patients pour lesquels les consultations doivent être
#' extraites. Les colonnes de ce data.frame doivent être "BEN_IDT_ANO" et
#' "BEN_NIR_PSA" (en majuscules). Les "BEN_NIR_PSA" doivent être tous les
#' "BEN_NIR_PSA" associés aux "BEN_IDT_ANO" fournis. Si `patients_ids` n'est pas
#' fourni, les consultations de tous les patients sont extraites.
#' @param output_table_name character Le nom de la table de sortie dans la base de données. Si `output_table_name` n'est pas fourni, une table de sortie intermédiaire est créée.
#' @param conn dbConnection La connexion à la base de données. Si `conn` n'est pas fourni, une connexion à la base de données est initialisée.
#'
#' @return Un data.frame contenant les consultations. Les colonnes sont les suivantes :
#' - BEN_IDT_ANO : Identifiant bénéficiaire anonymisé (seulement si patient_ids non nul)
#' - NIR_ANO_17 : NIR anonymisé
#' - EXE_SOI_DTD : Date de la délivrance
#' - ACT_COD : Code de l'acte
#' - EXE_SPE : Code de spécialité du professionnel de soin prescripteur
#'
#' @examples
#' \dontrun{
#' extract_hospital_consultations(
#'   start_date = as.Date("2019-01-01"),
#'   end_date = as.Date("2019-12-31"),
#'   spe_codes = c("01", "02")
#' )
#' }
#' @export
extract_hospital_consultations <- function(start_date,
                                           end_date,
                                           spe_codes = NULL,
                                           prestation_codes = NULL,
                                           patient_ids = NULL,
                                           output_table_name = NULL,
                                           conn = NULL) {
  if (is.null((conn))) {
    conn <- connect_oracle() # Connect to database
  }
  if (is.null(output_table_name)) {
    output_table_name <-
      paste0("TMP_DISP_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  }

  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(end_date)
  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")

  if (!is.null(patient_ids)) {
    patient_ids_table_name <- "TMP_PATIENT_IDS"
    try(DBI::dbRemoveTable(conn, patient_ids_table_name),
      silent = TRUE
    )
    DBI::dbWriteTable(conn, patient_ids_table_name, patient_ids)
  }

  pb <- progress::progress_bar$new(
    format = "Extracting :year1 (going from :year2 to :year3) \
    [:bar] :percent in :elapsed (eta: :eta)",
    total = (end_year - start_year + 1),
    clear = FALSE,
    width = 80
  )
  pb$tick(0)
  for (year in start_year:end_year) {
    pb$tick(tokens = list(
      year1 = year,
      year2 = start_year,
      year3 = end_year
    ))

    formatted_year <- sprintf("%02d", year %% 100)

    cstc <-
      dplyr::tbl(conn, glue::glue("T_MCO{formatted_year}CSTC")) |>
      filter(
        NIR_RET == "0",
        NAI_RET == "0",
        SEX_RET == "0",
        ENT_DAT_RET == "0",
        IAS_RET == "0"
      ) |>
      select(ETA_NUM, SEQ_NUM, NIR_ANO_17, EXE_SOI_DTD) |>
      distinct()

    fcstc <-
      dplyr::tbl(conn, glue::glue("T_MCO{formatted_year}FCSTC")) |>
      select(ETA_NUM, SEQ_NUM, ACT_COD, EXE_SPE) |>
      distinct()

    date_condition <- glue::glue(
      "EXE_SOI_DTD <= DATE '{formatted_end_date}' AND EXE_SOI_DTD >= DATE '{formatted_start_date}'"
    )
    ace <- cstc |>
      filter(sql(date_condition)) |>
      left_join(fcstc, by = c("ETA_NUM", "SEQ_NUM")) |>
      select(NIR_ANO_17, EXE_SOI_DTD, ACT_COD, EXE_SPE) |>
      distinct()

    if (!is.null(spe_codes)) {
      ace <- ace |>
        filter(EXE_SPE %in% spe_codes)
    }

    if (!is.null(prestation_codes)) {
      ace <- ace |>
        filter(ACT_COD %in% prestation_codes)
    }

    if (!is.null(patient_ids)) {
      patient_ids_table <- tbl(conn, patient_ids_table_name)
      query <- patient_ids_table |>
        inner_join(ace,
          by = c("BEN_NIR_PSA" = "NIR_ANO_17"),
          keep = TRUE
        )
      selected_columns <-
        c(
          "BEN_IDT_ANO",
          "NIR_ANO_17",
          "EXE_SOI_DTD",
          "ACT_COD",
          "EXE_SPE"
        )
    } else {
      query <- ace
      selected_columns <-
        c("NIR_ANO_17", "EXE_SOI_DTD", "ACT_COD", "EXE_SPE")
    }
    query <- query |>
      select(all_of(selected_columns)) |>
      distinct()


    if (!is.null(output_table_name)) {
      create_table_or_insert_from_query(
        conn = conn,
        output_table_name = output_table_name,
        query = query,
        append = (year != start_year)
      )
    }
  }

  query <- dplyr::tbl(conn, output_table_name)
  consultations <- dplyr::collect(query)

  return(consultations)
}
