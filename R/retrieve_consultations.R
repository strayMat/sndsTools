#' Extraction des consultations dans le DCIR.
#' @description
#' Cette fonction permet d'extraire les consultations dans le DCIR. Les
#' délivrances dont les dates EXE_SOI_DTD sont comprises entre start_date et
#' end_date (incluses) sont extraites. Le décalage de remontée des données est
#' pris en compte en récupérant également les délivrances dont les dates
#' FLX_DIS_DTD sont comprises dans les `dis_dtd_lag_months` mois suivant
#' end_date.
#'
#' Si pse_spe_codes est fourni, seules les consultations des spécialités
#' renseignées sont extraites. Dans le cas contraire, les consultations pour
#' toutes les spécialités sont extraites.
#'
#' Si prestation_codes est renseigné, seules les consultations des prestations correspondantes sont extraites.
#'
#' Si patients_ids est fourni, seules les consultations pour les patients dont
#' les identifiants sont dans patients_ids sont extraites. Dans le cas
#' contraire, les consultations de tous les patients sont extraites.
#' @details
#' Pour être à flux constant sur l'ensemble des années, il faut utiliser
#' dis_dtd_lag_months = 27 Cela rallonge le temps d'extraction alors que
#' l'impact sur l'extraction est minime car la Cnam estime que 99 % des soins
#' sont remontés à 6 mois c'est-à-dire pour dis_dtd_lag_months = 6 Voir
#' https://documentation-snds.health-data-hub.fr/snds/formation_snds/initiation/schema_relationnel_snds.html#_3-3-dcir
#'
#' Un guide sur l'activité des médecins libéraux est disponibles sur la page [Activité des médecins libéraux](https://documentation-snds.health-data-hub.fr/snds/fiches/activite_medecins.html#contexte).
#'
#' @param start_date Date. La date de début de la période des délivrances des
#'   médicaments à extraire.
#' @param end_date Date La date de fin de la période des délivrances des
#'   médicaments à extraire.
#' @param pse_spe_codes character vector Les codes spécialités des médecins effectuant les consultations à extraire. Si `pse_spe_codes` n'est pas fourni, les consultations de tous les spécialités sont extraites.
#' @param prestation_codes character vector Les codes des prestations à extraire en norme B5 (colonnne `PRS_NAT_REF`). Si `prestation_codes` n'est pas fourni, les consultations de tous les prestations sont extraites. Les codes des prestations sont disponibles sur la page [Cibler selon les natures de prestations](https://documentation-snds.health-data-hub.fr/snds/fiches/prestation.html).
#' @param patients_ids data.frame Optionnel. Un data.frame contenant les
#'   paires d'identifiants des patients pour lesquels les délivrances de
#'   médicaments doivent être extraites. Les colonnes de ce data.frame
#'   doivent être "BEN_IDT_ANO" et "BEN_NIR_PSA". Les "BEN_NIR_PSA" doivent
#'   être tous les "BEN_NIR_PSA" associés aux "BEN_IDT_ANO" fournis.
#' @param output_table_name Character Optionnel. Si fourni, les résultats seront
#'   sauvegardés dans une table portant ce nom dans la base de données au lieu
#'   d'être retournés sous forme de data frame.
#' @param overwrite Logical. Indique si la table `output_table_name` doit être
#'  écrasée dans le cas où elle existe déjà.
#' @param conn DBI connection Une connexion à la base de données Oracle. Si non
#'   fournie, une connexion est établie par défaut.
#' @return Si output_table_name est NULL, retourne un data.frame contenant les
#'   consultations. Si output_table_name est fourni, sauvegarde les résultats
#'   dans la table spécifiée dans Oracle et retourne NULL de manière invisible.
#'   Dans les deux cas les colonnes de la table de sortie sont :
#'   - BEN_NIR_PSA : Colonne présente uniquement si les identifiants
#'   patients (`patients_ids`) ne sont pas fournis. Identifiant SNDS,
#'   ausi appelé pseudo-NIR.
#'   - BEN_IDT_ANO : Colonne présente uniquement si les identifiants
#'   patients (`patients_ids`) sont fournis. Numéro d’inscription
#'   au répertoire (NIR) anonymisé.
#'   - EXE_SOI_DTD : Date de la délivrance
#'
#' @examples
#' \dontrun{
#' dispenses <- extract_consultations_erprsf(
#'   start_date = as.Date("2010-01-01"),
#'   end_date = as.Date("2010-01-03"),
#'   pse_spe_codes = c("0", "00", "36")
#' )
#' }
#' @export
extract_consultations_erprsf <- function(
    start_date = NULL,
    end_date = NULL,
    pse_spe_codes = NULL,
    prestation_codes = NULL,
    patients_ids = NULL,
    output_table_name = NULL,
    overwrite = NULL,
    conn = NULL) {
  stopifnot(
    !is.null(start_date),
    !is.null(end_date),
    inherits(start_date, "Date"),
    inherits(end_date, "Date"),
    start_date <= end_date
  )
  connection_opened <- FALSE
  if (is.null(conn)) {
    conn <- connect_oracle()
    connection_opened <- TRUE
  }
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  if (!is.null(output_table_name)) {
    output_table_name_is_temp <- FALSE
    stopifnot(
      is.character(output_table_name),
      !DBI::dbExistsTable(conn, output_table_name) || (DBI::dbExistsTable(conn, output_table_name) && overwrite)
    )
    if (DBI::dbExistsTable(conn, output_table_name) && overwrite) {
      warning(
        glue::glue(
          "Table {output_table_name} already exists and will be overwritten."
        )
      )
      DBI::dbRemoveTable(conn, output_table_name)
    }
  } else {
    output_table_name_is_temp <- TRUE
    output_table_name <- glue::glue("TMP_DISP_{timestamp}")
  }
  if (!is.null(patients_ids)) {
    stopifnot(
      identical(
        names(patients_ids),
        c("BEN_IDT_ANO", "BEN_NIR_PSA")
      ),
      !anyDuplicated(patients_ids)
    )
    patients_ids_table_name <- glue::glue("TMP_PATIENTS_IDS_{timestamp}")
    DBI::dbWriteTable(conn, patients_ids_table_name, patients_ids)
  }

  dis_dtd_end_date <-
    end_date |>
    lubridate::add_with_rollback(months(dis_dtd_lag_months)) |>
    lubridate::floor_date("months")

  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(dis_dtd_end_date)

  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")
  formatted_dis_dtd_end_date <- format(dis_dtd_end_date, "%Y-%m-%d")

  first_non_archived_year <- get_first_non_archived_year(conn)

  if (!is.null(pse_spe_codes)) {
    print(
      glue::glue(
        "Extracting consultations from all specialties among  {paste(pse_spe_codes, collapse = ' or ')}..."
      )
    )
  } else {
    print(
      glue::glue(
        "Extracting consultations from all specialties codes..."
      )
    )
  }

  pb <- progress::progress_bar$new(
    format = "Extracting :year1 (going from :year2 to :year3) [:bar] :percent in :elapsed (eta: :eta)",
    total = (end_year - start_year + 1), clear = FALSE, width = 80
  )
  pb$tick(0)
  for (year in start_year:end_year) {
    pb$tick(tokens = list(year1 = year, year2 = start_year, year3 = end_year))
    if (year < first_non_archived_year) {
      er_prs_f <- dplyr::tbl(conn, glue::glue("ER_PRS_F_{year}"))
    } else {
      er_prs_f <- dplyr::tbl(conn, "ER_PRS_F")
    }

    if (year == end_year) {
      dis_dtd_condition <- glue::glue(
        "FLX_DIS_DTD >= DATE '{year}-02-01'
        AND FLX_DIS_DTD <= DATE '{formatted_dis_dtd_end_date}'"
      )
    } else {
      dis_dtd_condition <- glue::glue(
        "FLX_DIS_DTD >= DATE '{year}-02-01'
      AND FLX_DIS_DTD <= DATE '{year + 1}-01-01'"
      )
    }
    soi_dtd_condition <- glue::glue(
      "EXE_SOI_DTD >= DATE '{formatted_start_date}'
      AND EXE_SOI_DTD <= DATE '{formatted_end_date}'"
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

    er_prs_f_clean <- er_prs_f %>%
      dplyr::filter(
        dbplyr::sql(soi_dtd_condition),
        dbplyr::sql(dis_dtd_condition)
      ) |>
      filter(
        (DPN_QLF != 71 | is.na(DPN_QLF)), # Suppression de l'activité des actes et consultations externes (ACE) rémontée pour information, cette activité est mesurée par ailleurs pour les établissements de santé dans le champ de la SAE
        (PRS_DPN_QLP != 71 | is.na(PRS_DPN_QLP)), # Suppression des ACE pour information
        (CPL_MAJ_TOP < 2), # Suppression des majorations
        (CPL_AFF_COD != 16), # Suppression des participations forfaitaires
        !(PSE_STJ_COD %in% c(61, 62, 63, 64, 69)), # Suppression des prestations de professionneles exécutants salariés (impact négligeable)
        "PRS_ACT_QTE" > 0
      )

    cols_to_select <- c(
      "EXE_SOI_DTD", "PSE_SPE_COD", "PFS_EXE_NUM", "PRS_NAT_REF", "PRS_ACT_QTE"
    )
    query <- er_prs_f_clean |>
      filter(
        PRS_NAT_REF %in% prs_nat_ref_codes,
        PSE_SPE_COD %in% pse_spe_codes
      ) |>
      dplyr::select(
        BEN_NIR_PSA,
        dplyr::all_of(cols_to_select)
      ) |>
      dplyr::distinct()



    ben <- ben %>%
      select(BEN_IDT_ANO, BEN_NIR_PSA) %>%
      distinct()

    query <- ben %>%
      inner_join(prs_clean, by = "BEN_NIR_PSA") %>%
      select(BEN_IDT_ANO, EXE_SOI_DTD, PSE_SPE_COD, PFS_EXE_NUM, PRS_NAT_REF, PRS_ACT_QTE) %>%
      distinct()

    create_table_or_insert_from_query(conn = conn, output_table_name = output_table_name, query = query)

    end_time <- Sys.time()
    print(glue("Time taken for year {year}: {round(difftime(end_time, start_time, units='mins'),1)} mins."))
  }

  if (!is.null(r_output_path)) {
    # Save the table to a R data file
    query <- tbl(conn, output_table_name)
    data <- collect(query)
    saveRDS(data, glue("{r_output_path}/{tolower(output_table_name)}.RDS"))
  }

  dbDisconnect(conn)
}
