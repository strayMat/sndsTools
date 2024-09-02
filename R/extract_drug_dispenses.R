#' Extraction des délivrances de médicaments.
#'
#' Cette fonction permet d'extraire les délivrances de médicaments.
#' Les délivrances dont les dates EXE_SOI_DTD sont comprises
#' entre start_date et end_date (incluses) sont extraites.
#' Le décalage de remontée des données est pris en compte
#' en récupérant également les délivrances dont les dates
#' FLX_DIS_DTD sont comprises dans les `dis_dtd_lag_months`
#' mois suivant end_date.
#' Si atc_cod_starts_with est fourni, seules les délivrances
#' de médicaments dont le code ATC commence par l'un des
#' éléments de atc_cod_starts_with sont extraites. Dans le
#' cas contraire, les délivrances pour tous les codes
#' ATC sont extraites.
#' Si patients_ids est fourni, seules les délivrances
#' de médicaments pour les patients dont les identifiants
#' sont dans patients_ids sont extraites. Dans le cas
#' contraire, les délivrances de tous les patiens sont
#' extraites.
#'
#' @param start_date Date La date de début de la période
#'   des délivrances des médicaments à extraire.
#' @param end_date Date La date de fin de la période
#'   des délivrances des médicaments à extraire.
#' @param atc_cod_starts_with Character vector Optionnel. Les codes ATC
#'   par lesquels les délivrances de médicaments à extraire
#'   doivent commencer.
#' @param dis_dtd_lag_months Integer Optionnel. Le nombre maximum de
#'   mois de décalage de FLX_DIS_DTD par rapport à EXE_SOI DTD pris en compte
#'   pour récupérer les délivrances de médicaments. Par défaut, 7 mois.
#' @param patients_ids data.frame Optionnel. Un data.frame contenant les
#'   paires d'identifiants des patients pour lesquels les délivrances de
#'   médicaments doivent être extraites. Les colonnes de ce data.frame
#'   doivent être "BEN_IDT_ANO" et "BEN_NIR_PSA". Les "BEN_NIR_PSA" doivent
#'   être tous les "BEN_NIR_PSA" associés aux "BEN_IDT_ANO" fournis.
#' @param output_table_name Character Optionnel. Si fourni, les résultats seront
#'   sauvegardés dans une table portant ce nom dans la base de données au lieu
#'   d'être retournés sous forme de data frame.
#' @param overwrite Logical Optionnel. Si TRUE, et si output_table_name est
#'   fourni et que la table de sortie existe déjà, la table de sortie sera
#'   écrasée. Par défaut, FALSE.
#' @param conn DBI connection Une connexion à la base de données Oracle.
#'   Si non fournie, une connexion est établie par défaut.
#' @return Si output_table_name est NULL, retourne un data.frame contenant les
#'   délivrances de médicaments. Si output_table_name est fourni, sauvegarde les
#'   résultats dans la table spécifiée dans Oracle et retourne NULL de manière
#'   invisible. Dans les deux cas les colonnes de la table de sortie sont :
#'   - BEN_NIR_PSA : Colonne présente uniquement si les identifiants
#'   patients (`patients_ids`) ne sont pas fournis. Identifiant SNDS,
#'   ausi appelé pseudo-NIR.
#'   - BEN_IDT_ANO : Colonne présente uniquement si les identifiants
#'   patients (`patients_ids`) sont fournis. Numéro d’inscription
#'   au répertoire (NIR) anonymisé.
#'   - EXE_SOI_DTD : Date de la délivrance
#'   - PHA_ACT_QSN : Quantité délivrée
#'   - PHA_ATC_CLA : Code ATC du médicament délivré
#'   - PHA_PRS_C13 : Code CIP du médicament délivré (nom dans la table
#'    ER_PHA_F : PHA_PRS_C13, nom dans la table IR_PHA_R : PHA_CIP_C13)
#'   - PSP_SPE_COD : Code de spécialité du professionnel de soin prescripteur
#'   (voir nomenclature IR_SPE_V)
#'
#' @examples
#' \dontrun{
#' start_date <- as.Date("2010-01-01")
#' end_date <- as.Date("2010-01-03")
#' atc_cod_starts_with <- c("N04A")
#'
#' dispenses <- extract_drug_dispenses(
#'   start_date = start_date,
#'   end_date = end_date,
#'   atc_cod_starts_with = atc_cod_starts_with
#' )
#' }
#' @export
extract_drug_dispenses <- function(
    start_date = NULL,
    end_date = NULL,
    atc_cod_starts_with = NULL,
    dis_dtd_lag_months = 7,
    patients_ids = NULL,
    output_table_name = NULL,
    overwrite = FALSE,
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

  if (!is.null(output_table_name)) {
    output_table_name_is_temp <- FALSE
    if (!is.character(output_table_name)) {
      stop("output_table_name must be a character string")
    }
    if (DBI::dbExistsTable(conn, output_table_name)) {
      if (overwrite) {
        warning(
          glue::glue(
            "Table {output_table_name} already exists.
            It will be overwritten."
          )
        )
      } else {
        stop(
          glue::glue(
            "Table {output_table_name} already exists.
            Set overwrite = TRUE to overwrite it."
          )
        )
      }
    }
  } else {
    output_table_name_is_temp <- TRUE
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    output_table_name <- glue::glue("TMP_DISP_{timestamp}")
  }
  try(
    DBI::dbRemoveTable(conn, output_table_name),
    silent = TRUE
  )
  dis_dtd_end_date <- lubridate::floor_date(
    lubridate::add_with_rollback(
      end_date,
      months(dis_dtd_lag_months)
    ),
    "months"
  )

  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(dis_dtd_end_date)

  formatted_start_date <- format(start_date, "%Y-%m-%d")
  formatted_end_date <- format(end_date, "%Y-%m-%d")
  formatted_dis_dtd_end_date <- format(dis_dtd_end_date, "%Y-%m-%d")

  first_non_archived_year <- get_first_non_archived_year(conn)

  if (!is.null(atc_cod_starts_with)) {
    print(
      glue::glue(
        "Extracting drug dispenses with ATC codes starting with
        {paste(atc_cod_starts_with, collapse = ' or ')}..."
      )
    )
  } else {
    print(
      glue::glue(
        "Extracting drug dispenses for all ATC codes..."
      )
    )
  }

  if (!is.null(patients_ids)) {
    patients_ids_table_name <- "TMP_PATIENTS_IDS"
    try(DBI::dbRemoveTable(conn, patients_ids_table_name), silent = TRUE)
    DBI::dbWriteTable(conn, patients_ids_table_name, patients_ids)
  }
  pb <- progress::progress_bar$new(
    format = "Extracting :year1 (going from :year2 to :year3)
    [:bar] :percent in :elapsed (eta: :eta)",
    total = (end_year - start_year + 1), clear = FALSE, width = 80
  )
  pb$tick(0)
  for (year in start_year:end_year) {
    pb$tick(tokens = list(year1 = year, year2 = start_year, year3 = end_year))

    arc_suffix <- ifelse(
      year < first_non_archived_year,
      glue::glue("_{year}"),
      ""
    )
    er_prs_f <- dplyr::tbl(conn, glue::glue("ER_PRS_F{arc_suffix}"))
    er_pha_f <- dplyr::tbl(conn, glue::glue("ER_PHA_F{arc_suffix}"))
    ir_pha_r <- dplyr::tbl(conn, "IR_PHA_R")

    starts_with_conditions <- vapply(
      atc_cod_starts_with,
      function(code) glue::glue("PHA_ATC_CLA LIKE '{code}%'"),
      character(1)
    )
    atc_conditions <- paste(starts_with_conditions, collapse = " OR ")
    atc_conditions <- glue::glue("({atc_conditions})")

    if (year == end_year) {
      dis_dtd_condition <- glue::glue(
        "FLX_DIS_DTD >= DATE '{year}-01-01'
        AND FLX_DIS_DTD <= DATE '{formatted_dis_dtd_end_date}'"
      )
    } else {
      dis_dtd_condition <- glue::glue(
        "FLX_DIS_DTD >= DATE '{year}-01-01'
      AND FLX_DIS_DTD <= DATE '{year}-12-01'"
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

    query <- er_prs_f |>
      dplyr::inner_join(er_pha_f, by = dcir_join_keys) |>
      dplyr::inner_join(ir_pha_r, by = c("PHA_PRS_C13" = "PHA_CIP_C13")) |>
      dplyr::filter(
        dbplyr::sql(soi_dtd_condition)
      )

    # Dans les tables archivées par années, toutes les délivrances dont les
    # dates EXE_SOI_DTD appartiennent à l'année d'archivage en question sont
    # présentes. Il n'est donc pas nécessaire de filtrer sur les dates de
    # remontée FLX_DIS_DTD pour les années archivées, sauf si la dernière année
    # à extraire est atteinte, auquel cas il faut prendre en compte le seuil
    # pour les dates de remontée FLX_DIS_DTD défini par dis_dtd_end_date.
    # A l'inverse, pour la table  ER_PRS_F non archivée, un découpage selon
    # les dates de remontée FLX_DIS_DTD est mis en place pour faire une
    # extraction année par année.
    # NB : Ce choix induit une quasi exhaustivité des données pour les années
    # archivées, par opposition aux années très récentes pour lesquelles les
    # données ne sont pas toutes remontées. Il pourrait être souhaitable
    # d'introduire un filtre pour être à flux constant sur l'ensemble des années
    if ((year >= first_non_archived_year) || (year == end_year)) {
      query <- query |>
        dplyr::filter(
          dbplyr::sql(dis_dtd_condition)
        )
    }

    if (!is.null(atc_cod_starts_with)) {
      query <- query |>
        dplyr::filter(dbplyr::sql(atc_conditions))
    }

    cols_to_select <- c(
      "EXE_SOI_DTD",
      "PHA_ACT_QSN",
      "PHA_ATC_CLA",
      "PHA_PRS_C13",
      "PSP_SPE_COD"
    )

    query <- query |>
      dplyr::select(
        BEN_NIR_PSA,
        dplyr::all_of(cols_to_select)
      ) |>
      dplyr::distinct()

    if (!is.null(patients_ids)) {
      patients_ids_table <- dplyr::tbl(conn, patients_ids_table_name)
      patients_ids_table <- patients_ids_table |>
        dplyr::select(BEN_IDT_ANO, BEN_NIR_PSA) |>
        dplyr::distinct()
      query <- query |>
        dplyr::inner_join(patients_ids_table, by = "BEN_NIR_PSA") |>
        dplyr::select(
          BEN_IDT_ANO,
          dplyr::all_of(cols_to_select)
        ) |>
        dplyr::distinct()
    }

    create_table_or_insert_from_query(
      conn = conn,
      output_table_name = output_table_name,
      query = query
    )
  }

  if (!is.null(patients_ids)) {
    try(
      DBI::dbRemoveTable(conn, patients_ids_table_name),
      silent = TRUE
    )
  }

  if (output_table_name_is_temp) {
    query <- dplyr::tbl(conn, output_table_name)
    result <- dplyr::collect(query)
    try(
      DBI::dbRemoveTable(conn, output_table_name),
      silent = TRUE
    )
  } else {
    result <- invisible(NULL)
    message(
      glue::glue("Results saved to table {output_table_name} in the database.")
    )
  }

  if (connection_opened) {
    DBI::dbDisconnect(conn)
  }

  return(result)
}
