#' Extraction des consultations en médecine de ville
#'
#' Cette fonction permet d'extraire les  consultations en
#' médecine de ville.
#' Les consultations dont les dates EXE_SOI_DTD sont comprises
#' entre start_date et end_date (incluses) sont extraites.
#' Le décalage de remontée des données est pris en compte
#' en récupérant également les délivrances dont les dates
#' FLX_DIS_DTD sont comprises dans les `dis_dtd_lag_months`
#' mois suivant end_date.
#' Si pse_spe_codes est fourni, seules les consultations
#' dont ... sont extraites. Dans le
#' cas contraire, les consultations pour ...
#' sont extraites.
#' Si prs_nat_ref_codes est fourni, seules les consultations
#' dont ... sont extraites. Dans le
#' cas contraire, les consultations pour ...
#' sont extraites.
#' Si patients_ids est fourni, seules les consultations
#' des patients dont les identifiants sont dans patients_ids sont
#' extraites. Dans le cas contraire, les consultations de tous les
#' patients sont extraites.
#'
#' @param start_date Date La date de début de la période
#'   des consultations à extraire.
#' @param end_date Date La date de fin de la période
#'   des consultations à extraire.
#' @param pse_spe_codes Integer vector Optionnel. Les codes des
#'   spécialités médicales de médecin à extraire (voir nomenclature IR_SPE_V).
#' @param prs_nat_ref_codes Integer vector Optionnel. Les codes des
#'   ...
#' @param dis_dtd_lag_months Integer Optionnel. Le nombre maximum de
#'   mois de décalage de FLX_DIS_DTD par rapport à EXE_SOI DTD pris en compte
#'   pour récupérer les consultations. Par défaut, 7 mois.
#' @param patients_ids data.frame Optionnel. Un data.frame contenant les
#'   paires d'identifiants des patients pour lesquels les délivrances de
#'   médicaments doivent être extraites. Les colonnes de ce data.frame
#'   doivent être "ben_idt_ano" et "ben_nir_psa" (en minuscules). Les
#'   "ben_nir_psa" doivent être tous les "ben_nir_psa" associés aux
#'   "ben_idt_ano" fournis.
#' @param output_table_name Character Optionnel. Si fourni, les résultats seront
#'   sauvegardés dans une table portant ce nom dans la base de données au lieu
#'   d'être retournés sous forme de data frame.
#' @param conn DBI connection Une connexion à la base de données Oracle.
#'   Si non fournie, une connexion est établie par défaut.
#' @return Si output_table_name est NULL, retourne un data.frame contenant les
#'   consultations. Si output_table_name est fourni, sauvegarde les
#'   résultats dans la table spécifiée dans Oracle et retourne NULL de manière
#'   invisible. Dans les deux cas les colonnes de la table de sortie sont :
#'   - BEN_NIR_PSA : Colonne présente uniquement si les identifiants
#'   patients (`patients_ids`) ne sont pas fournis. Identifiant SNDS,
#'   ausi appelé pseudo-NIR.
#'   - BEN_IDT_ANO : Colonne présente uniquement si les identifiants
#'   patients (`patients_ids`) sont fournis. Numéro d’inscription
#'   au répertoire (NIR) anonymisé.
#'   - EXE_SOI_DTD : Date de la consultation
#'   - PSE_SPE_COD : Code de spécialité du professionnel de soin exécutant
#'   (voir nomenclature IR_SPE_V)
#'   (autres colonnes)
#'
#' @examples
#' \dontrun{
#' start_date <- as.Date("2010-01-01")
#' end_date <- as.Date("2010-01-03")
#' pse_spe_codes <- c()
#' prs_nat_ref_codes <- c()
#'
#' dispenses <- extract_private_consultations(
#'   start_date = start_date,
#'   end_date = end_date,
#'   pse_spe_codes = NULL,
#'   prs_nat_ref_codes = NULL
#' )
#' }
#' @export
extract_private_consultations <- function(
    start_date = NULL,
    end_date = NULL,
    pse_spe_codes = NULL,
    prs_nat_ref_codes = NULL,
    dis_dtd_lag_months = 7,
    patients_ids = NULL,
    output_table_name = NULL,
    conn = NULL) {
  if (is.null(start_date) || is.null(end_date)) {
    stop("Both start_date and end_date must be provided.")
  }

  if (!inherits(start_date, "Date") || !inherits(end_date, "Date")) {
    stop("start_date and end_date must be Date objects.")
  }

  if (start_date > end_date) {
    stop("start_date must be earlier than or equal to end_date.")
  }

  connection_opened <- FALSE
  if (is.null(conn)) {
    conn <- initialize_connection()
    connection_opened <- TRUE
  }

  if (!is.null(output_table_name)) {
    if (!is.character(output_table_name)) {
      stop("output_table_name must be a character string")
    }
    table_name <- output_table_name
    if (DBI::dbExistsTable(conn, table_name)) {
      warning(paste("Table", table_name, "already exists. It will be overwritten."))
    }
  } else {
    table_name <- paste0("TMP_DISP_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  }
  try(
    DBI::dbRemoveTable(conn, table_name),
    silent = TRUE
  )
  dis_dtd_end_date <- end_date + lubridate::days(30 * dis_dtd_lag_months)
  start_year <- lubridate::year(start_date)
  end_year <- lubridate::year(dis_dtd_end_date)

  first_non_archived_year <- get_first_non_archived_year(conn)

  for (year in start_year:end_year) {
    start_time <- Sys.time()
    print(glue("Processing year: {year}"))
    formatted_year_start <- glue("{year}-01-01")
    formatted_year_end <- glue("{year}-12-31")

    arc_suffix <- ifelse(year < first_non_archived_year, paste0("_", year), "")
    prs_table_name <- glue("ER_PRS_F{arc_suffix}")

    prs <- tbl(conn, prs_table_name)
    ben <- tbl(conn, ben_table_name)


    prs_clean <- prs %>%
      filter(
        EXE_SOI_DTD >= TO_DATE(formatted_start_date, "YYYY-MM-DD"),
        EXE_SOI_DTD <= TO_DATE(formatted_end_date, "YYYY-MM-DD"),
        FLX_DIS_DTD >= TO_DATE(formatted_year_start, "YYYY-MM-DD"),
        FLX_DIS_DTD <= TO_DATE(formatted_year_end, "YYYY-MM-DD"),
        FLX_DIS_DTD - EXE_SOI_DTD < 183 & # pour être à flux constant sur la durée pour les remontées de données
          (DPN_QLF != 71 | is.na(DPN_QLF)), # Suppression de l'activité des actes et consultations externes (ACE) rémontée pour information, cette activité est mesurée par ailleurs pour les établissements de santé dans le champ de la SAE
        (PRS_DPN_QLP != 71 | is.na(PRS_DPN_QLP)), # Suppression des ACE pour information
        (CPL_MAJ_TOP < 2), # Suppression des majorations
        (CPL_AFF_COD != 16), # Suppression des participations forfaitaires
        !(PSE_STJ_COD %in% c(61, 62, 63, 64, 69)) # Suppression des prestations de professionneles exécutants salariés (impact négligeable)
      ) %>%
      select(
        BEN_NIR_PSA, EXE_SOI_DTD, PSE_SPE_COD, PFS_EXE_NUM, PRS_NAT_REF, PRS_ACT_QTE
      )
    prs_clean <- prs_clean %>%
      filter(
        PRS_ACT_QTE > 0,
        PRS_NAT_REF %in% prs_nat_ref_codes,
        PSE_SPE_COD %in% pse_spe_codes
      )

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
