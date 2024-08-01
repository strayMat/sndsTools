library(ROracle)
library(dbplyr)
library(DBI)
library(glue)
source("utils.R")


retrieve_ids <- function(
    tab_xtr_name = NULL,
    opt_nir = 1,
    output_table_name = NULL) {
  conn <- initialize_connection()

  tab_xtr <- tbl(conn, tab_xtr_name)
  ref_ir_ben <- tbl(conn, "REF_IR_BEN")

  ref_ir_ben <- ref_ir_ben %>%
    select(BEN_IDT_ANO, BEN_NIR_PSA, BEN_CDI_NIR, SOURCE) %>%
    distinct()

  ref_nir_opt_2_1 <- tab_xtr %>%
    select(BEN_NIR_PSA) %>%
    distinct() %>%
    inner_join(
      ref_ir_ben,
      by = "BEN_NIR_PSA"
    ) %>%
    select(BEN_IDT_ANO) %>%
    inner_join(
      ref_ir_ben,
      by = "BEN_IDT_ANO"
    ) %>%
    distinct(BEN_IDT_ANO, BEN_NIR_PSA, BEN_CDI_NIR, SOURCE)

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_OPT_2_1",
    query = ref_nir_opt_2_1
  )

  ref_nir_opt_2_1 <- tbl(conn, "REF_NIR_OPT_2_1")

  if (opt_nir == 1) {
    nir_cod <- c("00")
  } else if (opt_nir == 2) {
    nir_cod <- c("00", "03", "04")
  } else if (opt_nir == 3) {
    nir_cod <- c("00", "01", "03", "04")
  } else if (opt_nir == 4) {
    nir_cod <- c("00", "01", "02", "03", "04", "05", "06", "08", "09", "11", "12", "13", "14", "15")
  }

  ref_nir_opt_2_2 <- ref_nir_opt_2_1 %>%
    filter(BEN_CDI_NIR %in% nir_cod | (SOURCE == 2 & is.na(BEN_CDI_NIR))) %>%
    select(BEN_NIR_PSA, BEN_IDT_ANO) %>%
    distinct()

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_OPT_2_2",
    query = ref_nir_opt_2_2
  )

  ref_nir_opt_2_2 <- tbl(conn, "REF_NIR_OPT_2_2")

  # 3 - Filtre sur les beneficiaires sans jumeaux/jumelles (cf. methodo CNAM-TS)
  ref_nir_opt_3_1 <- ref_nir_opt_2_2 %>%
    select(BEN_NIR_PSA, BEN_IDT_ANO) %>%
    distinct() %>%
    group_by(BEN_NIR_PSA) %>%
    filter(n() > 1)

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_OPT_3_1",
    query = ref_nir_opt_3_1
  )
  # Peut etre une erreur ci dessous : anti join a faire
  # sur BEN_NIR_PSA et non BEN_IDT_ANO
  ref_nir_opt_3_2 <- ref_nir_opt_2_2 %>%
    anti_join(ref_nir_opt_3_1, by = "BEN_IDT_ANO") %>%
    mutate()

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_OPT_3_2",
    query = ref_nir_opt_3_2
  )

  ref_nir_opt_3_2 <- tbl(conn, "REF_NIR_OPT_3_2")


  # 4 - Anonymisation de BEN_NIR_OPT  => Non retenue

  # 5 - Filtre sur les BEN_NIR_OPT associes a < 6 BEN_NIR_PSA
  ref_nir_opt_5 <- ref_nir_opt_3_2 %>%
    group_by(BEN_IDT_ANO) %>%
    filter(n() < 6) %>%
    distinct(BEN_IDT_ANO)

  create_table_from_query(
    conn = conn,
    output_table_name = "REF_NIR_OPT_5",
    query = ref_nir_opt_5
  )

  ref_nir_cle <- ref_nir_opt_5 %>%
    inner_join(ref_nir_opt_3_2, by = "BEN_IDT_ANO") %>%
    distinct()

  create_table_from_query(
    conn = conn,
    output_table_name = output_table_name,
    query = ref_nir_cle
  )
  ref_nir_cle <- tbl(conn, output_table_name)

  # 6 - Resume quantitatif (flowchart) de la constitution de la table finale

  res1 <- tab_xtr %>%
    select(BEN_NIR_PSA) %>%
    distinct() %>%
    summarise(N = n()) %>%
    collect()

  res2 <- ref_nir_opt_2_1 %>%
    select(BEN_IDT_ANO) %>%
    distinct() %>%
    summarise(N = n()) %>%
    collect()

  res3 <- ref_nir_opt_2_2 %>%
    select(BEN_IDT_ANO) %>%
    distinct() %>%
    summarise(N = n()) %>%
    collect()

  res4 <- ref_nir_opt_3_2 %>%
    select(BEN_IDT_ANO) %>%
    distinct() %>%
    summarise(N = n()) %>%
    collect()

  res5 <- ref_nir_cle %>%
    select(BEN_IDT_ANO) %>%
    distinct() %>%
    summarise(N = n()) %>%
    collect()


  # Note : par rapport au script ref_nir_opt original, les pseudo-NIR sont ici
  # des BEN_NIR_PSA uniquement et non des couples BEN_NIR_PSA/BEN_RNG_GEM
  flowchart <- bind_rows(
    mutate(res1, I = "Pseudo-NIR sniiram (ben_nir_psa) de la table de depart"),
    mutate(res2, I = "Personnes (ben_idt_ano) correspondantes"),
    mutate(res3, I = "Personnes (ben_idt_ano) avec un ben_cdi_nir normal ou provisoire"),
    mutate(res4, I = "Personnes (ben_idt_ano) sans jumeaux/jumelles du meme sexe"),
    mutate(res5, I = "Personnes (ben_idt_ano) sans ben_nir_psa trop nombreux, incluses dans la table intermediaire de population")
  ) %>% select(I, N)


  tables_to_remove <- c(
    "REF_NIR_OPT_2_1",
    "REF_NIR_OPT_2_2",
    "REF_NIR_OPT_3_1",
    "REF_NIR_OPT_3_2",
    "REF_NIR_OPT_5"
  )
  for (table_name in tables_to_remove) {
    dbRemoveTable(conn, table_name)
  }

  # Fermer la connexion
  dbDisconnect(conn)

  return(flowchart)
}
