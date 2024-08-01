library(ROracle)
library(dplyr)
library(dbplyr)
library(DBI) # For database operations
library(glue)
library(knitr)
# setwd("/sasdata/prd/users/44a001280010899/snds_extraction_pipeline/referentiel")
source("utils.R")
source("ref_ir_ben.R")
source("ref_nir_cle_opt.R")
source("ref_nir_dem.R")
source("ref_nir_dc.R")


build_population <- function(
    tab_xtr_name = NULL,
    project_name = NULL,
    population_name = NULL,
    opt_nir = 1,
    build_ir_ben = TRUE) {
    if (build_ir_ben) {
        print("***** Retrieval of all IDs and Demographics available in the SNDS *****")
        start_time <- Sys.time()
        create_ref_ir_ben()
        end_time <- Sys.time()
        print(glue("Time taken : {round(difftime(end_time, start_time, units='mins'),1)} mins."))
    }

    
    print("***** Building Population IDs Table *****")
    aliases_table_name <- glue("{project_name}_{population_name}_IDS")

    start_time <- Sys.time()
    flowchart <- retrieve_ids(
        tab_xtr_name = tab_xtr_name,
        opt_nir = opt_nir,
        output_table_name = aliases_table_name
    )
    print("Flowchart of the population IDs table creation:")
    print(kable(flowchart, col.names = c("Description", "Effectif")))
    
    end_time <- Sys.time()
    print(glue("Time taken : {round(difftime(end_time, start_time, units='mins'),1)} mins."))

    print("***** Building Population Demographics Table *****")
    demographics_table_name <- glue("{project_name}_{population_name}_DEM")
    
    start_time <- Sys.time()
    
    flowchart <- create_ref_nir_dem(
        aliases_table_name = aliases_table_name,
        demographics_table_name = demographics_table_name
    )
    print("Flowchart of birth dates and sex table creation:")
    print(kable(flowchart, col.names = c("Description", "Effectif")))

    flowchart <- create_ref_nir_dc(
        aliases_table_name = aliases_table_name,
        demographics_table_name = demographics_table_name
    )
    print("Flowchart of birth dates and sex table creation:")
    print(kable(flowchart, col.names = c("Description", "Effectif")))

    end_time <- Sys.time()
    print(glue("Time taken : {round(difftime(end_time, start_time, units='mins'),1)} mins."))
}

build_population(
    tab_xtr_name = "TEST_SAMPLE_PSA",
    project_name = "PROJECT",
    population_name = "TESTPOP",
    opt_nir = 1,
    build_ir_ben = TRUE
)
