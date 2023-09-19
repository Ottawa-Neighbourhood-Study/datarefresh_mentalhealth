## CODE to create ONS Mental Health Resources data
# NOTE it requries an input file in the same format as the one in data/mental_health_resources_Aug2023 (03).xlsx

library(sf)
library(targets)
library(openxlsx)
library(dplyr)
library(tidyr)
library(tidygeocoder)
library(valhallr)


source ("R/functions.R")

list(
  ## Ottawa inputs
  targets::tar_target(ons_gen3_shp, clean_ons_shp(neighbourhoodstudy::ons_gen3_shp)),
  targets::tar_target(ons_gen3_pop2021, clean_ottawa_pop(neighbourhoodstudy::ons_gen3_pop2021)),
  targets::tar_target(ons_buffer, sf::st_transform(sf::st_buffer(sf::st_union(sf::st_transform(ons_gen3_shp, crs=32189)), 1000), crs="WGS84")),
  targets::tar_target(ottawa_dbs, neighbourhoodstudy::ottawa_dbs_shp2021),
  targets::tar_target(sli_da_ons, neighbourhoodstudy::sli_das_gen3_mape),
  
  ## Mental Health inputs
  targets::tar_target(mh_file, "data/mental_health_resources_Aug2023 (3).xlsx", format="file"),
  targets::tar_target(mh_raw, load_xlsx(mh_file, sheet=1)),
  targets::tar_target(mh_categories, load_categories(mh_file, sheet=2)),
  targets::tar_target(mh_prepped, dplyr::left_join(mh_raw, mh_categories, by = "Type") |> tibble::rowid_to_column(var = "mh_id")),
  
  targets::tar_target(mh_shp, geocode_mh(mh_prepped)),
  
  ## calculate distances
  targets::tar_target(mh_db_dist, get_distances(ottawa_dbs, mh_shp, destinations_id_col = "mh_id")),
  
  ## MH SERVICES ----
  # in hood
  # with buffer / 1000
  # nearest 1
  # nearest 3
  # nearest 5
  targets::tar_target(mh_services, 
                      compute_ons_values_tidy_variable(
                        point_data=dplyr::filter(mh_shp, category == "MH services" ),
                        prefix="mh_services_", distances=mh_db_dist, sli=neighbourhoodstudy::sli_das_gen3_mape, 
                        dbpops=neighbourhoodstudy::ottawa_dbs_pop2021, ons_gen3_shp = ons_gen3_shp, 
                        closest = c(1,3,5)
                      )),
  
  
  
  ## SUPPORT GROUP ----
  #"Support Group"
  # in hood
  # with buffer / 1000
  # nearest 5
  # nearest 10
  targets::tar_target(mh_supportgroup, 
                      compute_ons_values_tidy_variable(
                        point_data=dplyr::filter(mh_shp, category == "Support Group" ),
                        prefix="mh_supportgroup_", distances=mh_db_dist, sli=neighbourhoodstudy::sli_das_gen3_mape, 
                        dbpops=neighbourhoodstudy::ottawa_dbs_pop2021, ons_gen3_shp = ons_gen3_shp, 
                        closest = c(5, 10)
                      )),
  
  ## COUNSELLING ----
  # "Counselling"
  # in hood
  # with buffer / 1000
  # nearest 3
  # nearest 5
  targets::tar_target(mh_counselling, 
                      compute_ons_values_tidy_variable(
                        point_data=dplyr::filter(mh_shp, category == "Counselling" ),
                        prefix="mh_counselling_", distances=mh_db_dist, sli=neighbourhoodstudy::sli_das_gen3_mape, 
                        dbpops=neighbourhoodstudy::ottawa_dbs_pop2021, ons_gen3_shp = ons_gen3_shp, 
                        closest = c(3, 5)
                      )),
  
  
  targets::tar_target(mh_results, {
    
    dplyr::left_join(mh_counselling, mh_services, by= "ONS_ID") |>
      dplyr::left_join(mh_supportgroup, by= "ONS_ID") |>
      tidyr::pivot_longer(names_to = "variable", cols = -ONS_ID) |>
      na_popvalues_in_nonhoods() |>
      dplyr::filter(!stringr::str_detect(variable, "pct")) |>
      tidyr::pivot_wider(names_from = ONS_ID, values_from = value)
  }),
  
  targets::tar_target(mh_savetofile, {
    readr::write_csv(mh_results, paste0("output/ons_mentalhealth_",Sys.Date(),".csv"))
  }),
  
  
  NULL
)