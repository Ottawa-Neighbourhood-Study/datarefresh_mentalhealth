# Functions for ONS Mental Health data refresh

message("Loading functions...")


# take neighbourhoodstudy::ons_gen3_pop2021, add Ottawa row, remove unneeded cols
clean_ottawa_pop <- function(pops){
  dplyr::bind_rows(dplyr::summarise(pops, SF_TotalPop = sum(SF_TotalPop)) |>
                     dplyr::mutate(ONS_Name = "OTTAWA", ONS_ID = "0"),
                   pops) |>
    dplyr::select(ONS_ID, SF_TotalPop)
}

# Take the full ONS gen3 and filter it down to just the Ottawa region, including
# one union shape for ottawa itself. This allows us to get neighbourhood and
# city-wide stats using the same methods
clean_ons_shp <- function(ons_shp, create_ottawa_shp = FALSE) {
  ons_shp <- ons_shp |>
    dplyr::filter(ONS_Region == "OTTAWA") |>
    dplyr::select(ONS_ID, ONS_Name)
  
  if (create_ottawa_shp){
  ons_shp  <-
    sf::st_union(ons_shp) |>
    sf::st_as_sf() |>
    dplyr::rename(geometry = x) |>
    dplyr::mutate(ONS_ID = "0", ONS_Name = "OTTAWA", .before= 1) |>
    dplyr::bind_rows(ons_shp) |>
    sf::st_make_valid()
  }
  
  return(ons_shp)
}

load_xlsx <- function (filepath, sheet) {
  
  result <- dplyr::as_tibble(openxlsx::readWorkbook(filepath, sheet))  
  
  return (result)
}

geocode_mh <- function(mh_raw){
  
  # load api key into api_key
  source(".ignorethis")
  Sys.setenv(GOOGLEGEOCODE_API_KEY = api_key)
  
  geocoded <- mh_raw |> 
    tidygeocoder::geocode(address = Address, method="google", long="lon")
  
  results <- sf::st_as_sf(geocoded, coords=c("lon","lat"), remove=FALSE, crs="WGS84")
  
  return(results)
  
}


load_categories <- function(filepath, sheet) {
  
  result <- dplyr::as_tibble(openxlsx::readWorkbook(filepath, sheet))  |>
    dplyr::select(1:2) |>
    tidyr::drop_na()
  
  return(result)
}


# destinations = mh_shp; destinations_id_col = "mh_id"; parallel_workers=20; testsize=100
get_db_centroid_walkdistance <- function(ottawa_dbs, destinations, destinations_id_col, parallel_workers = 10, testsize = NA) {
  
  
  # parallel_workers=10
  # testsize=100
  # handle either shp input or tibble with lat and lon columns
  if ("lat" %in% colnames(destinations)) {
    message("lat column present")
    destinations_latlon <- destinations
    
  } else {
    message("last column NOT present")
    destinations_latlon <- dplyr::bind_cols(
      destinations, sf::st_coordinates(destinations)
    ) |>
      dplyr::rename(lat = Y, lon = X) |>
      sf::st_drop_geometry() |>
      dplyr::select(dplyr::all_of(c(destinations_id_col, "lat", "lon")))
    
  }
  
  destinations <- sf::st_drop_geometry(destinations)
  
  db_centroids <- sf::st_centroid(sf::st_make_valid(ottawa_dbs)) |> suppressWarnings()
  
  db_centroids_latlon <- dplyr::bind_cols(
    db_centroids, sf::st_coordinates(db_centroids)
  ) |>
    dplyr::rename(lat = Y, lon = X) |>
    sf::st_drop_geometry() |>
    dplyr::select(DBUID, lat, lon)
  
  if (!is.na(testsize)) {
    warning("testing with only some data! use with caution!!!!")
    db_centroids_latlon <-  head(db_centroids_latlon, n = testsize)
  }
  
  future::plan(future::multisession, workers = parallel_workers)
  
  batchsize <- 500
  
  tranches <-
    db_centroids_latlon |>
    tibble::rowid_to_column() |>
    dplyr::group_by(rowid) |>
    dplyr::group_split()
  
  results <- furrr::future_map(tranches, function(db_centroids_tranche) {
    valhallr::od_table(froms = db_centroids_tranche, from_id_col = "DBUID",
                       costing="pedestrian",
                       batch_size=batchsize,
                       tos = destinations_latlon, to_id_col = destinations_id_col, verbose = TRUE , hostname = "192.168.0.150")
  }, .progress=TRUE)
  
  future::plan(future::sequential)
  
  results <- dplyr::bind_rows(results)
  results
}



## a stupid function to do it in two tranches because i kept getting errors about ``C stack usage is too close to the limit``
get_distances <- function(ottawa_dbs, mh_shp, destinations_id_col = "mh_id"){
  
  dbs1 <- ottawa_dbs[1:4000,]  
  dbs2 <- ottawa_dbs[4001:nrow(ottawa_dbs),]
  
  dist1 <- get_db_centroid_walkdistance(dbs1, mh_shp, destinations_id_col = "mh_id")
  dist2 <- get_db_centroid_walkdistance(dbs2, mh_shp, destinations_id_col = "mh_id")
  
  result <- dplyr::bind_rows(dist1, dist2)
  
  return(result)
  
  }


# function to set to NA pop-based values in "non-neighbourhoods "
# 1.  3019: Carleton U
# 2.  3084: Orleans industrial
# 3.  3006: beechwood cemetery
# 4.  3030: Colonnade business park
# 5.  3064: LeBreton development
# 6.  3042: Experimental farm
# 7.  3057: Industrial East
# 8.  3051: Greenbelt West
# 9.  3001: Airport
# 10. 3050: Greenbelt East
na_popvalues_in_nonhoods <- function(results_long, popvar_regex = "per_1000|pct",  nonhood_ids = c(3019, 3084, 3006, 3030, 3064, 3042, 3057, 3051, 3001, 3050)) {
  
  results_long |>
    dplyr::mutate(value = dplyr::if_else (stringr::str_detect(variable, popvar_regex) & ONS_ID %in% nonhood_ids ,
                                  NA, value))
  
}



# mh_db_dist |>
#   dplyr::left_join(mh_prepped, by = "mh_id")
# prefix="test"; point_data=mh_shp; distances=mh_db_dist; sli=neighbourhoodstudy::sli_das_gen3_mape; dbpops=neighbourhoodstudy::ottawa_dbs_pop2021
# 
# compute_ons_values_tidy <- function(prefix, point_data, distances, ons_gen3_shp, sli = neighbourhoodstudy::sli_das_gen3_mape, dbpops = neighbourhoodstudy::ottawa_dbs_pop2021){
#   # remove errant items
#   distances <- dplyr::filter(distances, !is.na(DBUID))
#   
#   # get the four values of interest
#   num_per_region <- neighbourhoodstudy::get_number_per_region_plus_buffer(ons_gen3_shp, point_data, buffer_m=0)
#   num_per_1000_res_plus_buffer <- neighbourhoodstudy::get_number_per_region_per_1000_residents_plus_buffer(region_shp = ons_gen3_shp, point_data = point_data, pop_data = neighbourhoodstudy::ons_gen3_pop2021, region_id_col = "ONS_ID", pop_col = "SF_TotalPop")
#   avg_dist_closest_3 <- neighbourhoodstudy::get_avg_dist_to_closest_n(distances, from_id_col = "DBUID", to_id_col = "osm_id", froms_to_ons_sli = neighbourhoodstudy::sli_das_gen3_mape, dbpops = neighbourhoodstudy::ottawa_dbs_pop2021, n=3)
#   pct_within_15_minutes <- neighbourhoodstudy::get_pct_within_15_mins(od_table = distances, from_id_col = "DBUID", to_id_col = "osm_id", froms_to_ons_sli = sli, dbpops = dbpops)
#   
#   # combine and tidy
#   results_tidy <- dplyr::left_join(num_per_region, num_per_1000_res_plus_buffer, by = "ONS_ID") |>
#     dplyr::left_join(avg_dist_closest_3, by = "ONS_ID") |>
#     dplyr::left_join(pct_within_15_minutes, by = "ONS_ID") |>
#     dplyr::mutate(dplyr::across(dplyr::where(is.numeric), \(x) round(x, digits = 3)  )) |>
#     dplyr::rename_with(.cols = - ONS_ID, .fn = \(x) paste0(prefix, x))
#   
#   return(results_tidy)
# }



## function to compute the ONS variables including variable n closests
compute_ons_values_tidy_variable <- function(prefix, point_data, distances, ons_gen3_shp, sli = neighbourhoodstudy::sli_das_gen3_mape, dbpops = neighbourhoodstudy::ottawa_dbs_pop2021, closest = 0){
  # remove errant items
  distances <- dplyr::filter(distances, !is.na(DBUID))
  
  # get the four values of interest
  num_per_region <- neighbourhoodstudy::get_number_per_region_plus_buffer(ons_gen3_shp, point_data, buffer_m=0)
  num_per_1000_res_plus_buffer <- neighbourhoodstudy::get_number_per_region_per_1000_residents_plus_buffer(region_shp = ons_gen3_shp, point_data = point_data, pop_data = neighbourhoodstudy::ons_gen3_pop2021, region_id_col = "ONS_ID", pop_col = "SF_TotalPop")
  
  pct_within_15_minutes <- neighbourhoodstudy::get_pct_within_15_mins(od_table = distances, from_id_col = "DBUID", to_id_col = "osm_id", froms_to_ons_sli = sli, dbpops = dbpops)

  if (all(closest != 0)  ) {
  
    closests <- dplyr::tibble()
    
    for ( i in closest) {
      message(i)
      avg_dist_closest_i <- neighbourhoodstudy::get_avg_dist_to_closest_n(distances, from_id_col = "DBUID", to_id_col = "osm_id", froms_to_ons_sli = neighbourhoodstudy::sli_das_gen3_mape, dbpops = neighbourhoodstudy::ottawa_dbs_pop2021, n=i)    
      
      # work around underlying bug with column names in flexible way
      if (("dist_closest_3_popwt" %in% colnames(avg_dist_closest_i) && i != 3)) {
        avg_dist_closest_i[,paste0("dist_closest_", i, "_popwt")] <- avg_dist_closest_i[,"dist_closest_3_popwt"]
        avg_dist_closest_i[,"dist_closest_3_popwt"] <- NULL
      }
        
      if (nrow(closests) == 0) {
        closests <- avg_dist_closest_i
      } else {
        closests <- dplyr::left_join(closests, avg_dist_closest_i, by = "ONS_ID")
      }
    }
  }
  
  
  
  # combine and tidy
  results_tidy <- dplyr::left_join(num_per_region, num_per_1000_res_plus_buffer, by = "ONS_ID") |>
    dplyr::left_join(closests, by = "ONS_ID") |>
    dplyr::left_join(pct_within_15_minutes, by = "ONS_ID") |>
    dplyr::mutate(dplyr::across(dplyr::where(is.numeric), \(x) round(x, digits = 3)  )) |>
    dplyr::rename_with(.cols = - ONS_ID, .fn = \(x) paste0(prefix, x))
  
  return(results_tidy)
}

message("...done loading functions")
