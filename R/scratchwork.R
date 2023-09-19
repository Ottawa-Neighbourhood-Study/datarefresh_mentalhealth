targets::tar_load_everything()
library(sf)

mh_prepped |>
  dplyr::group_by(category) |>
  dplyr::count()

mh_shp|>
  dplyr::group_by(category) |>
  dplyr::count()

mh_shp |>
  sf::st_filter(ons_buffer)|>
  dplyr::group_by(category) |>
  dplyr::count()

popvar_regex = "per_1000|pct"

results_long |>
  dplyr::filter(ONS_ID %in% c(3001,3002)) |>
  dplyr::mutate(value = dplyr::if_else (stringr::str_detect(variable, popvar_regex) & ONS_ID %in% nonhood_ids, NA, value)) |> View()
  
  dplyr::mutate(value = dplyr::if_else (stringr::str_detect(variable, popvar_regex) & ONS_ID %in% nonhood_ids ,
                                      NA, value))

  results_long |> na_popvalues_in_nonhoods() |> View(
    
  )
  