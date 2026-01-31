#' The forecast requires a lot of data: This script describes what needs to be done
#' each year.
#'
#' 1) RTRA:  upload SAS scripts to https://eft-tef.statcan.gc.ca/, download all the .csv files
#' 2) If new census year, use 20/20 to get regional noc-naics matricies (for employed) and give names EXACTLY like 2026_Cariboo.csv.
#' You MUST also use the correct region names... FAFO.
#' "Cariboo"
#' "Kootenay"
#' "Lower Mainland-Southwest"
#' "Nechako"
#' "North Coast"
#' "Northeast"
#' "Thompson-Okanagan"
#' "Vancouver Island and Coast"
#' 3) Add any updated mapping files or description files (for naics or nocs)
#' 4) Add the new files: year_constraint.xlsx, year_driver.xlsx, year_stokes_supply_demand.xlsx.
#' 5) source this file.

library(arrow)
library(fs)
library(readr)
library(tools)
library(dplyr)

source(here::here("..", "shared_functions", "pond_utilities.R"))
path <- here::here("data_store", "add_to_pond")
prepend <- paste0(as.numeric(format(Sys.time(),"%Y"))-1,"_")

#'bc data catalogue url does not look particularly stable...

bc_stats_url <- "https://catalogue.data.gov.bc.ca/dataset/86839277-986a-4a29-9f70-fa9b1166f6cb/resource/36610a52-6f90-4ed6-946d-587641a490df/download/regional-district-population.csv"

utils::download.file(bc_stats_url,
                     destfile = here::here(path, paste0(prepend,"Population_Projections.csv")),
                     mode = "wb",
                     method = "curl")

#stats can urls look more stable

urls <- c("https://www150.statcan.gc.ca/n1/tbl/csv/17100015-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/17100014-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/14100327-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100446-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100593-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100449-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100316-eng.zip")


#functions---------------------------------

get_cansim_safe <- function(url, path) {
  stopifnot(dir.exists(path))
  table_id <- sub(".*/([0-9]+)-eng\\.zip$", "\\1", url)
  zipfile     <- paste0(prepend, table_id, ".zip")
  zip_path      <- here::here(path, zipfile)
  utils::download.file(url, destfile = zip_path, mode = "wb", method = "curl")
}

#retrieve the data----------------------------

results <- lapply(urls, function(u) {
  tryCatch(
    get_cansim_safe(
      url = u,
      path = path
    ),
    error = function(e) {
      message("Failed: ", u)
      message("  ", e$message)
      return(NULL)
    }
  )
})

ingest_pond("data_store")

parquet_large_csvs(
  data_pond   = "data_store/data_pond",
  data_parquet = "data_store/data_parquet",
  views_dir   = "data_index/views",
  min_size_mb = 250,
  chunk_size = 1000000 #make smaller if running out of ram.
)







