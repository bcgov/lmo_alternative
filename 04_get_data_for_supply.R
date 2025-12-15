#'This script downloads most of the files necessary for the supply side of the model
#'from statistics canada. We parquet the large files. Each year you need to:

#'1) download the file "Population_Projections.csv" from https://bcstats.shinyapps.io/popApp/.  Choose:
  #'region type=="Regional District",
  #'region=="British Columbia",
  #''year %in% 2000:year(today())+10,
  #'gender=="Totals",
  #'statistic=="counts",
  #'age format=="single age groups",
  #'display as columns=="none".
#'2) delete all files in data/supply_side (but leave directories "noc_descriptions" and"put_bc_stats_demographics_here").
#'3) delete 05_supply_cache.
#'4) source this file.
#'5) render file 05_supply.qmd

#constants--------------------
urls <- c("https://www150.statcan.gc.ca/n1/tbl/csv/17100015-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/17100014-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/14100327-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100446-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100593-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100449-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100316-eng.zip")

path <- here::here("data","supply_side")

#functions---------------------------------

get_cansim_safe <- function(
    url,
    path,
    partition_by = NULL,
    size_cutoff = 500 * 1024^2,
    refresh = FALSE
) {
  stopifnot(dir.exists(path))

  table_id <- sub(".*/([0-9]+)-eng\\.zip$", "\\1", url)

  zipfile     <- paste0(table_id, ".zip")
  csvfile     <- paste0(table_id, ".csv")
  parquet_dir <- paste0(table_id, "_parquet")
  sentinel    <- paste0(table_id, ".parquet.ok")

  zip_path      <- here::here(path, zipfile)
  csv_path      <- here::here(path, csvfile)
  parquet_path  <- here::here(path, parquet_dir)
  sentinel_path <- here::here(path, sentinel)

  # ---- refresh handling ----
  if (isTRUE(refresh)) {
    if (file.exists(zip_path))      file.remove(zip_path)
    if (file.exists(csv_path))      file.remove(csv_path)
    if (dir.exists(parquet_path))   unlink(parquet_path, recursive = TRUE)
    if (file.exists(sentinel_path)) file.remove(sentinel_path)
    message("Refresh enabled: old files removed.")
  }

  # ---- skip if parquet already exists ----
  if (file.exists(sentinel_path)) {
    message("Parquet already exists — skipping.")
    return(invisible(parquet_path))
  }

  # ---- download zip if needed ----
  if (!file.exists(zip_path)) {
    utils::download.file(url, destfile = zip_path, mode = "wb", method = "curl")
  }

  # ---- inspect ZIP contents ----
  zip_info <- utils::unzip(zip_path, list = TRUE)

  if (!csvfile %in% zip_info$Name) {
    stop("CSV file not found inside ZIP: ", csvfile)
  }

  csv_size <- zip_info$Length[zip_info$Name == csvfile]

  # ---- small file: unzip and return CSV ----
  if (csv_size < size_cutoff) {
    if (!file.exists(csv_path)) {
      utils::unzip(zip_path, exdir = path, files = csvfile)
    }
    message("CSV below cutoff — leaving as CSV.")
    return(invisible(csv_path))
  }

  # ---- unzip large CSV if needed ----
  if (!file.exists(csv_path)) {
    utils::unzip(zip_path, exdir = path, files = csvfile)
  }

  # ---- fix header (duplicate column names) ----
  header <- names(data.table::fread(csv_path, nrows = 0))
  header_unique <- make.unique(header, sep = "_")

  # ---- open CSV lazily with Arrow ----
  ds <- arrow::open_dataset(
    sources = csv_path,
    format = "csv",
    column_names = header_unique
  )

  # ---- write Parquet ----
  if (is.null(partition_by) || !length(partition_by)) {
    arrow::write_dataset(
      ds,
      parquet_path,
      format = "parquet"
    )
  } else {
    arrow::write_dataset(
      ds,
      parquet_path,
      format = "parquet",
      partitioning = partition_by
    )
  }

  # ---- cleanup ----
  file.remove(csv_path)
  file.remove(zip_path)

  # ---- success sentinel ----
  file.create(sentinel_path)

  message("Parquet created at: ", parquet_path)
  invisible(parquet_path)
}

#retrieve the data----------------------------

results <- lapply(urls, function(u) {
  tryCatch(
    get_cansim_safe(
      url = u,
      path = path,
      partition_by = NULL
    ),
    error = function(e) {
      message("Failed: ", u)
      message("  ", e$message)
      return(NULL)
    }
  )
})









