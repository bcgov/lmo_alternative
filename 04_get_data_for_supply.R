#'This script downloads most of the files necessary for the supply side of the model
#'from statistics canada. We parquet the large files. Each year you need to:

path <- here::here("data","supply_side")

#'1) delete all files in data/supply_side EXCEPT directory "noc_descriptions".
#'2) bc data catalogue url does not look particularly stable... likely have to update:

bc_stats_url <- "https://catalogue.data.gov.bc.ca/dataset/86839277-986a-4a29-9f70-fa9b1166f6cb/resource/36610a52-6f90-4ed6-946d-587641a490df/download/regional-district-population.csv"

utils::download.file(bc_stats_url,
                     destfile = here(path, "Population_Projections.csv"),
                     mode = "wb",
                     method = "curl")

#stats can urls... look more stable

urls <- c("https://www150.statcan.gc.ca/n1/tbl/csv/17100015-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/17100014-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/14100327-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100446-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100593-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100449-eng.zip",
          "https://www150.statcan.gc.ca/n1/tbl/csv/98100316-eng.zip")

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

#example usage
#
# library(dplyr)
# library(arrow)
# library(purrr)
# library(tibble)
#
# ds <- open_dataset(here("data", "supply_side", "98100446_parquet"))
# #this does NOT load the dataset into memory, but allows you to peek inside to find variables to filter on and to select prior to loading into memory.
#
# #find categorical variables and their levels.
# infer_categorical_levels <- function(
#     ds,
#     min_card = 4,
#     max_card = 200,
#     sort_levels = TRUE
# ) {
#   stopifnot(inherits(ds, "Dataset"))
#
#   # 1) Cardinality (cheap, 1 row)
#   card <- ds |>
#     summarise(across(everything(), n_distinct)) |>
#     collect()
#
#   keep <- tibble(
#     column = names(card),
#     n_distinct = as.integer(card[1, ]))|>
#     filter(n_distinct >= min_card, n_distinct <= max_card)
#
#   if (nrow(keep) == 0) {
#     return(keep |> mutate(levels = list())) #short circuit if no variables have cardinality in range.
#   }
#
#   # 3) Retrieve levels (column-by-column)
#   levels <- map(keep$column, function(col) {
#     q <- ds |> distinct(!!sym(col))
#     if (sort_levels) q <- q |> arrange(!!sym(col))
#     q |> collect() |> pull(1)
#   })
#
#   # 4) Return named list
#   keep |>
#     mutate(levels = levels)|>
#     select(-n_distinct)|>
#     deframe()
# }
#
# infer_categorical_levels(ds)
#
# #now we know the variable names and levels, we can filter, select, and then finally collect... collect is what loads data into RAM... you can see from below that the dataset we actually end up loading into RAM is tiny compared to the original table.
#
# ds|>
#   filter(GEO == "British Columbia",
#        `Immigrant status and period of immigration (11)`%in% c("2016 to 2021","Non-permanent residents"),
#        `Highest certificate, diploma or degree (7)`=="Total - Highest certificate, diploma or degree",
#        `Age (15A)`%in% c("15 to 24 years","25 to 64 years"),
#        `Gender (3)`=="Total - Gender",
#        `Visible minority (15)`=="Total - Visible minority",
#        `Statistics (3)`=="Count")|>
#   select(contains("status"), contains("age"))|>
#   collect()
#
#
