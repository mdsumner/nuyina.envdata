files <- fs::dir_ls(regexp = ".*parquet$")
names <- gsub("\\.parquet", "", basename(files))
l <- lapply(files, \(.x) arrow::read_parquet(.x))
for (i in seq_along(l)) {
  names(l[[i]]) <- c("gml_id", names[i])
}

d <- l[[1]]
for (i in 2:length(l)) {
  d <- dplyr::inner_join(d, l[[i]], "gml_id")
}

## Read Nuyina underway (1-minute interval data collection from the ocean)
get_underway <- function(x) {
  ## read the bulk
  d <- arrow::read_parquet("https://github.com/mdsumner/nuyina.underway/raw/main/data-raw/nuyina_underway.parquet")
  ## read the rest
  d1 <- tibble::as_tibble(vapour::vapour_read_fields("WFS:https://data.aad.gov.au/geoserver/ows?service=wfs&version=2.0.0&request=GetCapabilities",
                                                     sql = sprintf("SELECT * FROM \"underway:nuyina_underway\" WHERE datetime > '%s'",
                                                                   format(max(d$datetime, "%Y-%m-%dT%H:%M:%SZ")))))
  dplyr::bind_rows(d, d1)

}

## reads a cached Parquet and gets more recent rows from the geoserver
uwy <- get_underway()
uwy <- dplyr::distinct(uwy, gml_id, .keep_all = T)

d <- dplyr::inner_join(d, uwy, "gml_id")
