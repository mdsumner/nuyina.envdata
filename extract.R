.libPaths("/software/projects/pawsey0973/mdsumner/R/x86_64-pc-linux-gnu-library/")

## new parallel framework for purrr, via mirai
library(purrr)
library(mirai)
daemons(parallelly::availableCores())


variables <- tibble::tibble(dataid = c("esacci-tif", "oisst-tif",
                                       "ghrsst-tif",
                                       "NSIDC_SEAICE_PS_S25km",
                                       "antarctica-amsr2-asi-s3125-tif",
                                       rep("SEALEVEL_GLO_PHY_L4", 4)),
                            varname = c(1, 1, 1, 1, 1, "sla", "ugos", "vgos", "adt"))



## idea extract for Nuyina

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
d <- get_underway()
d <- dplyr::distinct(d, gml_id, .keep_all = T)



for (i in seq_len(nrow(variables))) {
var <- variables[i, ]
dataid <- var$dataid[1L]
varname <- var$varname[1L]

## GHRSST files on source.coop
library(sooty)
ds <- dataset(); ds@id <- dataid
files <- ds@source




## exact match because have every day (not right up to the minute)
d$day <- match(as.Date(d$datetime), as.Date(files$date))
if (dataid == "esacci-tif") {
  d$day <- findInterval(d$datetime, files$date)
}

## subset to the files we can match to
d$var <- varname
nuy <- d[!is.na(d$day), c("longitude", "latitude", "datetime", "day", "var", "gml_id")]
nuy$file <- files$source[nuy$day]
l <- split(nuy, nuy$day)
## function to apply in parallel to get GHRSST on underway
extractit <- function(x) {
  var <- NULL
  if (!is.null(x$var) && !is.na(x$var[1])) var <- x$var[1]

  if (!is.na(as.integer(var))) {
    var <- as.integer(var)-1
  }
  data <- terra::rast(x$file[1], var)
  xy <- cbind(x$longitude, x$latitude)

  if (!gdalraster::srs_is_geographic(terra::crs(data))) {
    xy <- gdalraster::transform_xy(xy, srs_to = terra::crs(data), srs_from = "EPSG:4326")
  }
  terra::extract(data, xy)[,1L, drop = TRUE]
}

### takes about 2 minutes for GHRSST, 1e6 ship locations, 725 days of voyaging

print(varname)

st <- system.time({
  l2 <- map(l, extractit, .parallel = TRUE)
})

print(st)

out <- tibble::tibble(gml_id = nuy$gml_id, value = unlist(l2))
arrow::write_parquet(out, sprintf("%s_%s.parquet", dataid, gsub("-", "_", varname)))
}
