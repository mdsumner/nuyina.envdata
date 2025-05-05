files <- fs::dir_ls(regexp = ".*parquet$")
files <- setdiff(files, "envdata.parquet")
names <- gsub("\\.parquet", "", basename(files))
l <- lapply(files, \(.x) arrow::read_parquet(.x))
for (i in seq_along(l)) {
  names(l[[i]]) <- c("gml_id", names[i])
}

d <- l[[1]]
for (i in 2:length(l)) {
  d <- dplyr::inner_join(d, l[[i]], "gml_id")
}


arrow::write_parquet(d, "envdata.parquet")
