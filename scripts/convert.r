# Pull and convert

library(glue)
library(tidyverse)
library(rio)
library(lubridate)

create_matrix <- function(d) {
  dt <- dtplyr::lazy_dt(d)
  dd <- dt %>% 
    group_by(PULocationID, DOLocationID) %>% 
    count() %>% 
    as_tibble()
  de <- dd %>% 
    expand(PULocationID = 1:265, DOLocationID = 1:265)
  
  dd <- left_join(de, dd) %>% 
    replace(is.na(.), 0)
  
  mat <- matrix(data = dd$n, nrow = 265, ncol = 265, byrow = TRUE)
  
  return(mat)
}

pull_and_convert_win <- function(start_date, end_date) {
  
  create_filenames <- function(start_year_month, end_year_month) {
    dates <- seq(ym(start_year_month),
                 ym(end_year_month),
                 by = "months")
    
    years <- year(dates)
    months <- formatC(month(dates), width = 2, flag = "0")
    filenames <- glue("yellow_tripdata_{years}-{months}.csv")
    return(filenames)
  }
  
  filenames <- create_filenames(start_date, end_date)
  
  tmp_dir <- "../tmpfiles/"
  dest_dir <- "../matrices/"
  
  if (!dir.exists(tmp_dir)) dir.create(tmp_dir)
  if (!dir.exists(dest_dir)) dir.create(dest_dir)
  
  for (i in seq_along(filenames)) {
    # Pull file from nyc-tlc
    object_uri <- glue("https://nyc-tlc.s3.amazonaws.com/trip+data/",
                       "{filenames[i]}")
    tmp_file <- glue("{tmp_dir}", "{filenames[i]}")
    print(glue("Pulling from {object_uri} into {tmp_file}"))
    download.file(url = object_uri, destfile = tmp_file)
    
    # create matrix and 
    d <- import(tmp_file)
    mat <- create_matrix(d)
    dest_file <- glue("{dest_dir}", "mat_{filenames[i]}")
    export(mat, dest_file)
    print(glue("Created {dest_file}, removing {tmp_file}"))
    file.remove(tmp_file)
  }
}

find_most_connected <- function(year, month) {
  date_str <- glue("{year}-{month}")
  date <- ym(date_str)
  year_str <- year(date)
  month_str <- formatC(month(date), width = 2, flag = "0")
  filename <- glue("../matrices/",
                   "mat_yellow_tripdata_{year_str}-{month_str}.csv")
  
  d <- import(filename)
  m <- as.matrix(d)
  max <- which(m == max(m), arr.ind = TRUE)
  return(max)
}
