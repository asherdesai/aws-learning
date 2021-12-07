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


pull_and_convert <- function(start_date, end_date) {
  
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
  
  bucket <- "asher-matrix-bucket"
  
  for (i in seq_along(filenames)) {
    shell(glue("aws s3 ls s3://{bucket} > {tmp_dir}file_list.txt"))
    con <- file(glue("{tmp_dir}file_list.txt"))
    done_files <- readLines(con)
    done_files <- gsub("mat_", "", done_files)
    close(con)
    if (filenames[i] %in% done_files) next
    
    object_uri <- glue("https://nyc-tlc.s3.amazonaws.com/trip+data/",
                       "{filenames[i]}")
    tmp_file <- glue("{tmp_dir}", "{filenames[i]}")
    download.file(url = object_uri, destfile = tmp_file)
    
    d <- import(tmp_file)
    mat <- create_matrix(d)
    dest_file <- glue("mat_{filenames[i]}")
    dest <- glue("{dest_dir}", "{dest_file}")
    export(mat, dest)
    
    shell(glue("aws s3 cp {dest} s3://{bucket}/{dest_file}"))
  
    file.remove(tmp_file)
  }
  
  file.remove(glue("{tmp_dir}file_list.txt"))
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
