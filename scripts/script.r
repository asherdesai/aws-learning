library(glue)
library(tidyr)
library(dplyr)
library(dtplyr)
library(data.table)
library(tibble)
library(rio)
library(lubridate)

args = commandArgs(trailingOnly = TRUE)

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
  
  if (!dir.exists(tmp_dir)) dir.create(tmp_dir)
  
  bucket <- "asher-matrix-bucket"
  
  for (i in seq_along(filenames)) {
    system(glue("aws s3 ls s3://{bucket} > {tmp_dir}file_list.txt"))
    con <- file(glue("{tmp_dir}file_list.txt"))
    done_files <- readLines(con)
    done_files <- gsub("mat_", "", done_files)
    close(con)
    if (filenames[i] %in% done_files) next
    
    object_uri <- glue("https://nyc-tlc.s3.amazonaws.com/trip+data/",
                       "{filenames[i]}")
    raw_file <- glue("{tmp_dir}", "{filenames[i]}")
    download.file(url = object_uri, destfile = raw_file)
    
    d <- import(raw_file)
    m <- create_matrix(d)
    mat_name <- glue("mat_{filenames[i]}")
    mat_file <- glue("{tmp_dir}", "{mat_name}")
    export(m, mat_file)
    
    system(glue("aws s3 cp {mat_file} s3://{bucket}/{mat_name}"))
    file.remove(mat_file) 
    file.remove(raw_file)
  }
  
  file.remove(glue("{tmp_dir}file_list.txt"))
}

find_most_connected <- function(year, month) {
  date_str <- glue("{year}-{month}")
  date <- ym(date_str)
  year_str <- year(date)
  month_str <- formatC(month(date), width = 2, flag = "0")

  bucket <- "asher-matrix-bucket"

  if (!dir.exists("tmpfiles/")) dir.create("tmpfiles/")

  filename <- glue("mat_yellow_tripdata_{year_str}-{month_str}.csv")

  system(glue("aws s3 cp s3://{bucket}/{filename} tmpfiles/{filename}"))
  
  d <- import(glue("tmpfiles/{filename}"))
  m <- as.matrix(d)
  max <- which(m == max(m), arr.ind = TRUE)
  return(c(max[1, 1], max[1, 2]))
}

if (args[3] == "1") pull_and_convert("2019-1", "2020-6")
max <- find_most_connected(args[1], args[2])
print(max)
