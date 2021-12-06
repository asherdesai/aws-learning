# Data

library(rio)
library(glue)

get_data_by_month <- function(month_str) {
  filename <- glue("yellow_tripdata_{month_str}.csv")
  d <- import(glue("../data/{filename}"))
}