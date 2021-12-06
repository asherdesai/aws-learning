# Matrix

library(tidyverse)

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