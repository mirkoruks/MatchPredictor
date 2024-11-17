rm(list = ls())

library(tidymodels)
library(tidyverse)

df <- read_csv("data/modeldata.csv")

df <- df %>% 
  mutate(home_result = factor(home_result))
str(df)


# impute missing data

# normalize
# use 5 lag, 10 lag 15 lag