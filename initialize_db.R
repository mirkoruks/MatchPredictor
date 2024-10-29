# creates fbref database with SQLite

rm(list = ls())
library(DBI)
library(RSQLite)
library(tidyverse)

source("sql_helpers.R")

# Create database
con <- dbConnect(SQLite(), "fbref.db")

#field_data <- field_data %>% # add to scraping!!
#  distinct()

str(field_data)
dbListTables(con)

shot_data <- read_csv("data/shot_data_backup.csv")
event_data <- read_csv("data/event_data_backup.csv")
field_data <- read_csv("data/field_data_backup.csv")
keeper_data <- read_csv("data/keeper_data_backup.csv")
fixtures <- read_csv("data/fixtures_backup.csv") %>% 
  mutate(date = as.character(date),
         time = as.character(time))


#create_seasons_table(con)  # , hard = TRUE)
# dbAppendTable(conn = con,
#               name = "seasons",
#               value = seasons)

#create_competitions_table(con)  # , hard = TRUE)
# dbAppendTable(conn = con,
#               name = "competitions",
#               value = competitions)

create_fixtures_table(con, hard = TRUE)
dbAppendTable(conn = con,
              name = "fixtures",
              value = fixtures)

create_shot_data_table(con, hard = TRUE)
dbAppendTable(conn = con,
              name = "shot_data",
              value = shot_data)

create_event_data_table(con, hard = TRUE)
dbAppendTable(conn = con,
              name = "event_data",
              value = event_data)

create_field_data_table(con, hard = TRUE)
dbAppendTable(conn = con,
              name = "field_data",
              value = field_data)

create_keeper_data_table(con, hard = TRUE)
dbAppendTable(conn = con,
              name = "keeper_data",
              value = keeper_data)
