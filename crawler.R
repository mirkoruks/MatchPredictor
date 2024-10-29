rm(list = ls())

logfilename <- paste0("log/logfile_",str_replace(format(Sys.time(), "%Y%m%d%H%M%S%Z"), " ", "_"),".txt")
sink(file = logfilename, split = TRUE)

cat("Load libraries\n")
library(rvest)
library(tidyverse)
library(DBI)
library(RSQLite)
library(glue)
library(httr2)

cat("Get helper functions\n")
source("crawler_helpers.R")
source("db_helpers.R")


cat("Connect to database\n")
con <- dbConnect(SQLite(), "fbref.db")

if (length(dbListTables(con)) == 0) {
  cat("database created. start first run\n")
} else {
  cat("database already exists. start update\n")
}

cat("get basic url...\n")
basic_url <- create_basic_url()

# Get competitions data ----
cat("get competitions data...\n")
competitions_table <- get_comp_data(basic_url, con)

# Get seasons data ----
cat("get seasons data...\n")
seasons_table <- get_seasons_data(competitions_table, con)

# Create fixture data ----
cat("get fixtures data...\n")
fixtures_table <- get_fixtures_data(seasons_table, con)

# Get match data ----
cat("get match data...\n")
get_match_data(seasons_table, fixtures_table, con)

cat("\n\nfinished.")


cat("\n\n\ndisconnect from databse\n\n\n")
dbDisconnect(con)
sink()

