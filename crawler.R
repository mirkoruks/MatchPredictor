rm(list = ls())

suppressMessages(library(rvest))
suppressMessages(library(tidyverse))
suppressMessages(library(DBI))
suppressMessages(library(RSQLite))
suppressMessages(library(glue))
suppressMessages(library(httr2))

make_message <- function(string, first = FALSE) {
  if (length(string) != 1) {
    stop("Provide string of length 1.")
  } else {
    n <- str_count(string)
    line <- paste0(rep("-",n),collapse="")
    if (first == FALSE) {
      statement <- paste0("\n\n",line,"\n",string,"\n",line,"\n\n")
    } else {
      statement <- paste0(line,"\n",string,"\n",line,"\n\n")
    }
  }
  cat(statement)
}

# Start log file
logfilename <- paste0("log/logfile_",format(Sys.time(), "%Y%m%d%H%M%S%Z"),".txt")
sink(file = logfilename, split = TRUE, )

make_message(paste0("Start Log File Match Predictor SCRAPING (",format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"),")"), first = TRUE)

make_message("Get helper functions!")
source("crawler_helpers.R")
source("db_helpers.R")

make_message("Connect to database!")
con <- dbConnect(SQLite(), "fbref.db")

if (length(dbListTables(con)) == 0) {
  make_message("Database created - Start first run")
} else {
  make_message("Database already exists - Start update")
}

make_message("Get basic URL!")
basic_url <- create_basic_url()

# Get competitions data ----
make_message("Get competition data")
competitions_table <- get_comp_data(basic_url, con)

# Get seasons data ----
make_message("Get seasons data")
seasons_table <- get_seasons_data(competitions_table, con)

# Get image data ----
make_message("Get image data")
get_image_data(seasons_table)

# Create fixture data ----
make_message("Get fixtures data")
fixtures_table <- get_fixtures_data(seasons_table, con)

# Get match data ----
make_message("Get match data")
get_match_data(seasons_table, fixtures_table, con)

Sys.sleep(0.5)
make_message("Scraping finished")

Sys.sleep(0.5)
make_message("Disconnect from database")
dbDisconnect(con)

Sys.sleep(1)
make_message("See you later aligator")
Sys.sleep(1)
sink()

