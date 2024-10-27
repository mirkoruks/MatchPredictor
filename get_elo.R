# add elo

# get elo values for 1. match week

library(DBI)
library(RSQLite)
library(tidyverse)
library(httr2)

# calculate within each season manually applying rules from https://www.eloratings.net/about
con <- dbConnect(SQLite(), "fbref.db")
dbListTables(con)
dbListFields(con, "fixtures")

first_week_teams_fbref <- dbGetQuery(con, "SELECT wk, date, home, away, comp_id, season FROM fixtures WHERE wk == 1")

teams_fbref <- first_week_teams_fbref %>% 
  select(home, away) %>%
  pivot_longer(c(home, away), 
               names_to = "type",
               values_to = "fbref_team") %>% 
  select(fbref_team) %>% 
  distinct() %>% 
  mutate(elo_team = fbref_team) %>% 
  mutate(elo_team = case_when(fbref_team == "Leicester City" ~ "Leicester",
                              fbref_team == "Crystal Palace" ~ "CrystalPalace",
                              fbref_team == "West Brom" ~ "WestBrom",
                              fbref_team == "Stoke City" ~ "Stoke",
                              fbref_team == "Swansea City" ~ "Swansea",
                              fbref_team == "Manchester City" ~ "ManCity",
                              fbref_team == "Newcastle Utd" ~ "Newcastle",
                              fbref_team == "Manchester Utd" ~ "ManUnited",
                              fbref_team == "West Ham" ~ "WestHam",
                              fbref_team == "Cardiff City" ~ "Cardiff",
                              fbref_team == "Norwich City" ~ "Norwich",
                              fbref_team == "Sheffield Utd" ~ "SheffieldUnited",
                              fbref_team == "Aston Villa" ~ "AstonVilla",
                              fbref_team == "Leeds United" ~ "Leeds",
                              fbref_team == "Nott'ham Forest" ~ "Forest",
                              fbref_team == "Luton Town" ~ "Luton",
                              fbref_team == "Ipswich Town" ~ "Ipswich",
                              fbref_team == "Hellas Verona" ~ "Verona",
                              fbref_team == "Leganés" ~ "Leganes",
                              fbref_team == "Alavés" ~ "Alaves",
                              fbref_team == "Las Palmas" ~ "LasPalmas",
                              fbref_team == "Celta Vigo" ~ "Celta",
                              fbref_team == "Real Sociedad" ~ "Sociedad",
                              fbref_team == "Atlético Madrid" ~ "Atletico",
                              fbref_team == "Athletic Club" ~ "Bilbao",
                              fbref_team == "La Coruña" ~ "Depor",
                              fbref_team == "Real Madrid" ~ "RealMadrid",
                              fbref_team == "Málaga" ~ "Malaga",
                              fbref_team == "Rayo Vallecano" ~ "RayoVallecano",
                              fbref_team == "Cádiz" ~ "Cadiz",
                              fbref_team == "Almería" ~ "Almeria",
                              fbref_team == "Paris S-G" ~ "ParisSG",
                              fbref_team == "Saint-Étienne" ~ "Saint-Etienne",
                              fbref_team == "Nîmes" ~ "Nimes",
                              fbref_team == "Clermont Foot" ~ "Clermont",
                              fbref_team == "Le Havre" ~ "LeHavre",
                              fbref_team == "Bayern Munich" ~ "Bayern",
                              fbref_team == "Hertha BSC" ~ "Hertha",
                              fbref_team == "Mainz 05" ~ "Mainz",
                              fbref_team == "Hannover 96" ~ "Hannover",
                              fbref_team == "Werder Bremen" ~ "Werder",
                              fbref_team == "Hamburger SV" ~ "Hamburg",
                              fbref_team == "Schalke 04" ~ "Schalke",
                              fbref_team == "RB Leipzig" ~ "RBLeipzig",
                              fbref_team == "Eint Frankfurt" ~ "Frankfurt",
                              fbref_team == "Köln" ~ "Koeln",
                              fbref_team == "Düsseldorf" ~ "Duesseldorf",
                              fbref_team == "Nürnberg" ~ "Nuernberg",
                              fbref_team == "Paderborn 07" ~ "Paderborn",
                              fbref_team == "Union Berlin" ~ "UnionBerlin",
                              fbref_team == "Arminia" ~ "Bielefeld",
                              fbref_team == "Greuther Fürth" ~ "Fuerth",
                              fbref_team == "Darmstadt 98" ~ "Darmstadt",
                              fbref_team == "Holstein Kiel" ~ "Holstein",
                              fbref_team == "St. Pauli" ~ "StPauli",
                              TRUE ~ elo_team))

# get club elo data
get_elo <- function(club) {
  Sys.sleep(3)
  cat("Read ELO data for",club)
  req <- httr2::request(paste0("api.clubelo.com/",club))
  req
  resp <- possibly(.f = httr2::req_perform(req), otherwise = "Error")
  if (resp == "Error") {
    
  }
  resp
  
  
  results <- resp %>% 
    resp_body_string() %>% 
    readr::read_csv(, show_col_types = FALSE)
  return(results)
}

teams_elo <- teams_fbref %>% 
  pull() %>% 
  set_names() %>% 
  map(possibly(.f = get_elo, otherwise = "Error"))


# if there is an error, let user know to check elo team names?
check_elo <- function(team_elo) {
  if(is_tibble(team_elo)) {
    if(nrow(team_elo > 0)) {
      result <- "Check"
    } else {
      result <- "Error"
    }
  } else {
    result <- "Error"
  }
}
teams_elo_result <- teams_elo %>% 
  map(check_elo) %>% 
  as_tibble() %>% 
  pivot_longer(everything(), 
               names_to = "fbref_team",
               values_to = "result")


teams_elo_result <- teams_elo_result %>% 
  mutate(elo_name = case_when(result == "Check" ~ fbref_team,
                              TRUE ~ NA))
teams_elo_result <- teams_elo_result %>% 
  
  
  teams_elo2 <- teams_elo_result %>% 
  filter(result == "Error") %>% 
  select(elo_name) %>% 
  pull() %>% 
  set_names() %>% 
  map(possibly(.f = get_elo, otherwise = "Error"))

teams_elo_result2 <- teams_elo2 %>% 
  map(check_elo) %>% 
  as_tibble() %>% 
  pivot_longer(everything(), 
               names_to = "fbref_team",
               values_to = "result")
