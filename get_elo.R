# get elo ratings
rm(list = ls())
library(DBI)
library(RSQLite)
library(tidyverse)
library(httr2)

# calculate within each season manually applying rules from https://www.eloratings.net/about
con <- dbConnect(SQLite(), "fbref.db")
dbListTables(con)
dbListFields(con, "fixtures")


get_elo <- function(fixtures) {
  
  first_week_teams_fbref <- fixtures %>% 
    filter(wk == 1) %>% 
    mutate(date = as.character(date)) %>% 
    select(match_id, wk, date, home, away, comp_id, season)
  
  
  teams_fbref <- first_week_teams_fbref %>% 
    select(home, away) %>%
    pivot_longer(c(home, away), 
                 names_to = "type",
                 values_to = "fbref_team") %>% 
    select(fbref_team) %>% 
    distinct() %>% 
    mutate(elo_team = fbref_team) %>% 
    mutate(elo_team = case_when(fbref_team == "Leicester City" ~ "Leicester",
                                fbref_team == "CrystaPalace" ~ "Crystal Palace",
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
  scrape_elo <- function(club) {
    Sys.sleep(1)
    cat("Read ELO data for",club,"\n")
    req <- httr2::request(paste0("api.clubelo.com/",club))
    resp <- tryCatch(
      {httr2::req_perform(req)},
      error = function(x) {
        return(NULL)
      }
    )
    if (class(resp) == "httr2_response") {
      results <- resp %>% 
        resp_body_string() %>% 
        readr::read_csv(, show_col_types = FALSE)
      if (nrow(results) == 0) {
        results <- NULL
      }
    } else {
      results <- NULL
    }
    
    if(!is_tibble(results) & club != "Skip") {
      old_club <- club
      
      while(!is_tibble(results) & club != "Skip") {
        
        cat("I could not find any elo score for the following club:", club, "\nPlease give me the correct team name. You can compare with the names listet here: http://clubelo.com/.\nTo skip the club and continue with the next, type in 'Skip':\n\n")
        club <- readline()
        req <- httr2::request(paste0("api.clubelo.com/",club))
        req
        resp <- tryCatch(
          {httr2::req_perform(req)},
          error = function(x) {
            return(NULL)
          }
        )
        if (class(resp) == "httr2_response") {
          results <- resp %>% 
            resp_body_string() %>% 
            readr::read_csv(, show_col_types = FALSE)
          if (nrow(results) == 0) {
            results <- NULL
          } else {
            teams_fbref[teams_fbref$elo_team == old_club, "elo_team"] <<- club # access global variable , be careful! but that allows to correct the names on the fly
          }
        } else {
          results <- NULL
        }
      }
    }
    
    
    
    return(results)
    
  }

  teams_elo <- teams_fbref %>%
    select(elo_team) %>% 
    pull() %>% 
    map(scrape_elo)
  
  names(teams_elo) <- teams_fbref %>% # get corrected elo team names
    select(elo_team) %>% 
    pull() 
  
  teams_elo_check <- teams_elo %>% 
    map_lgl(is_tibble)
  
  if (FALSE %in% teams_elo_check) {
    stop(paste("Please check the team names. For the following team names, no elo score was found:", paste(names(teams_elo_check[teams_elo_check == FALSE]), collapse = ", ")))
  }
  
  
  teams_elo_df <- teams_elo %>% 
    bind_rows(.id = "elo_team") %>% 
    inner_join(teams_fbref) %>% 
    select(Elo, fbref_team, elo_team, To) %>% 
    rename(date = To)
  
  
  fixtures_elo_data <- fixtures %>% 
    mutate(date = as_date(date)) %>% 
    pivot_longer(cols = c(home, away),
                 names_to = "home",
                 values_to = "fbref_team") %>% 
    left_join(teams_elo_df) %>% 
    mutate(Elo = case_when(match_report == "Head-to-Head" ~ NA,
                           TRUE ~ Elo)) %>% 
    arrange(season, wk) %>% 
    group_by(season, fbref_team) %>% 
    mutate(last_elo = last(Elo, na_rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(Elo = case_when(match_report == "Head-to-Head" ~ last_elo,
                           TRUE ~ Elo)) %>% 
    select(-last_elo) %>% 
    rename(team = fbref_team) %>% 
    pivot_wider(names_from = home,
                values_from = c(team, elo_team, Elo)) %>% 
    mutate(Elodiff_home = 100 + Elo_home - Elo_away, # 100 = home advantage
           We_home = 1/(10^(-Elodiff_home/400)+1),
           We_away = 1-We_home)
  
  
  
  return(fixtures_elo_data)
  
}







