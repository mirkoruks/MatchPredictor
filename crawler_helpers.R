# Create basic URL ----
create_basic_url <- function() {
  basic_url <- "https://fbref.com"
  return(basic_url)
}

# Get competition data ----
get_comp_data <- function(basic_url, con) {
  overall_comps_url <- paste0(basic_url,"/en/comps/")
  
  cat("Reading competition data...\n")
  Sys.sleep(6)
  start_page <- read_html(overall_comps_url) %>% 
    html_elements("#comps_club")
  
  comp_table <- start_page %>% 
    html_table()
  
  comp_links <- start_page %>% 
    html_element("tbody") %>% 
    html_elements("tr") %>% 
    html_elements("th") %>% 
    html_elements("a") %>% 
    html_attr("href")
  
  comp_table <- bind_cols(comp_table, links = paste0(basic_url,comp_links)) %>% 
    filter(row_number() < 6) %>% 
    mutate(comp_id = str_remove(string = links, pattern = ".*comps/"),
           comp_id = as.numeric(str_remove(string = comp_id, pattern = "/history.*")),
           Last_Season_Start = as.numeric(str_remove_all(string = `Last Season`, pattern = "-.*")),
           Last_Season_End = as.numeric(str_remove_all(string = `Last Season`, pattern = ".*-")))
  colnames(comp_table) <- str_to_lower(colnames(comp_table))
  colnames(comp_table) <- str_replace_all(colnames(comp_table), " ", "_")
  colnames(comp_table) <- str_replace_all(colnames(comp_table), "\\.", "_")
  
  create_competitions_table(con , hard = TRUE)
  dbAppendTable(conn = con,
                name = "competitions",
                value = comp_table)
  
  return(comp_table)
  cat("Done!")
}

# Get seasons data ----
get_seasons_data <- function(df_comp, con) {
  
  ind_comp_list <- list()
  for (i in 1:nrow(df_comp)) {
    Sys.sleep(1)
    ind_comp_name <- df_comp %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(competition_name)
    
    ind_comp_id <- df_comp %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(comp_id)
    
    if ("seasons" %in% dbListTables(con)) {
      last_existing_season <- df_comp %>% 
        filter(row_number() == as.numeric(i)) %>% 
        pull(last_season_start)
      
      df_season <- dbGetQuery(con, glue("SELECT * FROM seasons WHERE comp_id = {ind_comp_id}"))
      
      if (nrow(df_season) > 0) { 
        last_recorded_season <- df_season %>% 
          summarize(max(season_start)) %>% 
          pull()
        
        if (last_recorded_season == last_existing_season) {
          cat("Season data for",ind_comp_name,"is already up to date.\n")
          read_season <- FALSE
        } else {
          read_season <- TRUE
        }
      } else {
        read_season <- TRUE
      }
    } else {
      read_season <- TRUE
    }
    
    if (read_season == TRUE) {
      cat("Read seasons data for:",ind_comp_name,"\n")
      
      ind_comp_link <- df_comp %>% 
        filter(row_number() == as.numeric(i)) %>% 
        pull(links)
      
      Sys.sleep(6)
      ind_comp_page <- read_html(ind_comp_link) %>% 
        html_elements("#seasons")
      
      ind_comp_table <- ind_comp_page %>% 
        html_table() %>% 
        as.data.frame() %>% 
        mutate(season_start = as.numeric(str_remove_all(string = Season, pattern = "-.*")),
               season_end = as.numeric(str_remove_all(string = Season, pattern = ".*-"))) %>% 
        filter(season_start >= 2017) %>% 
        arrange(season_start) %>% 
        select(-c(X..Squads, Champion, Top.Scorer))
      
      ind_comp_links <- ind_comp_link %>% 
        str_remove("history.*") 
      ind_comp_links <- str_glue("{part1}{part2}/schedule/{part2}-{part3}-Scores-and-Fixtures",
                                 part1 = ind_comp_links,
                                 part2 = ind_comp_table$Season,
                                 part3 = str_replace_all(ind_comp_name, pattern = " ", replacement = "-"))
      
      ind_comp_table <- bind_cols(ind_comp_table, links = ind_comp_links, comp_id = ind_comp_id) 
      colnames(ind_comp_table) <- str_to_lower(colnames(ind_comp_table))
      colnames(ind_comp_table) <- str_replace_all(colnames(ind_comp_table), " ", "_")
      colnames(ind_comp_table) <- str_replace_all(colnames(ind_comp_table), "\\.", "_")
      ind_comp_list[[i]] <- ind_comp_table
    }
    
  }
  
  if (length(ind_comp_list) > 0) {
    
    seasons_table <- bind_rows(ind_comp_list) %>% 
      arrange(comp_id)
    
    create_seasons_table(con , hard = TRUE)
    dbAppendTable(conn = con,
                  name = "seasons",
                  value = seasons_table)
    
    return(seasons_table)
    cat("Done!")
  } else {
    seasons_table <- dbGetQuery(con = con, "SELECT * FROM seasons")
    return(seasons_table)
    cat("Done!")
  }
  
  
}

# Get fixtures data ----

## Helper functions ----
### Get elo data ----

scrape_elo <- function(club) {
  #Sys.sleep(1)
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

get_elo_data <- function(fixtures) {
  
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
    inner_join(teams_fbref, by = "elo_team") %>% 
    select(Elo, fbref_team, elo_team, To) %>% 
    rename(date = To)
  
  
  fixtures_elo_data <- fixtures %>% 
    mutate(date = as_date(date)) %>% 
    pivot_longer(cols = c(home, away),
                 names_to = "home",
                 values_to = "fbref_team") %>% 
    left_join(teams_elo_df, by = c("date","fbref_team")) %>% 
    mutate(Elo = case_when(match_report != "Match Report" ~ NA,
                           TRUE ~ Elo)) %>% 
    arrange(season, wk) %>% 
    group_by(season, fbref_team) %>% 
    mutate(last_elo = last(Elo, na_rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(Elo = case_when(match_report != "Match Report" ~ last_elo,
                           TRUE ~ Elo)) %>% 
    select(-last_elo) %>% 
    rename(team = fbref_team) %>% 
    pivot_wider(names_from = home,
                values_from = c(team, elo_team, Elo)) %>% 
    mutate(Elodiff_home = 100 + Elo_home - Elo_away, # 100 = home advantage
           We_home = 1/(10^(-Elodiff_home/400)+1),
           We_away = 1-We_home) %>% 
    select(match_id, Elo_home, Elo_away, Elodiff_home, We_home, We_away)
  
  
  fixtures_return <- fixtures %>% 
    inner_join(fixtures_elo_data, by = "match_id")
  
  return(fixtures_return)
  
}

get_fixtures_data <- function(df_seasons, con) {
  
  fixtures_list <- list()
  
  
  iterator <- 1:nrow(df_seasons)
  
  for (i in iterator) {
    current_season <- df_seasons %>% 
      filter(row_number() == i) %>% 
      pull(season)
    
    id <- df_seasons %>% 
      filter(row_number() == i) %>% 
      pull(comp_id)
    
    season_link <- df_seasons %>% 
      filter(row_number() == i) %>% 
      pull(links)
    
    comp_name <- df_seasons %>% 
      filter(row_number() == i) %>% 
      pull(competition_name) %>% 
      str_replace_all(" ", "-")
    
    
    match_scraping <- FALSE
    match_tuning <- FALSE
    
    if ("fixtures" %in% dbListTables(con)) {
      df_fixtures <- dbGetQuery(con, paste0("SELECT * FROM fixtures WHERE comp_id = ",id," AND season = '",current_season,"'"))
      #df_fixtures <- read_delim("data/fixtures.csv", show_col_types = FALSE) %>% 
      #  filter(comp_id == id & season == current_season) 
      
      if (nrow(df_fixtures) > 0) { 
        df_fixtures_uncomplete <- df_fixtures %>% 
          filter(season == current_season & comp_id == id & match_report != "Match Report")
        
        if (nrow(df_fixtures_uncomplete) == 0) {
          cat("Fixtures data for ", comp_name ," (Season ",current_season,") is already complete\n", sep = "")
          read_fixtures <- FALSE
          Sys.sleep(0.2)
        } else {
          cat("Fixtures data for ", comp_name ," (Season ",current_season,") are incomplete\n", sep = "")
          df_fixtures_scraped <- df_fixtures %>% 
            select(match_id, scraped)
          match_scraping <- TRUE
          
          df_fixtures_tuning <- df_fixtures %>% 
            select(match_id, tuning)
          match_tuning <- TRUE
          
          read_fixtures <- TRUE
          Sys.sleep(1)
        }
      } else {
        read_fixtures <- TRUE
      }
    } else {
      read_fixtures <- TRUE
    }
    
    
    if (read_fixtures == TRUE) {
      table_id <- paste0("sched_",current_season,"_",id,"_1")
      
      cat("Read data for: ",str_replace(comp_name, "-", " ")," (Season ",current_season,")\n", sep = "")
      Sys.sleep(6)
      fixtures_page <- map(season_link,
                           possibly(.f = read_html, otherwise = NULL, quiet = TRUE))[[1]]
      
      if (is.null(fixtures_page)) {
        cat("Something did not work. I will wait some more time and try again. Maximum tries = 20.\n")
        counter = 1
        
        while (is.null(fixtures_page) & counter <= 20) {
          cat("It did not work. Take some more time and try again...\n")
          Sys.sleep(180)
          fixtures_page <- map(season_link,
                               possibly(.f = read_html, otherwise = NULL, quiet = TRUE))
          counter <- counter + 1
        }
        
        if (counter > 20 & is.null(fixtures_page)) {
          cat("After 20 tries it still did not work... I will skip ",comp_name," (Season ",current_season,") for now and try later again.\n NOTE: THIS IS WORK IN PROGRESS. STOPPING HERE!", sep = "")
          skipped_seasons <- c(skipped_seasons, i)
          stop()
        }
        
        
      }
      
      fixtures_page <- fixtures_page %>% 
        html_elements(paste0("#",table_id))
      
      fixtures_table <- fixtures_page %>% 
        html_table() %>% 
        as.data.frame() %>% 
        filter(!is.na(Wk)) %>% 
        mutate(comp_id = id,
               Date = as_date(Date),
               Season = current_season,
               Attendance = as.numeric(str_replace_all(Attendance, ",", "")),
               goals_home = as.numeric(str_remove(Score, "–.*")),
               goals_away = as.numeric(str_remove(Score, ".*–")),
               points_home = case_when(goals_home > goals_away ~ 3,
                                       goals_home == goals_away ~ 1,
                                       goals_home < goals_away ~ 0,
                                       TRUE ~ NA),
               points_away = case_when(goals_home < goals_away ~ 3,
                                       goals_home == goals_away ~ 1,
                                       goals_home > goals_away ~ 0,
                                       TRUE ~ NA)) %>% 
        rename(xG_home = xG,
               xG_away = xG.1,
               Match_Report = Match.Report)
      
      match_links <- fixtures_page %>% 
        html_element("tbody") %>% 
        html_elements("tr") %>% 
        html_elements("td[data-stat='match_report']") %>% 
        html_elements("a") %>% 
        html_attr("href") 
      
      past_match_links <- match_links %>% 
        str_subset(".*/matches/.*") %>%
        str_unique()
      
      coming_match_links <- match_links %>% 
        str_subset(".*/matchup/.*") %>% 
        str_unique()
      
      link_home <- fixtures_page %>% 
        html_element("tbody") %>% 
        html_elements("tr") %>% 
        html_elements("td[data-stat='home_team']") %>% 
        html_elements("a") %>% 
        html_attr("href") 
      
      link_away <- fixtures_page %>% 
        html_element("tbody") %>% 
        html_elements("tr") %>% 
        html_elements("td[data-stat='away_team']") %>% 
        html_elements("a") %>% 
        html_attr("href") 
      
      home_id <- link_home %>% 
        str_remove_all(".*squads/") %>% 
        str_remove_all("/.*")
      away_id <- link_away %>% 
        str_remove_all(".*squads/") %>% 
        str_remove_all("/.*")
      
      fixtures_table <- fixtures_table %>% 
        bind_cols(link_home = link_home,
                  link_away = link_away,
                  home_id = home_id,
                  away_id = away_id) %>% 
        mutate(match_id = paste0(comp_id, Date, home_id, away_id))
      
      past_matches <- fixtures_table %>% 
        filter(Match_Report == "Match Report")
      
      coming_matches <- fixtures_table %>% 
        filter(Match_Report != "Match Report")
      
      if (nrow(past_matches) == 0) {
        cat("No past matches for: ",str_replace(comp_name, "-", " ")," (Season ",current_season,")\n", sep = "")
        final_fixtures_table <- bind_cols(bind_rows(past_matches, coming_matches), link_match = c(rep(NA, nrow(past_matches)),rep(NA, nrow(coming_matches)))) 
      } else {
        final_fixtures_table <- bind_cols(bind_rows(past_matches, coming_matches), link_match = c(paste0(basic_url, past_match_links),rep(NA, nrow(coming_matches))))
        
        
      }
      colnames(final_fixtures_table) <- str_to_lower(colnames(final_fixtures_table))
      colnames(final_fixtures_table) <- str_replace_all(colnames(final_fixtures_table), " ", "_")
      colnames(final_fixtures_table) <- str_replace_all(colnames(final_fixtures_table), "\\.", "_")
      
      if (match_scraping == TRUE) {
        final_fixtures_table <- final_fixtures_table %>% 
          full_join(df_fixtures_scraped, by = "match_id")
      } else {
        final_fixtures_table <- final_fixtures_table %>% 
          mutate(scraped = 0)
      }
      
      if (match_tuning == TRUE) {
        final_fixtures_table <- final_fixtures_table %>% 
          full_join(df_fixtures_tuning, by = "match_id")
      } else {
        final_fixtures_table <- final_fixtures_table %>% 
          mutate(tuning = 0)
      }
      
      
      fixtures_list[[i]] <- final_fixtures_table
    }
  }
  
  if (length(fixtures_list) > 0) {
    fixtures_table <- bind_rows(fixtures_list) %>% 
      arrange(comp_id) %>% 
      mutate(date = as.character(date),
             time = as.character(time)) 
    
    fixtures_table_elo <- get_elo_data(fixtures_table)
    
    if ("fixtures" %in% dbListTables(con)) {
      final_table <- dbGetQuery(con, "SELECT * FROM fixtures") %>% 
        rows_upsert(fixtures_table_elo, by = "match_id") %>% 
        arrange(comp_id)
      
      create_fixtures_table(con , hard = TRUE)
      dbAppendTable(conn = con,
                    name = "fixtures",
                    value = final_table)
    } else {
      final_table <- fixtures_table_elo
      create_fixtures_table(con , hard = TRUE)
      dbAppendTable(conn = con,
                    name = "fixtures",
                    value = final_table)
    }
  } else {
    final_table <- dbGetQuery(con, "SELECT * FROM fixtures")
    
  }
  return(final_table)
  
  
}



# Get match data ----

## Helper functions ----
rename_match_table <- function(df, string_to_split, drop_last = TRUE, shots = FALSE, keeper = FALSE) {
  new_column_names <- paste0(colnames(df),"_",df[1,])
  new_column_names <- new_column_names %>% 
    str_replace_all(pattern = " ", replacement = "_") %>% 
    str_replace_all(pattern = "%", replacement = "pct") %>% 
    str_replace_all(pattern = "1/3", replacement = "final3rd") %>% 
    str_replace_all(pattern = "^_", replacement = "") %>% 
    str_replace_all(pattern = "#", replacement = "number") %>% 
    str_replace_all(pattern = "\\+", replacement = "_") %>% 
    str_replace_all(pattern = "\\(", replacement = "") %>%
    str_replace_all(pattern = "\\)", replacement = "") %>%
    str_replace_all(pattern = "\\-", replacement = "_") %>%    
    str_to_lower()
  if (shots == FALSE & keeper == FALSE) {
    prefix <- str_split_i(string = string_to_split, pattern = "_", i = 3)
    new_column_names[7:length(new_column_names)] <- paste0(prefix,"_",new_column_names[7:length(new_column_names)])
  } else if(shots == TRUE) {
    prefix <- "shots"
    new_column_names <- paste0(prefix,"_",new_column_names)
  } else if (keeper == TRUE) {
    prefix <- "keeper"
    new_column_names[5:length(new_column_names)] <- paste0(prefix,"_",new_column_names[5:length(new_column_names)])
  }
  
  
  colnames(df) <- new_column_names
  if (drop_last == TRUE) {
    df <- df %>% 
      filter(row_number() > 1 & row_number() < nrow(df)) %>% 
      suppressMessages(type_convert())
  } else {
    df <- df %>% 
      filter(row_number() > 1) %>% 
      suppressMessages(type_convert())
  }
  
  return(df)
  
}

get_match_data <- function(df_seasons, df_fixtures, con) {
  
  for (i in 1:nrow(df_seasons)) {
    Sys.sleep(0.2)
    comp_name <- df_seasons %>% 
      filter(row_number() == i) %>% 
      pull(competition_name)
    
    current_season <- df_seasons %>% 
      filter(row_number() == i) %>% 
      pull(season)
    
    id <- df_seasons %>% 
      filter(row_number() == i) %>% 
      pull(comp_id)
    cat("\n---------------------------------------------------
Reading data for ", comp_name, " (Season ", current_season,")
---------------------------------------------------\n", sep = "")
    fixtures_season <- df_fixtures %>% 
      filter(season == current_season & comp_id == id & match_report == "Match Report" & scraped == 0 & notes == "") %>%    
      dplyr::select(wk, date, home, away, home_id, away_id, match_id, link_match)
    
    if (nrow(fixtures_season) == 0) {
      cat("Match data for ", comp_name ," (Season ",current_season,") is already complete\n", sep = "")
    } else {
      
      for (j in 1:nrow(fixtures_season)) {
        
        match_week <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(wk)
        
        home_name <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(home)
        
        away_name <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(away)
        
        id_away <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(away_id)
        
        id_home <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(home_id)
        
        match_date <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(date)
        
        match_link <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(link_match)
        
        match_id <- fixtures_season %>% 
          filter(row_number() == j) %>% 
          pull(match_id)
        
        cat("Read data for match: ",home_name," vs. ", away_name, " (Match ID: ",match_id, ", Match Week ",match_week,", ",as.character(match_date),")\n", sep = "")
        Sys.sleep(6)
        match_page <- map(match_link,
                          possibly(.f = read_html, otherwise = NULL, quiet = TRUE))[[1]]
        
        if (is.null(match_page)) {
          cat("Something did not work. I will wait some more time and try again. Maximum tries = 20.\n")
          counter = 1
          
          while (is.null(match_page) & counter <= 20) {
            cat("It did not work. Take some more time and try again...\n")
            Sys.sleep(180)
            match_page <- map(match_link,
                              possibly(.f = read_html, otherwise = NULL, quiet = TRUE))[[1]]
            counter <- counter + 1
          }
          
          if (counter > 20 & is.null(match_page)) {
            cat("After 20 tries it still did not work... I will skip match ",home_name," vs. ", away_name, " (Match Week ",match_week,", ",as.character(match_date),") for now and try later again.\n", sep = "")
            skipped_matches <- c(skipped_matches, i)
            next()
          }
          
          
        }
        
        
        event_time <- match_page %>%
          html_elements("#events_wrap") %>% 
          html_elements(".event") %>% 
          html_element("div") %>% 
          html_text2() %>% 
          str_remove("&.*\n.*") %>% 
          str_trim()
        
        event_score <- match_page %>%
          html_elements("#events_wrap") %>% 
          html_elements(".event") %>% 
          html_element("div") %>% 
          html_text2() %>% 
          str_remove(".*\n") %>% 
          str_trim() 
        
        event_score_away <- event_score %>% 
          str_remove(".*:")
        
        event_score_home <- event_score %>% 
          str_remove(":.*")
        
        event_player <- match_page %>%
          html_elements("#events_wrap") %>% 
          html_elements(".event") %>% 
          html_elements("div:nth-child(2)") %>%
          html_elements("div:nth-child(2)") 
        
        event_player_first <- event_player %>% 
          html_elements("div") %>% 
          html_text2() 
        
        event_player_first_id <- event_player %>% 
          html_elements("div") %>% 
          html_element("a") %>% 
          html_attr("href") %>% 
          str_remove(".*players/") %>% 
          str_remove("/.*")
        
        event_player_second <- event_player %>% 
          html_element("small") %>% 
          html_text2() %>% 
          str_remove(".*:") %>% 
          str_remove("^for ") %>% 
          str_trim()
        
        event_player_second_id <- event_player %>% 
          html_element("small") %>% 
          html_element("a") %>% 
          html_attr("href") %>% 
          str_remove(".*players/") %>% 
          str_remove("/.*")
        
        team_logo_link <- match_page %>%
          html_elements("#events_wrap") %>% 
          html_elements(".event") %>% 
          html_elements("div") %>% 
          html_elements("span") %>% 
          html_elements(".teamlogo") %>% 
          html_attr("src") 
        
        team_id <- team_logo_link %>% 
          str_remove(".png") %>% 
          str_remove(".*fb/")
        
        event_type <- match_page %>%
          html_elements("#events_wrap") %>%
          html_elements(".event") %>%
          html_children() %>%
          html_elements("div") %>%
          html_attr("class") %>%
          str_subset("^event") %>% 
          str_remove(".* ") %>% 
          str_replace("substitute_in","substitute")
        
        event_df <- tibble(event_time = event_time,
                           event_type = event_type,
                           event_score = event_score,
                           event_score_home = as.numeric(event_score_home),
                           event_score_away = as.numeric(event_score_away),
                           event_player_first = event_player_first,
                           event_player_second = event_player_second,
                           event_player_first_id = event_player_first_id,
                           event_player_second_id = event_player_second_id,
                           team_logo_link = team_logo_link,
                           team_id = team_id)
        
        shot_table <- match_page %>% 
          html_elements("#shots_all") %>% 
          html_table()
        shot_table <- rename_match_table(shot_table[[1]], drop_last = FALSE, shots = TRUE) 
        
        shot_player_links <- match_page %>% 
          html_elements("#shots_all") %>%
          html_element("tbody") %>% 
          html_elements("tr") %>% 
          html_elements("td[data-stat='player']") %>% 
          html_element("a") %>%
          html_attr("href") 
        
        shot_player_links <- str_glue("{basic_url}{player_links}",
                                      basic_url = basic_url,
                                      player_links = shot_player_links)
        
        shot_player_id <- shot_player_links %>% 
          str_replace_all(pattern = ".*players/", replacement = "") %>% 
          str_replace_all(pattern = "/.*", replacement = "")
        
        
        shot_team_links <- match_page %>% 
          html_elements("#shots_all") %>%
          html_element("tbody") %>% 
          html_elements("tr") %>% 
          html_elements("td[data-stat='team']") %>% 
          html_element("a") %>%
          html_attr("href") 
        
        shot_team_links <- str_glue("{basic_url}{player_links}",
                                    basic_url = basic_url,
                                    player_links = shot_team_links)
        
        shot_team_id <- shot_team_links %>% 
          str_replace_all(pattern = ".*squads/", replacement = "") %>% 
          str_replace_all(pattern = "/.*", replacement = "")
        
        shot_sca1_links <- match_page %>% 
          html_elements("#shots_all") %>%
          html_element("tbody") %>% 
          html_elements("tr") %>% 
          html_elements("td[data-stat='sca_1_player']") %>% 
          html_element("a") %>%
          html_attr("href") 
        
        shot_sca1_links <- str_glue("{basic_url}{player_links}",
                                    basic_url = basic_url,
                                    player_links = shot_sca1_links)
        
        shot_sca1_id <- shot_sca1_links %>% 
          str_replace_all(pattern = ".*players/", replacement = "") %>% 
          str_replace_all(pattern = "/.*", replacement = "")
        
        shot_sca2_links <- match_page %>% 
          html_elements("#shots_all") %>%
          html_element("tbody") %>% 
          html_elements("tr") %>% 
          html_elements("td[data-stat='sca_2_player']") %>% 
          html_element("a") %>%
          html_attr("href") 
        
        shot_sca2_links <- str_glue("{basic_url}{player_links}",
                                    basic_url = basic_url,
                                    player_links = shot_sca2_links)
        
        shot_sca2_id <- shot_sca2_links %>% 
          str_replace_all(pattern = ".*players/", replacement = "") %>% 
          str_replace_all(pattern = "/.*", replacement = "")
        
        shot_table <- shot_table %>% 
          bind_cols(shot_player_links = shot_player_links,
                    shot_player_id = shot_player_id,
                    shot_squad_links = shot_team_links,
                    shot_squad_id = shot_team_id,
                    shot_sca1_links = shot_sca1_links,
                    shot_sca1_id = shot_sca1_id,
                    shot_sca2_links = shot_sca2_links,
                    shot_sca2_id = shot_sca2_id) %>% 
          filter(shots_minute != "")
        
        # match data tables:
        field_match_data_list <- list()
        keeper_match_data_list <- list()
        
        for (team_id in c(id_home, id_away)) {
          field_match_data2 <- list()
          for (type in c(paste0("stats_",team_id,"_summary"),
                         paste0("stats_",team_id,"_passing"),
                         paste0("stats_",team_id,"_passing_types"),
                         paste0("stats_",team_id,"_defense"),
                         paste0("stats_",team_id,"_possession"),
                         paste0("stats_",team_id,"_misc"))) {
            team_data <- match_page %>% 
              html_elements(paste0("#",type)) %>% 
              html_table()
            team_data[[1]] <- rename_match_table(team_data[[1]], string_to_split = type)
            team_data_df <- team_data[[1]] %>% 
              bind_rows()
            
            if (!(str_detect(type, ".*summary$"))) {
              
              team_data_df <- team_data_df %>% 
                dplyr::select(-c(1:6))
              
            } else {
              
              player_links <- match_page %>% 
                html_elements(paste0("#",type)) %>%
                html_element("tbody") %>% 
                html_elements("tr") %>% 
                html_elements("th[data-stat='player']") %>% 
                html_elements("a") %>% 
                html_attr("href") 
              
              player_links <- str_glue("{basic_url}{player_links}",
                                       basic_url = basic_url,
                                       player_links = player_links)
              
              player_id <- player_links %>% 
                str_replace_all(pattern = ".*players/", replacement = "") %>% 
                str_replace_all(pattern = "/.*", replacement = "")
              
              team_data_df <- bind_cols(player_id = player_id, link_player = player_links, team_data_df)
              
            }
            field_match_data2[[type]] <- team_data_df
            
          }

          field_match_data_list[[team_id]] <- bind_cols(field_match_data2)

          keeper_table <- match_page %>% 
            html_elements(paste0("#",paste0("keeper_stats_",team_id))) %>% 
            html_table()
          keeper_table[[1]] <- rename_match_table(keeper_table[[1]], drop_last = FALSE, keeper = TRUE)
          keeper_data_df <- keeper_table[[1]] %>% 
            bind_rows()
          
          
          player_links <- match_page %>% 
            html_elements(paste0("#",paste0("keeper_stats_",team_id))) %>%
            html_element("tbody") %>% 
            html_elements("tr") %>% 
            html_elements("th[data-stat='player']") %>% 
            html_elements("a") %>% 
            html_attr("href") 
          
          player_links <- str_glue("{basic_url}{player_links}",
                                   basic_url = basic_url,
                                   player_links = player_links)
          
          player_id <- player_links %>% 
            str_replace_all(pattern = ".*players/", replacement = "") %>% 
            str_replace_all(pattern = "/.*", replacement = "")
          
          keeper_data_df <- keeper_data_df %>% 
            bind_cols(player_links = player_links,
                      player_id = player_id)
          
          keeper_match_data_list[[team_id]] <- keeper_data_df
          
        }
        field_match_data_df <- field_match_data_list %>% 
          bind_rows(.id = "team_id") %>% 
          mutate(comp_name = comp_name,
                 comp_id = id,
                 current_season = current_season,
                 match_id = match_id, 
                 match_week = match_week
          )
        
        keeper_match_data_df <- keeper_match_data_list %>% 
          bind_rows(.id = "team_id") %>% 
          mutate(comp_name = comp_name,
                 comp_id = id,
                 current_season = current_season,
                 match_id = match_id, 
                 match_week = match_week
          )
        
        event_df <- event_df %>% 
          mutate(comp_name = comp_name,
                 comp_id = id,
                 current_season = current_season,
                 match_id = match_id, 
                 match_week = match_week,
                 event_counter = row_number()
          )
        
        shot_table <- shot_table %>% 
          mutate(comp_name = comp_name,
                 comp_id = id,
                 current_season = current_season,
                 match_id = match_id, 
                 match_week = match_week,
                 shot_counter = row_number()
          )
        
        
        if ("event_data" %in% dbListTables(con)) {
          dbAppendTable(con, "event_data", event_df)
        } else {
          create_event_data_table(con , hard = TRUE)
          dbAppendTable(conn = con,
                        name = "event_data",
                        value = event_df)
        }
        
        if ("field_data" %in% dbListTables(con)) {
          dbAppendTable(con, "field_data", field_match_data_df)
        } else {
          create_field_data_table(con , hard = TRUE)
          dbAppendTable(conn = con,
                        name = "field_data",
                        value = field_match_data_df)
        }
        
        if ("keeper_data" %in% dbListTables(con)) {
          dbAppendTable(con, "keeper_data", keeper_match_data_df)
        } else {
          create_keeper_data_table(con , hard = TRUE)
          dbAppendTable(conn = con,
                        name = "keeper_data",
                        value = keeper_match_data_df)
        }
        
        if ("shot_data" %in% dbListTables(con)) {
          dbAppendTable(con, "shot_data", shot_table)
        } else {
          create_shot_data_table(con , hard = TRUE)
          dbAppendTable(conn = con,
                        name = "shot_data",
                        value = shot_table)
        }
        
        last_match_id <- match_id
        df_fixtures <- df_fixtures %>% 
          mutate(scraped = case_when(match_id == last_match_id ~ 1,
                                     TRUE ~ scraped))
        dbExecute(con, paste0("UPDATE fixtures SET scraped = 1 WHERE match_id = '",last_match_id,"'"))

      }
      
      
      
    }
    
  }
}

