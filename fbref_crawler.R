rm(list = ls())

library(rvest)
library(tidyverse)

create_basic_url <- function() {
  basic_url <- "https://fbref.com"
  return(basic_url)
}
basic_url <- create_basic_url()

# Create data directory ----
check_data_dir <- function() {
  data_dir <- "data"
  if (!dir.exists(data_dir)) {
    dir.create(data_dir)
  }
}

# Read competitions data ----
get_comp_data <- function() {
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
  
  write_csv(comp_table, "data/competitions.csv")
  cat("Done!")
}
get_comp_data()

# Read season data ----
get_seasons_data <- function() {
  if (file.exists("data/competitions.csv")) {
    cat("Reading competitions.csv\n")
    df_comp <- read_delim("data/competitions.csv", show_col_types = FALSE)
  } else {
    cat("Downloading competitions data\n")
    get_comp_data()
    df_comp <- read_delim("data/competitions.csv", show_col_types = FALSE)
  }
  
  ind_comp_list <- list()
  for (i in 1:nrow(df_comp)) {
    Sys.sleep(1)
    ind_comp_name <- df_comp %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(competition_name)
    
    ind_comp_id <- df_comp %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(comp_id)
    
    if (file.exists("data/seasons.csv")) {
      last_existing_season <- df_comp %>% 
        filter(row_number() == as.numeric(i)) %>% 
        pull(last_season_start)
      
      df_season <- read_delim("data/seasons.csv", show_col_types = FALSE) %>% 
        filter(comp_id == ind_comp_id) 
      
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
      cat("Read competition data for:",ind_comp_name,"\n")
      
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
        arrange(season_start)
      
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
    ind_comp_table <- bind_rows(ind_comp_list) %>% 
      arrange(comp_id)
    if (file.exists("data/seasons.csv")) {
      df_season <- read_delim("data/seasons.csv", show_col_types = FALSE) %>% 
        bind_rows(ind_comp_table) %>% 
        distinct() %>% 
        arrange(comp_id)
      write_csv(df_season, file = "data/seasons.csv") 
    } else {
      write_csv(ind_comp_table, file = "data/seasons.csv") 
    }
  }
}
get_seasons_data()

# Create fixture data ----

get_fixture_data <- function() {
  if (file.exists("data/seasons.csv")) {
    cat("Reading seasons.csv\n")
    df_seasons <- read_delim("data/seasons.csv", show_col_types = FALSE)
  } else {
    cat("Downloading competitions data\n")
    get_seasons_data()
    df_seasons <- read_delim("data/seasons.csv", show_col_types = FALSE)
  }
  
  fixtures_list <- list()
  skipped_seasons <- c()
  fixtures_counter <- 0
  first_run <- TRUE 
  
  while(length(skipped_seasons) > 0 | first_run == TRUE) {
    
    if (length(skipped_seasons) == 0) {
      iterator <- 1:nrow(df_seasons)
    } else {
      iterator <- skipped_seasons
    }
    
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
      
      
      if (file.exists("data/fixtures.csv")) {
        df_fixtures <- read_delim("data/fixtures.csv", show_col_types = FALSE) %>% 
          filter(comp_id == id & season == current_season) 
        
        if (nrow(df_fixtures) > 0) { 
          df_fixtures_uncomplete <- df_fixtures %>% 
            filter(season == current_season & comp_id == id & match_report != "Match Report")
          
          if (nrow(df_fixtures_uncomplete) == 0) {
            cat("Fixtures data for ", comp_name ," (Season ",current_season,") is already complete\n", sep = "")
            read_fixtures <- FALSE
            Sys.sleep(1)
          } else {
            cat("Fixtures data for ", comp_name ," (Season ",current_season,") are incomplete\n", sep = "")
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
            cat("After 20 tries it still did not work... I will skip ",comp_name," (Season ",current_season,") for now and try later again.\n", sep = "")
            skipped_seasons <- c(skipped_seasons, i)
            next()
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
        
        final_fixtures_table <- final_fixtures_table %>% 
          mutate(scraped = FALSE)
        
        fixtures_list[[i]] <- final_fixtures_table
      }
    }
    
    if (length(fixtures_list) > 0) {
      fixtures_table <- bind_rows(fixtures_list) %>% 
        arrange(comp_id)
      if (file.exists("data/fixtures.csv")) {
        df_fixtures <- read_delim("data/fixtures.csv", show_col_types = FALSE) %>% 
          bind_rows(fixtures_table) %>% 
          distinct() %>% 
          arrange(comp_id)
        write_csv(df_fixtures, file = "data/fixtures.csv") 
      } else {
        write_csv(fixtures_table, file = "data/fixtures.csv") 
      }
    }
    
    first_run <- FALSE
    fixtures_counter <- fixtures_counter + 1
    
    if (length(skipped_seasons) > 0) {
      if (fixtures_counter > 20) {
        cat("I tried it now 20 times and nothing worked... I will stop now. Sorry... Everything I scraped so far is saved here: data/fixtures.csv")
        stop()
      } else {
        cat("Some seasons were skipped. I will try to get the data for them now...")
      }
      
    }
    
    
  }
  
}


# Read match data ----


if (file.exists("data/seasons.csv")) {
  cat("Reading seasons.csv\n")
  df_seasons <- read_delim("data/seasons.csv", show_col_types = FALSE)
} else {
  cat("Downloading competitions data\n")
  get_seasons_data()
  df_seasons <- read_delim("data/seasons.csv", show_col_types = FALSE)
}

if (file.exists("data/fixtures.csv")) {
  cat("Reading fixture.csv\n")
  df_fixtures <- read_delim("data/fixtures.csv", show_col_types = FALSE)
} else {
  cat("Downloading fixture data\n")
  get_fixture_data()
  df_fixtures <- read_delim("data/fixtures.csv", show_col_types = FALSE)
}

# match_list <- list()
# skipped_matches <- c()
# match_counter <- 0
# first_run <- TRUE 

# while(length(skipped_matches) > 0 | first_run == TRUE) {
#   
#   if (length(skipped_matches) == 0) {
#     iterator <- 1:nrow(df_seasons)
#   } else {
#     iterator <- skipped_matches
#   }

rename_match_table <- function(df, drop_last = TRUE, shots = FALSE) {
  new_column_names <- paste0(colnames(df),"_",df[1,])
  new_column_names <- new_column_names %>% 
    str_replace_all(pattern = " ", replacement = "_") %>% 
    str_replace_all(pattern = "%", replacement = "pct") %>% 
    str_replace_all(pattern = "1/3", replacement = "final3rd") %>% 
    str_replace_all(pattern = "^_", replacement = "") %>% 
    str_replace_all(pattern = "#", replacement = "number") %>% 
    str_to_lower()
  if (shots == FALSE) {
    prefix <- str_split_i(string = t, pattern = "_", i = 3)
    new_column_names[7:length(new_column_names)] <- paste0(prefix,"_",new_column_names[7:length(new_column_names)])
  } else {
    prefix <- "shots"
    new_column_names <- paste0(prefix,"_",new_column_names)
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

tabula_rasa <- TRUE

if (tabula_rasa == TRUE) {
  cat("Tabula rasa was activated. Deleting existing data/event_data.csv, data/field_data.csv and data/keeper_data.csv files.\n")
  for (d in c("data/event_data.csv", "data/field_data.csv", "data/keeper_data.csv")) {
    if (file.exists(d)) {
      file.remove(d)
      cat("Removed ",d,"\n")
    } else {
      cat(d," does not exist yet\n")
    }
  }
  cat("Tabula rasa finished. Start scraping now.")
}


for (i in 1:nrow(df_seasons)) {
  
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
    filter(season == current_season & comp_id == id & match_report == "Match Report" & scraped == FALSE & is.na(notes)) %>% 
    dplyr::select(wk, date, home, away, home_id, away_id, match_id, link_match)
  
  if (nrow(fixtures_season) == 0) {
    cat("Match data for ", comp_name ," (Season ",current_season,") is already complete\n", sep = "")
  } else {
    
    
    # if (file.exists("data/field_data.csv")) {
    #   recorded_matches <- read_delim("data/field_data.csv", show_col_types = FALSE) %>% 
    #     filter(current_season == current_season & comp_id == id) %>% 
    #     pull(match_id) %>% 
    #     unique()
    #   
    #   if (length(recorded_matches) != 0) {
    #     fixtures_season <- fixtures_season %>% 
    #       filter(!(match_id %in% recorded_matches))
    #   } 
    #   
    #   
    #   if (nrow(fixtures_season) == 0) { 
    #     cat("Match data for ", comp_name ," (Season ",current_season,") is already complete\n", sep = "")
    #     read_match <- FALSE
    #     Sys.sleep(1)
    #   } else {
    #     cat("Match data for ", comp_name ," (Season ",current_season,") are incomplete\n", sep = "")
    #     read_match <- TRUE
    #     Sys.sleep(1)
    #   }
    # } else {
    #   read_match <- TRUE
    # }
    
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
        for (t in c(paste0("stats_",team_id,"_summary"),
                    paste0("stats_",team_id,"_passing"),
                    paste0("stats_",team_id,"_passing_types"),
                    paste0("stats_",team_id,"_defense"),
                    paste0("stats_",team_id,"_possession"),
                    paste0("stats_",team_id,"_misc"))) {
          team_data <- match_page %>% 
            html_elements(paste0("#",t)) %>% 
            html_table()
          
          team_data[[1]] <- rename_match_table(team_data[[1]])
          team_data_df <- team_data[[1]] %>% 
            bind_rows()
          
          if (!(str_detect(t, ".*summary$"))) {
            
            team_data_df <- team_data_df %>% 
              dplyr::select(-c(1:6))
            
          } else {
            
            player_links <- match_page %>% 
              html_elements(paste0("#",t)) %>%
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
          field_match_data2[[t]] <- team_data_df
          
        }
        field_match_data_list[[team_id]] <- bind_cols(field_match_data2)
        
        #df[!duplicated(as.list(df))]
        
        keeper_table <- match_page %>% 
          html_elements(paste0("#",paste0("keeper_stats_",team_id))) %>% 
          html_table()
        keeper_table[[1]] <- rename_match_table(keeper_table[[1]], drop_last = FALSE)
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
               match_week = match_week
        )
      
      shot_table <- shot_table %>% 
        mutate(comp_name = comp_name,
               comp_id = id,
               current_season = current_season,
               match_id = match_id, 
               match_week = match_week
        )
      
      
      if (file.exists("data/event_data.csv")) {
        write_csv(event_df, "data/event_data.csv", append = TRUE)
      } else {
        write_csv(event_df, "data/event_data.csv")
      }
      
      if (file.exists("data/field_data.csv")) {
        write_csv(field_match_data_df, "data/field_data.csv", append = TRUE)
      } else {
        write_csv(field_match_data_df, "data/field_data.csv")
      }
      
      if (file.exists("data/keeper_data.csv")) {
        write_csv(keeper_match_data_df, "data/keeper_data.csv", append = TRUE)
      } else {
        write_csv(keeper_match_data_df, "data/keeper_data.csv")
      }
      
      if (file.exists("data/shot_data.csv")) {
        write_csv(shot_table, "data/shot_data.csv", append = TRUE)
      } else {
        write_csv(shot_table, "data/shot_data.csv")
      }
      
      last_match_id <- match_id
      df_fixtures <- df_fixtures %>% 
        mutate(scraped = case_when(match_id == last_match_id ~ TRUE,
                                   TRUE ~ scraped))
      
    }
    write_csv(df_fixtures, "data/fixtures.csv")
  }
  
}



if (read_match == TRUE) {
  table_id <- paste0("sched_",current_season,"_",id,"_1")
  
  cat("Read data for: ",str_replace(comp_name, "-", " ")," (Season ",current_season,")\n", sep = "")
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
                        possibly(.f = read_html, otherwise = NULL, quiet = TRUE))
      counter <- counter + 1
    }
    
    if (counter > 20 & is.null(match_page)) {
      cat("After 20 tries it still did not work... I will skip ",comp_name," (Season ",current_season,") for now and try later again.\n", sep = "")
      skipped_matches <- c(skipped_matches, i)
      next()
    }
    
    
  }
  
  match_page <- match_page %>% 
    html_elements(paste0("#",table_id))
  
  match_table <- match_page %>% 
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
  
  match_links <- match_page %>% 
    html_element("tbody") %>% 
    html_elements("tr") %>% 
    html_elements("td") %>% 
    html_elements("a") %>% 
    html_attr("href") 
  
  past_match_links <- match_links %>% 
    str_subset(paste0(".*/matches/.*",comp_name)) %>% 
    str_unique()
  
  coming_match_links <- match_links %>% 
    str_subset(paste0(".*/matchup/.*",comp_name)) %>% 
    str_unique()
  
  team_links <- match_links %>% 
    str_subset("squads")
  
  home_index <- seq(1,length(team_links), by = 2)
  away_index <- seq(2,length(team_links), by = 2)
  link_home <- team_links[home_index]
  link_away <- team_links[away_index]
  home_id <- link_home %>% 
    str_remove_all(".*squads/") %>% 
    str_remove_all("/.*")
  away_id <- link_away %>% 
    str_remove_all(".*squads/") %>% 
    str_remove_all("/.*")
  
  match_table <- match_table %>% 
    bind_cols(link_home = link_home,
              link_away = link_away,
              home_id = home_id,
              away_id = away_id) %>% 
    mutate(match_id = paste0(comp_id, Wk, home_id, away_id))
  
  past_matches <- match_table %>% 
    filter(Match_Report == "Match Report")
  
  coming_matches <- match_table %>% 
    filter(Match_Report != "Match Report")
  
  if (nrow(past_matches) == 0) {
    cat("No past matches for: ",str_replace(comp_name, "-", " ")," (Season ",current_season,")\n", sep = "")
    final_match_table <- bind_cols(bind_rows(past_matches, coming_matches), link_match = c(rep(NA, nrow(past_matches)),rep(NA, nrow(coming_matches)))) 
  } else {
    final_match_table <- bind_cols(bind_rows(past_matches, coming_matches), link_match = c(paste0(basic_url, past_match_links),rep(NA, nrow(coming_matches))))
    
    
  }
  colnames(final_match_table) <- str_to_lower(colnames(final_match_table))
  colnames(final_match_table) <- str_replace_all(colnames(final_match_table), " ", "_")
  colnames(final_match_table) <- str_replace_all(colnames(final_match_table), "\\.", "_")
  match_list[[i]] <- final_match_table
}
}

if (length(match_list) > 0) {
  match_table <- bind_rows(match_list) %>% 
    arrange(comp_id)
  if (file.exists("data/matches.csv")) {
    df_matches <- read_delim("data/matches.csv", show_col_types = FALSE) %>% 
      bind_rows(match_table) %>% 
      distinct() %>% 
      arrange(comp_id)
    write_csv(df_matches, file = "data/matches.csv") 
  } else {
    write_csv(match_table, file = "data/matches.csv") 
  }
}

first_run <- FALSE
match_counter <- match_counter + 1

if (length(skipped_matches) > 0) {
  if (match_counter > 20) {
    cat("I tried it now 20 times and nothing worked... I will stop now. Sorry... Everything I scraped so far is saved here: data/matches.csv")
    stop()
  } else {
    cat("Some seasons were skipped. I will try to get the data for them now...")
  }
  
}


}
