library(tidyverse)
library(rvest)
library(DBI)
library(RSQLite)

df_seasons <- seasons_table




club_image_downloader <- function(image_link, comp_name) {
  
  image_dir <- paste0("data/images/",comp_name,"/")
  if (!dir.exists(image_dir)) {
    dir.create(image_dir, recursive = T)
  }
  image_name <- image_link %>% 
    str_replace(".*fb/","")
  download.file(image_link, paste0(image_dir,image_name), mode = "wb")
}


get_image_data <- function(df_season) {
  
  image_list <- list()
  for (i in 1:nrow(df_seasons)) {
    
    stats_season <- df_seasons %>% 
      arrange(desc(season_end)) %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(season)
    
    stats_compid <- df_seasons %>% 
      arrange(desc(season_end)) %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(comp_id)
    
    fixture_link <- df_seasons %>% 
      arrange(desc(season_end)) %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(links)
    
    stats_compname <- df_seasons %>% 
      arrange(desc(season_end)) %>% 
      filter(row_number() == as.numeric(i)) %>% 
      pull(competition_name) 
    
    if ("images" %in% dbListTables(con)) {
      Sys.sleep(0.2)
      existing_images <- dbGetQuery(con, paste0("select * from images where season = '",stats_season,"' AND comp_id = ",stats_compid))
      if (nrow(existing_images) > 0) {
        continue <- FALSE
        cat(paste0("Image data for ",stats_compname, " (",stats_season,") is already complete\n"))
      } else {
        continue <- TRUE
      }
    } else {
      continue <- TRUE
    }
    
    if (continue == TRUE) {
      
      
      stats_link <- fixture_link %>% 
        str_replace("/schedule","") %>% 
        str_replace("Scores-and-Fixtures","Stats")
      
      Sys.sleep(6)
      cat(paste0("Get image data for ",stats_compname, " (",stats_season,")\n"))
      image_page <- read_html(stats_link)
      
      image_links <- image_page %>%
        html_element(paste0("#all_results",stats_season,stats_compid,"1")) %>% 
        html_element("tbody") %>% 
        html_elements("tr") %>% 
        html_elements("td[data-stat='team']") %>% 
        html_elements("img") %>% 
        html_attr("src") 
      
      team_name <- image_page %>%
        html_element(paste0("#all_results",stats_season,stats_compid,"1")) %>% 
        html_element("tbody") %>% 
        html_elements("tr") %>% 
        html_elements("td[data-stat='team']") %>% 
        html_elements("a") %>% 
        html_text2()
      
      image_links <- image_links %>% 
        str_replace_all("mini\\.","")
      
      team_id <- image_links %>% 
        str_remove_all("(.*fb/)|(.png)")
      
      image_list[[i]] <- tibble(comp_id = stats_compid,
                                season = stats_season,
                                image_link = image_links,
                                team_id = team_id,
                                team_name = team_name)
      
    }
  }
  image_df <- image_list %>% 
    bind_rows()
  return(image_df)
}


a <- get_image_data(seasons_table)

fixture_link <- "https://fbref.com/en/comps/12/2023-2024/schedule/2023-2024-La-Liga-Scores-and-Fixtures"
"https://fbref.com/en/comps/12/2023-2024/2023-2024-La-Liga-Stats"
