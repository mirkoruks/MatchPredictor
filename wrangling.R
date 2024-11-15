# run ml model to predict match results 

rm(list = ls())

library(tidyverse)

library(DBI)
library(RSQLite)
library(zoo)

# first prepare a decent data frame

con <- dbConnect(SQLite(), "fbref.db")

fixture_data <- dbGetQuery(con, "SELECT * FROM fixtures")
field_data <- dbGetQuery(con, "SELECT * FROM field_data")
keeper_data <- dbGetQuery(con, "SELECT * FROM keeper_data")

# Prepare fixtures table ----
cat("Prepare fixtures data\n")
fixture_data <- fixture_data %>% 
  filter(scraped == 1) %>% 
  mutate(conceded_home = goals_away,
         conceded_away = goals_home) %>% 
  select(date, day, xg_home, xg_away, goals_home, goals_away, points_home, points_away, home_id, away_id, match_id, attendance, time, conceded_home, conceded_away, Elo_home, Elo_away, Elodiff_home, We_home, We_away) %>% 
  mutate(day = case_when(day == "Mon" ~ 1,
                         day == "Tue" ~ 2,
                         day == "Wed" ~ 3,
                         day == "Thu" ~ 4,
                         day == "Fri" ~ 5,
                         day == "Sat" ~ 6,
                         day == "Sun" ~ 7,
                         TRUE ~ NA),
         time = str_extract(time, "\\d\\d:\\d\\d"),
         time = as.numeric(hm(time)),
         Elodiff_away = Elodiff_home * -1) %>% 
  rename(team_id_home = home_id,
         team_id_away = away_id) %>% 
  pivot_longer(cols = ends_with(c("_home","_away")),
               names_to = c(".value","type"),
               names_pattern = "(.*)_(.*)$") %>% 
  mutate(xg_performance = goals/xg)


# Prepare field data table ----
cat("Prepare field data\n")
field_data_features <- field_data %>% 
  select(starts_with(c("summary_","passing_","defense_","possession_","misc_")))
field_data_features_names <- names(field_data_features[!duplicated(as.list(field_data_features))])
rm(field_data_features)

field_data <- field_data %>% 
  select(match_id:match_week, all_of(field_data_features_names)) %>% 
  mutate(age_years = str_extract(age, ".*-"),
         age_years = as.numeric(str_remove(age_years, "-")),
         age_days = str_extract(age, "-.*"),
         age_days = as.numeric(str_remove(age_days, "-")),
         age_full = age_years + age_days / 365) %>% 
  select(-c(age, age_years, age_days)) %>% 
  rename(age = age_full) %>% 
  group_by(match_id, team_id) %>% 
  mutate(nation_count = n_distinct(nation)) %>% 
  ungroup() %>% 
  select(-c(player_id, link_player, player, number, nation, pos, min, comp_name, comp_id, current_season, match_week, summary_expected_npxg, summary_passes_cmp, summary_passes_att, passing_medium_cmp, passing_medium_att, passing_short_cmp, passing_short_att, passing_long_cmp, passing_long_att, defense_challenges_att, defense_challenges_tkl, summary_take_ons_att, summary_take_ons_succ, possession_take_ons_tkld, misc_aerial_duels_won, misc_aerial_duels_lost)) 

field_data <- field_data %>% 
  group_by(match_id, team_id) %>% 
  summarize(across(c(age, nation_count, ends_with("pct")), 
                   ~ mean(.x, na.rm = TRUE)),
            across(-c(age, nation_count, ends_with("pct")),
                   ~ sum(.x, na.rm = TRUE))) %>% 
  ungroup()

# Prepare keeper data table ----
cat("Prepare keeper data\n")
keeper_data <- keeper_data %>% 
  relocate(starts_with("keeper"), .after = "match_week") %>% 
  relocate(match_id, .before = "team_id")

keeper_data_features <- keeper_data %>% 
  select(starts_with(c("keeper_")))
keeper_data_features_names <- names(keeper_data_features[!duplicated(as.list(keeper_data_features))])
rm(keeper_data_features)

keeper_data <- keeper_data %>% 
  select(match_id:match_week, all_of(keeper_data_features_names)) 

keeper_data <- keeper_data %>% 
  select(-c(keeper_shot_stopping_ga, keeper_shot_stopping_saves, keeper_launched_cmp, keeper_launched_att,keeper_crosses_opp, keeper_crosses_stp, keeper_passes_att_gk, keeper_passes_thr, keeper_goal_kicks_att))%>% 
  group_by(match_id, team_id) %>% 
  mutate(min_total = sum(min, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(min_rel = min/min_total)

keeper_data <- keeper_data %>% 
  select(match_id, min_rel, team_id, starts_with("keeper_")) 

keeper_data <- keeper_data %>% 
  group_by(match_id, team_id) %>% 
  summarize(across(everything(), 
                   ~ weighted.mean(.x, w = min_rel, na.rm = TRUE))) %>% 
  ungroup() %>% 
  select(-min_rel)

# Join everything ----
fixture_data <- fixture_data %>% 
  full_join(field_data) %>% 
  full_join(keeper_data)

rm(field_data)
rm(keeper_data)


fixture_data <- fixture_data %>% 
  relocate(c(day, attendance, time), .after = "type") %>% 
  relocate(team_id, .before = "day")

feature_space_raw <- names(fixture_data)[-c(1:4)]

cat("Create lagged variables\n")
fixture_data_final <- fixture_data %>% 
  arrange(as.Date(date)) %>% 
  group_by(team_id) %>% 
  mutate(EloChange = Elo - lag(Elo),
         ElodiffChange = Elodiff - lag(Elodiff),
         WeChange = We - lag(We)) %>% 
  mutate(across(.cols = all_of(feature_space_raw[!(feature_space_raw %in% c("Elo","Elodiff","We"))]),
                .fns = ~ lag(.x) - lag(.x,2),
                .names = "{col}Change")) %>% 
  ungroup()

#fixture_data_final %>% select(date,match_id, type, team_id, starts_with(c("Elo","passing_medium_cmppct"))) %>% View()
feature_space_update <- names(fixture_data_final)[-c(1:4)]

fixture_data_final <- fixture_data_final %>% 
  arrange(as.Date(date)) %>% 
  group_by(team_id) %>% 
  mutate(across(.cols = all_of(feature_space_update),
                .fns = ~ rollapply(lag(.x), 5, mean, na.rm = TRUE, align = "right", fill = NA),
                .names = "{col}_mean5"),
         across(.cols = all_of(feature_space_update),
                .fns = ~ rollapply(lag(.x), 10, mean, na.rm = TRUE, align = "right", fill = NA),
                .names = "{col}_mean10"),
         across(.cols = all_of(feature_space_update),
                .fns = ~ rollapply(lag(.x), 15, mean, na.rm = TRUE, align = "right", fill = NA),
                .names = "{col}_mean15")) %>% 
  ungroup()

fixture_data_final <- fixture_data_final %>% 
  mutate(home_result = case_when(points == 0 ~ "L",
                                 points == 1 ~ "D",
                                 points == 3 ~ "W"),
         home_result = factor(home_result)) 

fixture_data_final

cat("Create difference scores\n")
fixture_data_final <- fixture_data_final %>% 
  group_by(match_id) %>% 
  arrange(type, .by_group = TRUE) %>% 
  mutate(across(c(ends_with(c("_mean5", "_mean10", "_mean15"))),
                ~ .x - lag(.x),
                .names = "HomeDiff_{col}")) %>% 
  ungroup()


fixture_data_final <- fixture_data_final %>% 
  filter(type == "home") %>% 
  select(date, match_id, home_result, day, time, Elo, Elodiff, We, starts_with(c("HomeDiff")))

# %>% 
# select(date, match_id, type, team_id, day, time, Elo, Elodiff, We, home_result, ends_with(c("_mean5", "_mean10", "_mean15")))

write_csv(fixture_data_final, "data/modeldata.csv")


