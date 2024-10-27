# sql helpers

create_seasons_table <- function(con, hard = FALSE) {
  query_create_seasons_table <- "CREATE TABLE seasons
  (     season TEXT,
     competition_name TEXT,
     x__squads REAL,
     champion TEXT,
     top_scorer TEXT,
     season_start REAL,
     season_end REAL,
     links TEXT,
     comp_id REAL,
     PRIMARY KEY(season, comp_id)
  )"
  if (hard == TRUE) {
    dbExecute(con, "DROP TABLE IF EXISTS seasons")
  }
  dbExecute(con, query_create_seasons_table)
}

create_fixtures_table <- function(con, hard = FALSE) {
  query_create_fixtures_table <- "CREATE TABLE fixtures
  (    wk REAL,
     day TEXT,
     date TEXT,
     time TEXT,
     home TEXT,
     xg_home REAL,
     score TEXT,
     xg_away REAL,
     away TEXT,
     attendance REAL,
     venue TEXT,
     referee TEXT,
     match_report TEXT,
     notes TEXT,
     comp_id REAL,
     season TEXT,
     goals_home REAL,
     goals_away REAL,
     points_home REAL,
     points_away REAL,
     link_home TEXT,
     link_away TEXT,
     home_id TEXT,
     away_id TEXT,
     match_id TEXT,
     link_match TEXT,
     scraped INTEGER,
     PRIMARY KEY(match_id)
  )"
  if (hard == TRUE) {
    dbExecute(con, "DROP TABLE IF EXISTS fixtures")
  }
  dbExecute(con, query_create_fixtures_table)
}

create_competitions_table <- function(con, hard = FALSE) {
  query_create_competitions_table <- "CREATE TABLE competitions
  (competition_name TEXT,
     gender TEXT,
     country TEXT,
     first_season TEXT,
     last_season TEXT,
     tier TEXT,
     links TEXT,
     comp_id REAL,
     last_season_start REAL,
     last_season_end REAL,
     PRIMARY KEY(comp_id)
  )"
  if (hard == TRUE) {
    dbExecute(con, "DROP TABLE IF EXISTS competitions")
  }
  dbExecute(con, query_create_competitions_table)
}

create_shot_data_table <- function(con, hard = FALSE) {
  query_create_shot_data_table <- "CREATE TABLE shot_data
  (        shots_minute TEXT,
     shots_player TEXT,
     shots_squad TEXT,
     shots_xg REAL,
     shots_psxg REAL,
     shots_outcome TEXT,
     shots_distance REAL,
     shots_body_part TEXT,
     shots_notes TEXT,
     shots_sca_1_player TEXT,
     shots_sca_1_event TEXT,
     shots_sca_2_player TEXT,
     shots_sca_2_event TEXT,
     shot_player_links TEXT,
     shot_player_id TEXT,
     shot_squad_links TEXT,
     shot_squad_id TEXT,
     shot_sca1_links TEXT,
     shot_sca1_id TEXT,
     shot_sca2_links TEXT,
     shot_sca2_id TEXT,
     comp_name TEXT,
     comp_id REAL,
     current_season TEXT,
     match_id TEXT,
     match_week REAL,
     shot_counter REAL,
     PRIMARY KEY(match_id, shot_counter)
  )"
  if (hard == TRUE) {
    dbExecute(con, "DROP TABLE IF EXISTS shot_data")
  }
  dbExecute(con, query_create_shot_data_table)
}

create_keeper_data_table <- function(con, hard = FALSE) {
  query_create_keeper_data_table <- "CREATE TABLE keeper_data
  (     team_id TEXT,
     player TEXT,
     nation TEXT,
     age TEXT,
     min REAL,
     shot_stopping_sota REAL,
     shot_stopping_ga REAL,
     misc_shot_stopping_saves REAL,
     misc_shot_stopping_savepct REAL,
     misc_shot_stopping_psxg REAL,
     misc_launched_cmp REAL,
     misc_launched_att REAL,
     misc_launched_cmppct REAL,
     misc_passes_att_gk REAL,
     misc_passes_thr REAL,
     misc_passes_launchpct REAL,
     misc_passes_avglen REAL,
     misc_goal_kicks_att REAL,
     misc_goal_kicks_launchpct REAL,
     misc_goal_kicks_avglen REAL,
     misc_crosses_opp REAL,
     misc_crosses_stp REAL,
     misc_crosses_stppct REAL,
     misc_sweeper_numberopa REAL,
     misc_sweeper_avgdist REAL,
     player_links TEXT,
     player_id TEXT,
     comp_name TEXT,
     comp_id REAL,
     current_season TEXT,
     match_id TEXT,
     match_week REAL,
     PRIMARY KEY(match_id, player_id)
  )"
  if (hard == TRUE) {
    dbExecute(con, "DROP TABLE IF EXISTS keeper_data")
  }
  dbExecute(con, query_create_keeper_data_table)
}

create_field_data_table <- function(con, hard = FALSE) {
  query_create_field_data_table <- 
    "CREATE TABLE field_data
          (match_id TEXT,
          player_id TEXT,
          team_id TEXT,
          link_player TEXT,
          player TEXT,
          number REAL,
          nation TEXT,
          pos TEXT,
          age TEXT,
          min REAL,
          comp_name TEXT,
          comp_id REAL,
          current_season TEXT,
          match_week REAL,
          summary_performance_gls REAL,
     summary_performance_ast REAL,
     summary_performance_pk REAL,
     summary_performance_pkatt REAL,
     summary_performance_sh REAL,
     summary_performance_sot REAL,
     summary_performance_crdy REAL,
     summary_performance_crdr REAL,
     summary_performance_touches REAL,
     summary_performance_tkl REAL,
     summary_performance_int REAL,
     summary_performance_blocks REAL,
     summary_expected_xg REAL,
     summary_expected_npxg REAL,
     summary_expected_xag REAL,
     summary_sca_sca REAL,
     summary_sca_gca REAL,
     summary_passes_cmp REAL,
     summary_passes_att REAL,
     summary_passes_cmppct REAL,
     summary_passes_prgp REAL,
     summary_carries_carries REAL,
     summary_carries_prgc REAL,
     summary_take_ons_att REAL,
     summary_take_ons_succ REAL,
     passing_total_cmp REAL,
     passing_total_att REAL,
     passing_total_cmppct REAL,
     passing_total_totdist REAL,
     passing_total_prgdist REAL,
     passing_short_cmp REAL,
     passing_short_att REAL,
     passing_short_cmppct REAL,
     passing_medium_cmp REAL,
     passing_medium_att REAL,
     passing_medium_cmppct REAL,
     passing_long_cmp REAL,
     passing_long_att REAL,
     passing_long_cmppct REAL,
     passing_ast REAL,
     passing_xag REAL,
     passing_xa REAL,
     passing_kp REAL,
     passing_final3rd REAL,
     passing_ppa REAL,
     passing_crspa REAL,
     passing_prgp REAL,
     passing_att REAL,
     passing_pass_types_live REAL,
     passing_pass_types_dead REAL,
     passing_pass_types_fk REAL,
     passing_pass_types_tb REAL,
     passing_pass_types_sw REAL,
     passing_pass_types_crs REAL,
     passing_pass_types_ti REAL,
     passing_pass_types_ck REAL,
     passing_corner_kicks_in REAL,
     passing_corner_kicks_out REAL,
     passing_corner_kicks_str REAL,
     passing_outcomes_cmp REAL,
     passing_outcomes_off REAL,
     passing_outcomes_blocks REAL,
     defense_tackles_tkl REAL,
     defense_tackles_tklw REAL,
     defense_tackles_def_3rd REAL,
     defense_tackles_mid_3rd REAL,
     defense_tackles_att_3rd REAL,
     defense_challenges_tkl REAL,
     defense_challenges_att REAL,
     defense_challenges_tklpct REAL,
     defense_challenges_lost REAL,
     defense_blocks_blocks REAL,
     defense_blocks_sh REAL,
     defense_blocks_pass REAL,
     defense_int REAL,
     defense_tkl_int REAL,
     defense_clr REAL,
     defense_err REAL,
     possession_touches_touches REAL,
     possession_touches_def_pen REAL,
     possession_touches_def_3rd REAL,
     possession_touches_mid_3rd REAL,
     possession_touches_att_3rd REAL,
     possession_touches_att_pen REAL,
     possession_touches_live REAL,
     possession_take_ons_att REAL,
     possession_take_ons_succ REAL,
     possession_take_ons_succpct REAL,
     possession_take_ons_tkld REAL,
     possession_take_ons_tkldpct REAL,
     possession_carries_carries REAL,
     possession_carries_totdist REAL,
     possession_carries_prgdist REAL,
     possession_carries_prgc REAL,
     possession_carries_final3rd REAL,
     possession_carries_cpa REAL,
     possession_carries_mis REAL,
     possession_carries_dis REAL,
     possession_receiving_rec REAL,
     possession_receiving_prgr REAL,
     misc_performance_crdy REAL,
     misc_performance_crdr REAL,
     misc_performance_2crdy REAL,
     misc_performance_fls REAL,
     misc_performance_fld REAL,
     misc_performance_off REAL,
     misc_performance_crs REAL,
     misc_performance_int REAL,
     misc_performance_tklw REAL,
     misc_performance_pkwon REAL,
     misc_performance_pkcon REAL,
     misc_performance_og REAL,
     misc_performance_recov REAL,
     misc_aerial_duels_won REAL,
     misc_aerial_duels_lost REAL,
     misc_aerial_duels_wonpct REAL,
          PRIMARY KEY (match_id, player_id)
          )"
  if (hard == TRUE) {
    dbExecute(con, "DROP TABLE IF EXISTS field_data")
  }
  dbExecute(con, query_create_field_data_table)
}

create_event_data_table <- function(con) {
  query_create_event_data_table <- 
    "CREATE TABLE event_data
          (match_id TEXT,
          event_counter REAL,
          match_week REAL,
          comp_id REAL,
          comp_name TEXT,
          current_season TEXT,
          team_id TEXT,
          team_logo_link TEXT,
          event_time TEXT,
          event_type TEXT,
          event_score TEXT,
          event_score_home REAL,
          event_score_away REAL,
          event_player_first TEXT,
          event_player_second TEXT,
          event_player_first_id TEXT,
          event_player_second_id TEXT,
          PRIMARY KEY (match_id, event_counter)
          )"
  if (hard == TRUE) {
    dbExecute(con, "DROP TABLE IF EXISTS event_data")
  }
  dbExecute(con, query_create_event_data_table)
}