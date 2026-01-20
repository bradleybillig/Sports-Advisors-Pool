# ============================================================
# Script 1: Ingest Round Responses and Build Canonical Inputs
#
# Purpose:
#   Read the Google Form / Excel round sheet, keep the latest submission per player,
#   and produce the standardized “source of truth” CSVs used by the rest of the pipeline.
#
# Output directory structure (auto-created):
#   <project_root>/out/wild_card
#   <project_root>/out/divisional
#   <project_root>/out/conference
#   <project_root>/out/super_bowl
# ============================================================

library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(janitor)
library(readr)
library(jsonlite)
library(tibble)

# ----------------------------
# PATH HELPERS
# ----------------------------
find_up <- function(target, start = getwd(), max_depth = 25) {
  d <- normalizePath(start, winslash = "/", mustWork = FALSE)
  for (i in seq_len(max_depth)) {
    candidate <- file.path(d, target)
    if (file.exists(candidate)) return(candidate)
    parent <- dirname(d)
    if (identical(parent, d)) break
    d <- parent
  }
  NA_character_
}

project_root <- function() {
  proj <- find_up("Sports Advisors.Rproj")
  if (!is.na(proj)) return(dirname(proj))
  # fallback: assume script is run from scripts/ and project root is one level up
  normalizePath("..", winslash = "/", mustWork = FALSE)
}

# ----------------------------
# HELPERS
# ----------------------------
norm_email <- function(x) str_to_lower(str_trim(as.character(x)))

clean_pick_display <- function(x) {
  x |>
    as.character() |>
    str_replace_all("\u00A0", " ") |>
    str_squish() |>
    str_trim()
}

normalize_pick <- function(x) {
  x |>
    clean_pick_display() |>
    str_to_lower() |>
    str_replace_all("½", ".5") |>
    str_replace_all("–|—", "-") |>
    str_trim()
}

pretty_label_from_game <- function(game_label) {
  game_label |>
    str_replace_all("_", " ") |>
    str_squish() |>
    str_to_title()
}

is_lock_col <- function(col_nm) {
  str_detect(
    col_nm,
    "point_[12]|lock_[12]|first_lock|second_lock|bonus_lock|lock_of_the_weekend|lock_of_the_week"
  )
}

lock_slot_hint <- function(col_nm) {
  dplyr::case_when(
    str_detect(col_nm, "point_1|lock_1|first_lock") ~ 1L,
    str_detect(col_nm, "point_2|lock_2|second_lock") ~ 2L,
    TRUE ~ NA_integer_
  )
}

is_conference_tiebreaker_col <- function(col_nm) {
  str_detect(col_nm, "weekly_payout_tiebreaker|tiebreak|price_is_right")
}

# ----------------------------
# MAIN
# ----------------------------
process_pool_round <- function(path,
                               sheet_name,
                               season_val,
                               round_code,     # "wc", "dr", "con", "sb"
                               out_dir = ".") {
  
  round_code <- tolower(round_code)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  # 1) Read + clean names
  df <- read_excel(path, sheet = sheet_name) |>
    clean_names()
  
  # 2) Find email column, standardize
  email_col <- names(df)[str_detect(names(df), "email")]
  stopifnot(length(email_col) >= 1)
  email_col <- email_col[1]
  
  df <- df |>
    mutate(email = norm_email(.data[[email_col]]))
  
  # Standardize name if present
  if (!("name" %in% names(df))) {
    name_guess <- names(df)[str_detect(names(df), "^name$|full_name|your_name")]
    if (length(name_guess) >= 1) df <- df |> rename(name = all_of(name_guess[1]))
  }
  
  stopifnot("timestamp" %in% names(df))
  
  # 3) Latest submission wins
  df_latest <- df |>
    arrange(email, desc(timestamp)) |>
    group_by(email) |>
    slice(1) |>
    ungroup()
  
  # 4) Identify pick columns
  id_cols <- names(df_latest)[str_detect(names(df_latest), "email|timestamp|time|name")]
  pick_cols <- setdiff(names(df_latest), id_cols)
  stopifnot(length(pick_cols) > 0)
  
  # Cast all pick cols to character to avoid pivot_longer type collisions
  df_latest <- df_latest |>
    mutate(across(all_of(pick_cols), as.character))
  
  col_order <- tibble(col = pick_cols, col_pos = seq_along(pick_cols))
  
  # 5) Pivot long (raw)
  picks_raw <- df_latest |>
    select(any_of(c("timestamp", "email", "name")), all_of(pick_cols)) |>
    pivot_longer(cols = all_of(pick_cols), names_to = "col", values_to = "response") |>
    filter(!is.na(response), response != "") |>
    mutate(
      season = season_val,
      round  = round_code
    ) |>
    select(season, round, timestamp, email, any_of("name"), col, response)
  
  # 6) Field map
  field_map_base <- picks_raw |>
    distinct(season, round, col) |>
    left_join(col_order, by = "col") |>
    mutate(
      market_base = case_when(
        round_code == "con" & is_conference_tiebreaker_col(col) ~ "conf_tiebreaker",
        str_detect(col, "_spread$") ~ "spread",
        str_detect(col, "_total$")  ~ "total",
        is_lock_col(col) ~ "bonus_lock",
        TRUE ~ NA_character_
      ),
      lock_slot_hint = if_else(market_base == "bonus_lock", lock_slot_hint(col), NA_integer_)
    )
  
  lock_slots <- field_map_base |>
    filter(market_base == "bonus_lock") |>
    arrange(col_pos) |>
    group_by(season, round) |>
    group_modify(~{
      x <- .x
      if (nrow(x) > 2) x <- x[1:2, , drop = FALSE]
      
      slot <- x$lock_slot_hint
      
      if (sum(slot == 1L, na.rm = TRUE) > 1) slot[which(slot == 1L)[-1]] <- NA_integer_
      if (sum(slot == 2L, na.rm = TRUE) > 1) slot[which(slot == 2L)[-1]] <- NA_integer_
      
      used <- slot[!is.na(slot)]
      remaining <- setdiff(c(1L, 2L), used)
      
      na_idx <- which(is.na(slot))
      if (length(na_idx) > 0) {
        slot[na_idx] <- head(rep(remaining, length.out = length(na_idx)), length(na_idx))
      }
      
      x$lock_slot <- slot
      x
    }) |>
    ungroup() |>
    select(season, round, col, lock_slot)
  
  field_map <- field_map_base |>
    left_join(lock_slots, by = c("season", "round", "col")) |>
    mutate(
      market = case_when(
        market_base %in% c("spread", "total") ~ market_base,
        market_base == "bonus_lock" & !is.na(lock_slot) ~ paste0("bonus_lock_", lock_slot),
        market_base == "conf_tiebreaker" ~ "conf_tiebreaker_total_points",
        TRUE ~ NA_character_
      ),
      value_type = case_when(
        market == "conf_tiebreaker_total_points" ~ "tiebreaker",
        TRUE ~ "pick"
      ),
      game_key = case_when(
        market == "spread" ~ str_replace(col, "(_spread(_|$).*)$", ""),
        market == "total"  ~ str_replace(col, "(_total(_|$).*)$", ""),
        TRUE ~ NA_character_
      ) |>
        str_replace_all("_+$", "")
    )
  
  # 7) Games table with stable IDs
  new_games <- field_map |>
    filter(market %in% c("spread", "total"), !is.na(game_key), game_key != "") |>
    distinct(season, round, game_key) |>
    arrange(game_key) |>
    transmute(
      season, round,
      game_label = game_key,
      game_title = pretty_label_from_game(game_key)
    )
  
  games_path <- file.path(out_dir, sprintf("games_%s.csv", round_code))
  
  if (file.exists(games_path)) {
    old_games <- read_csv(games_path, show_col_types = FALSE) |>
      transmute(season, round, game_id = as.character(game_id), game_label)
    
    games_fixed <- new_games |>
      left_join(old_games, by = c("season", "round", "game_label")) |>
      mutate(game_id = as.character(game_id))
    
    existing_nums <- old_games$game_id |>
      str_extract("\\d+$") |>
      suppressWarnings(as.integer())
    
    max_n <- max(existing_nums, na.rm = TRUE)
    if (!is.finite(max_n)) max_n <- 0L
    
    need_id <- which(is.na(games_fixed$game_id) | games_fixed$game_id == "")
    if (length(need_id) > 0) {
      new_ids <- sprintf("%s_%02d", toupper(round_code), seq(max_n + 1L, max_n + length(need_id)))
      games_fixed$game_id[need_id] <- new_ids
    }
    
    games_fixed <- games_fixed |>
      select(season, round, game_id, game_label, game_title)
    
  } else {
    games_fixed <- new_games |>
      mutate(game_id = sprintf("%s_%02d", toupper(round_code), row_number())) |>
      select(season, round, game_id, game_label, game_title)
  }
  
  if (sum(field_map$market %in% c("spread", "total")) > 0) {
    stopifnot(nrow(games_fixed) >= 1)
  }
  
  # 8) Attach game_id back to each column
  field_map_filled <- field_map |>
    left_join(games_fixed, by = c("season", "round", "game_key" = "game_label")) |>
    distinct(season, round, col, .keep_all = TRUE)
  
  missing_game <- field_map_filled |>
    filter(market %in% c("spread", "total")) |>
    filter(is.na(game_id) | game_id == "")
  
  if (nrow(missing_game) > 0) {
    stop("Some spread/total columns did not receive a game_id. Check field_map and column naming.")
  }
  
  # 9) Clean picks
  picks_clean <- picks_raw |>
    left_join(field_map_filled, by = c("season", "round", "col")) |>
    filter(!is.na(market)) |>
    transmute(
      season,
      round,
      email,
      timestamp,
      source_col = col,
      game_id,
      market,
      value_type,
      pick_value_raw = as.character(response),
      pick_value     = clean_pick_display(response),
      pick_norm      = normalize_pick(response)
    )
  
  # 10) Players
  players <- df_latest |>
    transmute(
      player_id = row_number(),
      season = season_val,
      round  = round_code,
      email,
      name = if ("name" %in% names(df_latest)) str_trim(name) else NA_character_
    ) |>
    arrange(email)
  
  # 11) Diagnostics + unmapped
  unmapped <- field_map_filled |>
    filter(is.na(market)) |>
    arrange(col_pos) |>
    select(season, round, col, col_pos)
  
  diagnostics <- tibble(
    season = season_val,
    round  = round_code,
    sheet  = sheet_name,
    players = nrow(df_latest),
    picks_raw_rows = nrow(picks_raw),
    picks_clean_rows = nrow(picks_clean),
    mapped_spread_total_cols = sum(field_map_filled$market %in% c("spread", "total"), na.rm = TRUE),
    mapped_lock_cols = sum(str_detect(coalesce(field_map_filled$market, ""), "^bonus_lock_"), na.rm = TRUE),
    mapped_tiebreaker_cols = sum(field_map_filled$market == "conf_tiebreaker_total_points", na.rm = TRUE),
    unique_games = nrow(games_fixed),
    unmapped_cols = nrow(unmapped)
  )
  
  # 12) Write outputs
  write_csv(players,          file.path(out_dir, sprintf("players_%s.csv", round_code)))
  write_csv(picks_raw,        file.path(out_dir, sprintf("picks_raw_%s.csv", round_code)))
  write_csv(games_fixed,      file.path(out_dir, sprintf("games_%s.csv", round_code)))
  write_csv(field_map_filled, file.path(out_dir, sprintf("field_map_%s.csv", round_code)))
  write_csv(picks_clean,      file.path(out_dir, sprintf("picks_clean_%s.csv", round_code)))
  write_csv(unmapped,         file.path(out_dir, sprintf("unmapped_cols_%s.csv", round_code)))
  write_csv(diagnostics,      file.path(out_dir, sprintf("extract_diagnostics_%s.csv", round_code)))
  
  cfg <- list(
    season = season_val,
    round_code = round_code,
    sheet_name = sheet_name,
    out_dir = out_dir,
    created_at = format(Sys.time(), tz = "America/Chicago", usetz = TRUE)
  )
  write_json(cfg, file.path(out_dir, sprintf("config_%s.json", round_code)),
             auto_unbox = TRUE, pretty = TRUE)
  
  message(
    sheet_name,
    " -> players: ", nrow(df_latest),
    " | picks_raw rows: ", nrow(picks_raw),
    " | picks_clean rows: ", nrow(picks_clean),
    " | games: ", nrow(games_fixed),
    " | unmapped cols: ", nrow(unmapped)
  )
  
  invisible(list(
    players = players,
    picks_raw = picks_raw,
    games = games_fixed,
    field_map = field_map_filled,
    picks_clean = picks_clean,
    diagnostics = diagnostics
  ))
}

# ----------------------------
# RUN CALLS
# ----------------------------
root_dir <- project_root()

# Workbook expected in project root
path <- file.path(root_dir, "2026 Football Responses.xlsx")
if (!file.exists(path)) stop("Workbook not found at: ", path)

season_val <- 2026

# All outputs go under <project_root>/out/<round_folder>
out_base <- file.path(root_dir, "out")
dir.create(out_base, showWarnings = FALSE, recursive = TRUE)

out_wc  <- file.path(out_base, "wild_card")
out_dr  <- file.path(out_base, "divisional")
out_con <- file.path(out_base, "conference")
out_sb  <- file.path(out_base, "super_bowl")

dir.create(out_wc,  showWarnings = FALSE, recursive = TRUE)
dir.create(out_dr,  showWarnings = FALSE, recursive = TRUE)
dir.create(out_con, showWarnings = FALSE, recursive = TRUE)
dir.create(out_sb,  showWarnings = FALSE, recursive = TRUE)

wc  <- process_pool_round(path, "Wild Card Weekend", season_val, "wc",  out_dir = out_wc)
dr  <- process_pool_round(path, "Divisional Round",  season_val, "dr",  out_dir = out_dr)
con <- process_pool_round(path, "Conference",        season_val, "con", out_dir = out_con)
sb  <- process_pool_round(path, "SuperBowl",         season_val, "sb",  out_dir = out_sb)
