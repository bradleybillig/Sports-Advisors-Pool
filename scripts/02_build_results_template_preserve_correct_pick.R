# ============================================================
# 02_build_results_template_preserve_correct_pick.R
#
# PURPOSE
# Build (or update) the per-round results entry file:
#   <out_dir>/results_<round>.csv
#
# This script:
#   1) Reads Script 1 outputs:
#        - picks_clean_<round>.csv   (spread/total picks)
#        - games_<round>.csv         (game_id + game_label)
#   2) Creates a results template with:
#        - one row per (game_id x market), where market is spread/total
#        - option_1, option_2, ... columns listing observed pick options
#   3) Preserves any existing "correct_pick" values if results_<round>.csv
#      already exists (safe to re-run after you start entering winners).
#   4) Deduplicates option values using pick_norm (from Script 1) to avoid
#      duplicate-looking options caused by spacing/case/formatting.
#
# HOW TO USE
# 1) Set round_code to the current round (e.g., "wc", "dr", "con", "sb").
# 2) Run Script 1 first for that round so the required input files exist.
# 3) Run this script to generate/update:
#      <round> out/results_<round>.csv
# 4) Open results_<round>.csv and fill in "correct_pick" for each row:
#      - Choose EXACTLY one of the option_* values for that row
#      - Leave blank (NA) until the game is final
# 5) Re-run this script any time you want to refresh the option columns
#    (for example after late submissions) without losing correct_pick entries.
#
# NOTES / GUARANTEES
# - Safe re-runs: correct_pick is carried forward by (season, round, game_id, market).
# - Human-friendly: game_label is included to help you recognize matchups quickly.
# - If you see many NA option_* columns, verify picks_clean_<round>.csv has rows.
# ============================================================

library(readr)
library(dplyr)
library(tidyr)
library(stringr)

# ----------------------------
# ROUND + PATH CONFIG
# ----------------------------
round_code <- "con"   # "wc", "dr", "con", "sb"

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
  normalizePath("..", winslash = "/", mustWork = FALSE)
}

round_folder <- function(rc) {
  rc <- tolower(rc)
  map <- c(
    wc  = "wild_card",
    dr  = "divisional",
    con = "conference",
    sb  = "super_bowl"
  )
  if (!rc %in% names(map)) stop("Unknown round_code: ", rc)
  unname(map[[rc]])
}


root_dir <- project_root()
out_dir  <- file.path(root_dir, "out", round_folder(round_code))
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

results_path <- file.path(out_dir, sprintf("results_%s.csv", round_code))


# Load cleaned picks (spread/total only)
picks <- read_csv(
  file.path(out_dir, sprintf("picks_clean_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  filter(market %in% c("spread", "total")) |>
  mutate(
    season  = as.character(season),
    round   = as.character(round),
    game_id = as.character(game_id)
  )

# Load games
games <- read_csv(
  file.path(out_dir, sprintf("games_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  mutate(
    season  = as.character(season),
    round   = as.character(round),
    game_id = as.character(game_id)
  )

# Guards
if (nrow(games) == 0) stop("games_<round>.csv is empty. Run Script 1 and confirm game mapping.")
if (nrow(picks) == 0) stop("No spread/total picks found in picks_clean_<round>.csv.")

# If Script 1 now provides pick_norm, use it. Otherwise fall back to a simple normalization.
if (!("pick_norm" %in% names(picks))) {
  picks <- picks |>
    mutate(
      pick_norm = pick_value |>
        str_replace_all("\u00A0", " ") |>
        str_squish() |>
        str_to_lower()
    )
}

# Build answer choices (dedupe by normalized value, keep one display pick_value)
choices_long <- picks |>
  mutate(
    pick_value = pick_value |>
      str_replace_all("\u00A0", " ") |>
      str_squish()
  ) |>
  distinct(season, round, game_id, market, pick_norm, .keep_all = TRUE) |>
  arrange(game_id, market, pick_value) |>
  group_by(season, round, game_id, market) |>
  mutate(option_id = row_number()) |>
  ungroup() |>
  select(season, round, game_id, market, option_id, pick_value)

choices_wide <- choices_long |>
  pivot_wider(
    names_from = option_id,
    values_from = pick_value,
    names_prefix = "option_"
  )

# Start template (include game_label for readability)
results_template <- games |>
  select(season, round, game_id, game_label) |>
  expand_grid(market = c("spread", "total")) |>
  left_join(choices_wide, by = c("season", "round", "game_id", "market")) |>
  arrange(game_id, market) |>
  mutate(correct_pick = NA_character_) |>
  select(
    season, round, game_id, game_label, market,
    correct_pick,
    starts_with("option_")
  )

# Preserve existing correct_pick if file exists
if (file.exists(results_path)) {
  old <- read_csv(results_path, show_col_types = FALSE) |>
    mutate(
      season  = as.character(season),
      round   = as.character(round),
      game_id = as.character(game_id)
    )
  
  results_template <- results_template |>
    left_join(
      old |>
        select(season, round, game_id, market, correct_pick) |>
        rename(correct_pick_old = correct_pick),
      by = c("season", "round", "game_id", "market")
    ) |>
    mutate(correct_pick = coalesce(correct_pick_old, correct_pick)) |>
    select(-correct_pick_old)
}

write_csv(results_template, results_path)
