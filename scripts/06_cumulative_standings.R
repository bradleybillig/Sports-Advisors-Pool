# ============================================================
# Script 6: Cumulative Standings (Season-to-Date Leaderboard)
#
# Purpose:
#   Builds season-to-date standings across COMPLETED playoff rounds
#   using the authoritative outputs from Script 4:
#     <round> out/weekly_totals_with_locks_<round>.csv
#
# How to use (each time you want an updated cumulative leaderboard):
#   1) Run Scripts 1–4 for each round that is completed.
#      The key file this script looks for is:
#        <round> out/weekly_totals_with_locks_<round>.csv
#   2) Put this script at the project root (same level as "wc out/", "dr out/", etc).
#   3) Set season_val (and optionally round_order).
#   4) Run the script.
#
# Outputs (standings/):
#   - standings_cumulative.csv   (full data + audit fields)
#   - standings_cumulative.html  (formatted table)
#
# Key behaviors:
#   - Auto-detects completed rounds by checking for each round’s
#     weekly_totals_with_locks_<round>.csv file (stops at first missing).
#   - Normalizes emails to avoid duplicates from case/whitespace.
#   - Uses consistent tiebreak ordering for the full leaderboard:
#       Points, Overall Win %, WC Win %, DR Win %, CON Win %, SB spread correct, then alphabetical.
#   - Flags last-place payout eligibility: must submit picks for every completed round.
#   - Highlights podium (rows 1–3) and the eligible last-place payout winner.
#   - Adds tie notes using symbols in order (*, †, ‡, §) for ties affecting
#     1st, 2nd, 3rd, and eligible last-place payout (if they exist).
#
# Suggested filename: 06_cumulative_standings.R
# ============================================================

library(readr)
library(dplyr)
library(stringr)
library(tidyr)
library(gt)
library(glue)
library(purrr)
library(tibble)

# ----------------------------
# CONFIG
# ----------------------------
season_val  <- 2026
round_order <- c("wc", "dr", "con", "sb")

# Choose which rounds to include:
# - "auto"        -> include all completed rounds in order (stops at first missing)
# - character vec -> include ONLY these rounds (e.g., c("wc","dr") or c("wc","dr","con"))
rounds_to_include <- c("wc","dr")

# Standings output directory (relative to project root)
standings_dir_name <- "standings"

# ----------------------------
# PROJECT ROOT + ROUND FOLDER MAP
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
  if (!rc %in% names(map)) stop("Unknown round code: ", rc)
  unname(map[[rc]])
}

root_dir <- project_root()
standings_dir <- file.path(root_dir, standings_dir_name)
dir.create(standings_dir, showWarnings = FALSE, recursive = TRUE)

# ----------------------------
# DETECT / SELECT ROUNDS
# ----------------------------
totals_path_for_round <- function(rc) {
  file.path(
    root_dir, "out", round_folder(rc),
    sprintf("weekly_totals_with_locks_%s.csv", rc)
  )
}

# Auto-detect completed rounds (consecutive only)
auto_completed_rounds <- character()
for (rc in round_order) {
  p <- totals_path_for_round(rc)
  if (file.exists(p)) auto_completed_rounds <- c(auto_completed_rounds, rc) else break
}

if (length(auto_completed_rounds) == 0) {
  stop("No completed rounds detected. Expected at least: ", totals_path_for_round("wc"))
}

# Final included rounds based on your setting
included_rounds <- if (identical(rounds_to_include, "auto")) {
  auto_completed_rounds
} else {
  rounds_to_include <- tolower(rounds_to_include)
  bad <- setdiff(rounds_to_include, round_order)
  if (length(bad) > 0) stop("Invalid rounds_to_include: ", paste(bad, collapse = ", "))
  
  # Keep only rounds that actually have totals present
  present <- rounds_to_include[file.exists(vapply(rounds_to_include, totals_path_for_round, character(1)))]
  if (length(present) == 0) stop("None of the selected rounds have totals files present.")
  present
}

message("Rounds included: ", paste(included_rounds, collapse = ", "))


# ----------------------------
# HELPERS
# ----------------------------
round_label <- function(rc) {
  dplyr::case_when(
    rc == "wc"  ~ "Wild Card",
    rc == "dr"  ~ "Divisional",
    rc == "con" ~ "Conference",
    rc == "sb"  ~ "Super Bowl",
    TRUE ~ toupper(rc)
  )
}

safe_div <- function(num, den) ifelse(den > 0, num / den, NA_real_)

# Normalize emails to prevent duplicates from case/whitespace
norm_email <- function(x) str_to_lower(str_trim(as.character(x)))

# Nicer fallback label when no name exists anywhere
pretty_email_handle <- function(email) {
  h <- str_remove(email, "@.*")
  h <- str_replace_all(h, "[._-]+", " ")
  h <- str_squish(h)
  str_to_title(h)
}

# Tiebreak stack (same order you use to sort the whole list)
tiebreak_fields <- c(
  "win_pct_overall",
  "win_pct_wc",
  "win_pct_dr",
  "win_pct_con",
  "sb_spread_correct",
  "player"
)

tiebreak_labels <- c(
  win_pct_overall    = "Overall Win %",
  win_pct_wc         = "Wild Card Win %",
  win_pct_dr         = "Divisional Win %",
  win_pct_con        = "Conference Win %",
  sb_spread_correct  = "Super Bowl spread correct",
  player             = "Player (alphabetical)"
)

# Labels specifically for LAST-place tie note direction (lower values = worse)
tiebreak_labels_last <- c(
  win_pct_overall    = "Overall Win % (lower)",
  win_pct_wc         = "Wild Card Win % (lower)",
  win_pct_dr         = "Divisional Win % (lower)",
  win_pct_con        = "Conference Win % (lower)",
  sb_spread_correct  = "Super Bowl spread correct (lower)",
  player             = "Player (reverse alphabetical)"
)

# Which tiebreaker field actually separates a tied group?
first_separator <- function(df_group, fields = tiebreak_fields) {
  for (f in fields) {
    v <- df_group[[f]]
    if (all(is.na(v))) next
    if (length(unique(v[!is.na(v)])) > 1) return(f)
  }
  NA_character_
}

extract_rank_num <- function(x) as.integer(str_extract(x, "\\d+"))

ordinal_suffix <- function(n) {
  if (n == 1) "st" else if (n == 2) "nd" else if (n == 3) "rd" else "th"
}

# ----------------------------
# LOAD PER-ROUND TOTALS
# ----------------------------
need_cols <- c("email", "base_points", "wins", "losses", "lock_hits", "lock_points", "total_points", "season")

round_totals <- purrr::map_dfr(completed_rounds, function(rc) {
  out_dir <- sprintf("%s out", rc)
  path <- file.path(out_dir, sprintf("weekly_totals_with_locks_%s.csv", rc))
  if (!file.exists(path)) stop("Missing file: ", path)
  
  df <- read_csv(path, show_col_types = FALSE, col_select = any_of(need_cols))
  
  # Season consistency: if the per-round file contains a season column, filter it.
  if ("season" %in% names(df)) {
    df <- df |>
      mutate(season = suppressWarnings(as.integer(season))) |>
      filter(is.na(season) | season == season_val)
  }
  
  df |>
    mutate(
      email_raw = email,
      email     = norm_email(email),
      round     = rc,
      base_points  = coalesce(base_points, 0),
      wins         = coalesce(wins, 0),
      losses       = coalesce(losses, 0),
      lock_hits    = coalesce(lock_hits, 0),
      lock_points  = coalesce(lock_points, 0),
      total_points = coalesce(total_points, 0)
    )
})

needed <- c("email", "round", "base_points", "wins", "losses", "lock_hits", "lock_points", "total_points")
missing_cols <- setdiff(needed, names(round_totals))
if (length(missing_cols) > 0) {
  stop("Missing columns in weekly_totals_with_locks files: ", paste(missing_cols, collapse = ", "))
}

# Audit: detect multiple raw emails mapping to one normalized email (usually whitespace/case differences)
email_collisions <- round_totals |>
  distinct(email, email_raw) |>
  count(email, name = "n_raw") |>
  filter(n_raw > 1)

if (nrow(email_collisions) > 0) {
  collision_out <- file.path(standings_dir, "email_collisions.csv")
  write_csv(email_collisions, collision_out)
  message("WARNING: Some normalized emails map to multiple raw emails. See: ", collision_out)
}

# ----------------------------
# OPTIONAL: LOAD NAMES (from ANY completed round that has a players file)
# ----------------------------
players <- purrr::map_dfr(completed_rounds, function(rc) {
  p <- file.path(sprintf("%s out", rc), sprintf("players_%s.csv", rc))
  if (!file.exists(p)) return(tibble(email = character(), name = character()))
  
  read_csv(p, show_col_types = FALSE, col_select = any_of(c("email", "name"))) |>
    transmute(
      email = norm_email(email),
      name  = na_if(str_trim(name), "")
    )
}) |>
  group_by(email) |>
  summarize(
    name = name[which(!is.na(name) & name != "")][1],
    .groups = "drop"
  )

# ----------------------------
# CUMULATIVE TOTALS (for display)
# ----------------------------
cum <- round_totals |>
  group_by(email) |>
  summarize(
    points      = sum(total_points, na.rm = TRUE),
    base_points = sum(base_points, na.rm = TRUE),
    wins        = sum(wins, na.rm = TRUE),
    losses      = sum(losses, na.rm = TRUE),
    lock_hits   = sum(lock_hits, na.rm = TRUE),
    lock_points = sum(lock_points, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    win_pct_overall = safe_div(wins, wins + losses)
  )

# ----------------------------
# ROUND-SPECIFIC WIN% TIEBREAKERS (WC, DR, CON)
# ----------------------------
round_winpct <- round_totals |>
  mutate(win_pct_round = safe_div(wins, wins + losses)) |>
  select(email, round, win_pct_round) |>
  mutate(round = factor(round, levels = round_order)) |>
  pivot_wider(names_from = round, values_from = win_pct_round)

# Ensure columns exist even if a round not completed yet
for (rc in c("wc", "dr", "con")) {
  if (!rc %in% names(round_winpct)) round_winpct[[rc]] <- NA_real_
}

round_winpct <- round_winpct |>
  rename(
    win_pct_wc  = wc,
    win_pct_dr  = dr,
    win_pct_con = con
  )

# ----------------------------
# SUPER BOWL SPREAD CORRECT TIEBREAKER (only if SB completed)
# ----------------------------
sb_spread_correct <- tibble(email = character(), sb_spread_correct = NA_integer_)

if ("sb" %in% completed_rounds) {
  sb_scored_path <- file.path("sb out", "scored_picks_sb.csv")
  if (file.exists(sb_scored_path)) {
    sb_scored <- read_csv(sb_scored_path, show_col_types = FALSE) |>
      mutate(email = norm_email(email))
    
    sb_spread_correct <- sb_scored |>
      filter(market == "spread") |>
      group_by(email) |>
      summarize(
        sb_spread_correct = as.integer(any(is_win %in% TRUE)),
        .groups = "drop"
      )
  } else {
    sb_spread_correct <- cum |>
      transmute(email, sb_spread_correct = NA_integer_)
  }
} else {
  sb_spread_correct <- cum |>
    transmute(email, sb_spread_correct = NA_integer_)
}

# ----------------------------
# PER-ROUND POINT BREAKOUT (nice columns)
# ----------------------------
round_points_wide <- round_totals |>
  select(email, round, total_points) |>
  mutate(round = factor(round, levels = round_order)) |>
  pivot_wider(names_from = round, values_from = total_points, values_fill = 0)

# ----------------------------
# ELIGIBILITY: last-place payout requires submitting picks every completed round
# ----------------------------
eligibility <- round_totals |>
  distinct(email, round) |>
  group_by(email) |>
  summarize(
    rounds_submitted = n_distinct(round),
    missing_rounds = {
      missed <- setdiff(completed_rounds, unique(round))
      paste(vapply(missed, round_label, character(1)), collapse = ", ")
    },
    ineligible_last_place = rounds_submitted < length(completed_rounds),
    .groups = "drop"
  ) |>
  mutate(
    missing_rounds = coalesce(missing_rounds, "")
  )

# ----------------------------
# BUILD FINAL TABLE WITH TIEBREAKERS
# ----------------------------
standings <- cum |>
  left_join(round_winpct, by = "email") |>
  left_join(sb_spread_correct, by = "email") |>
  left_join(round_points_wide, by = "email") |>
  left_join(players, by = "email") |>
  left_join(eligibility, by = "email") |>
  mutate(
    player = case_when(
      !is.na(name) & name != "" ~ name,
      TRUE ~ pretty_email_handle(email)
    ),
    ineligible_last_place = coalesce(ineligible_last_place, TRUE),
    missing_rounds = coalesce(missing_rounds, "")
  )

# ----------------------------
# APPLY YOUR TIEBREAK ORDER (whole list)
# Rank display: golf-style ties based on POINTS (T1, T1, then 3)
# ----------------------------
standings_ordered <- standings |>
  arrange(
    desc(points),
    desc(win_pct_overall),
    desc(win_pct_wc),
    desc(win_pct_dr),
    desc(win_pct_con),
    desc(coalesce(sb_spread_correct, -1L)),
    player,
    email
  ) |>
  mutate(
    rank_num = min_rank(desc(points)),
    is_tied  = duplicated(rank_num) | duplicated(rank_num, fromLast = TRUE),
    rank     = if_else(is_tied, paste0("T", rank_num), as.character(rank_num))
  ) |>
  select(-rank_num, -is_tied)

# ----------------------------
# TIE NOTES WITH SYMBOLS (in order): 1st, 2nd, 3rd, Last (eligible payout)
# Symbols used in order: *, †, ‡, §
# ----------------------------
symbols <- c("*", "†", "‡", "§")
all_notes_text <- character()

standings_ordered <- standings_ordered |>
  mutate(rank_num_display = extract_rank_num(rank))

tie_groups_places <- standings_ordered |>
  group_by(points) |>
  filter(n() > 1) |>
  summarize(
    min_place = min(rank_num_display, na.rm = TRUE),
    .groups = "drop"
  )

tie_points_1 <- tie_groups_places |> filter(min_place == 1) |> pull(points)
tie_points_2 <- tie_groups_places |> filter(min_place == 2) |> pull(points)
tie_points_3 <- tie_groups_places |> filter(min_place == 3) |> pull(points)

eligible_pool <- standings_ordered |> filter(ineligible_last_place == FALSE)

eligible_min_points <- if (nrow(eligible_pool) > 0) min(eligible_pool$points, na.rm = TRUE) else NA_real_
last_group_eligible <- if (!is.na(eligible_min_points)) {
  eligible_pool |> filter(points == eligible_min_points)
} else {
  eligible_pool[0, ]
}
last_is_tie <- nrow(last_group_eligible) > 1

note_specs <- list()
if (length(tie_points_1) > 0) note_specs <- append(note_specs, list(list(kind = "place", place = 1, points = unique(tie_points_1))))
if (length(tie_points_2) > 0) note_specs <- append(note_specs, list(list(kind = "place", place = 2, points = unique(tie_points_2))))
if (length(tie_points_3) > 0) note_specs <- append(note_specs, list(list(kind = "place", place = 3, points = unique(tie_points_3))))
if (last_is_tie)                note_specs <- append(note_specs, list(list(kind = "last",  points = eligible_min_points)))

if (length(note_specs) > 0) {
  for (i in seq_along(note_specs)) {
    spec <- note_specs[[i]]
    sym <- symbols[i]
    
    if (spec$kind == "place") {
      plc <- spec$place
      pts <- spec$points[1]
      
      grp <- standings_ordered |> filter(points == pts)
      sep_field <- first_separator(grp)
      
      standings_ordered <- standings_ordered |>
        mutate(
          rank = if_else(points == pts & str_detect(rank, "^T"),
                         paste0(rank, sym),
                         rank)
        )
      
      note_line <- paste0(
        sym, " Ties for ", plc, ordinal_suffix(plc),
        " place at ", pts, " points are broken by ",
        if (is.na(sep_field)) "no tiebreaker difference found (check data)"
        else unname(tiebreak_labels[sep_field]),
        "."
      )
      
      all_notes_text <- c(all_notes_text, note_line)
      
    } else if (spec$kind == "last") {
      pts <- spec$points[1]
      sep_field <- first_separator(last_group_eligible)
      
      standings_ordered <- standings_ordered |>
        mutate(
          rank = if_else(ineligible_last_place == FALSE &
                           points == pts &
                           str_detect(rank, "^T"),
                         paste0(rank, sym),
                         rank)
        )
      
      note_line <- paste0(
        sym, " Ties for last-place payout at ", pts,
        " points are ordered by ",
        if (is.na(sep_field)) "no tiebreaker difference found (check data)"
        else unname(tiebreak_labels_last[sep_field]),
        "."
      )
      
      all_notes_text <- c(all_notes_text, note_line)
    }
  }
}

standings_ordered <- standings_ordered |> select(-rank_num_display)

# ----------------------------
# LAST-PLACE PAYOUT WINNER (eligible-only, same tiebreakers reversed)
# ----------------------------
eligible_last_order <- standings_ordered |>
  filter(ineligible_last_place == FALSE) |>
  arrange(
    points,                      # low points = worse
    win_pct_overall,
    win_pct_wc,
    win_pct_dr,
    win_pct_con,
    coalesce(sb_spread_correct, -1L),
    desc(player),                # reverse alpha = worse
    desc(email)
  )

last_place_winner_email <- if (nrow(eligible_last_order) > 0) eligible_last_order$email[1] else NA_character_

# ----------------------------
# EXPORT CSV
# ----------------------------
csv_out <- file.path(standings_dir, "standings_cumulative.csv")
write_csv(standings_ordered, csv_out)

# ----------------------------
# PRETTY HTML
# ----------------------------
round_cols_present <- intersect(round_order, names(standings_ordered))
round_labels <- setNames(
  paste0("Pts: ", vapply(round_cols_present, round_label, character(1))),
  round_cols_present
)

# Keep email hidden so we can style rows reliably if needed later
html_table <- standings_ordered |>
  mutate(row_id = row_number()) |>
  select(
    row_id, email,  # keep for styling, hide later
    rank, player,
    points, base_points,
    wins, losses, win_pct_overall,
    lock_hits, lock_points,
    all_of(round_cols_present),
    ineligible_last_place,
    missing_rounds
  )

standings_gt <- html_table |>
  gt() |>
  tab_header(
    title = "Cumulative Standings",
    subtitle = glue("Season {season_val} through {round_label(tail(completed_rounds, 1))}")
  ) |>
  cols_label(
    rank = "Rank",
    player = "Player",
    points = "Points",
    base_points = "Base Points",
    wins = "W",
    losses = "L",
    win_pct_overall = "Win %",
    lock_hits = "Lock Hits",
    lock_points = "Lock Pts"
  ) |>
  cols_label(.list = round_labels) |>
  cols_hide(columns = c(row_id, email, ineligible_last_place, missing_rounds)) |>
  
  # Highlight ONLY: top 3 rows + last-place payout winner
  tab_style(
    style = cell_fill(color = "#C8E6C9"),
    locations = cells_body(rows = row_id == 1)
  ) |>
  tab_style(
    style = cell_fill(color = "#E6EE9C"),
    locations = cells_body(rows = row_id == 2)
  ) |>
  tab_style(
    style = cell_fill(color = "#FFE082"),
    locations = cells_body(rows = row_id == 3)
  ) |>
  tab_style(
    style = cell_fill(color = "#FFCDD2"),
    locations = cells_body(rows = email == last_place_winner_email)
  ) |>
  
  # Ineligible players: red name text (no row fill)
  tab_style(
    style = cell_text(color = "red"),
    locations = cells_body(
      columns = player,
      rows = ineligible_last_place == TRUE
    )
  ) |>
  
  cols_align(
    align = "center",
    columns = everything()
  ) |>
  cols_width(
    player ~ px(150),
    rank ~ px(60),
    points ~ px(70),
    base_points ~ px(90),
    wins ~ px(45),
    losses ~ px(45),
    win_pct_overall ~ px(70),
    lock_hits ~ px(70),
    lock_points ~ px(70),
    all_of(round_cols_present) ~ px(90)
  ) |>
  fmt_number(columns = c(points, base_points, lock_points), decimals = 0) |>
  fmt_percent(columns = win_pct_overall, decimals = 1) |>
  tab_style(
    style = list(cell_text(weight = "bold")),
    locations = cells_column_labels(everything())
  ) |>
  tab_options(
    table.font.size = px(13),
    data_row.padding = px(3)
  )

# ----------------------------
# FOOTNOTES
# - tie notes: one line per symbol (*, †, ‡, §)
# - then ineligible notes
# ----------------------------
if (length(all_notes_text) > 0) {
  for (note_line in all_notes_text) {
    standings_gt <- standings_gt |>
      tab_source_note(source_note = note_line)
  }
}

ineligible_names <- html_table |>
  filter(ineligible_last_place) |>
  pull(player)

if (length(ineligible_names) > 0) {
  standings_gt <- standings_gt |>
    tab_source_note(
      source_note = "Red player names are ineligible for the last-place payout because they missed submitting picks for at least one completed round."
    )
  
  detail <- html_table |>
    filter(ineligible_last_place) |>
    transmute(x = paste0(player, " (missed: ", missing_rounds, ")")) |>
    pull(x)
  
  if (length(detail) > 0) {
    detail <- detail[seq_len(min(length(detail), 8))]
    standings_gt <- standings_gt |>
      tab_source_note(source_note = paste("Ineligible:", paste(detail, collapse = "; ")))
  }
}

# ----------------------------
# SAVE HTML
# ----------------------------
html_out <- file.path(standings_dir, "standings_cumulative.html")
gtsave(standings_gt, html_out)

message("Wrote: ", csv_out)
message("Wrote: ", html_out)
