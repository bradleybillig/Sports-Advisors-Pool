#### Script 5: Build Weekly Results HTML (No Recompute) ####
# ============================================================
# Purpose:
#   Create an HTML “Weekly Pick Results” table that shows:
#     - Leaderboard (points, W-L, win%, lock points)
#     - Per-game cells: pick + ✅/❌ + 🔒 tags for matched locks
#     - Conference only: one "Tiebreaker" column AFTER game columns
#
# Notes:
#   - No recompute: reads scored_picks / lock_scoring / weekly_totals outputs.
#   - If results_<round>.csv exists, used only for stable game column ordering.
#   - We do NOT rename wide columns; we label via cols_label().
# ============================================================

library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(gt)
library(glue)

# ----------------------------
# CONFIG
# ----------------------------
round_code <- "con"  # "wc", "dr", "con", "sb"

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
message("Using out_dir: ", out_dir)

lock_style <- "plain"
use_results_for_order <- TRUE

# ----------------------------
# HELPERS
# ----------------------------
pretty_label_from_game <- function(game_label) {
  x <- game_label |>
    str_replace_all("_", " ") |>
    str_squish() |>
    str_to_title()

  # Force acronyms back to uppercase
  x <- str_replace_all(x, "\\bAfc\\b", "AFC")
  x <- str_replace_all(x, "\\bNfc\\b", "NFC")
  x
}

safe_div <- function(num, den) ifelse(den > 0, num / den, NA_real_)
norm_email  <- function(x) str_to_lower(str_trim(x))
norm_market <- function(x) str_to_lower(str_trim(x))
norm_gameid <- function(x) as.character(x)

norm_pick <- function(x) {
  x |>
    str_replace_all("\u00A0", " ") |>
    str_squish() |>
    str_trim()
}

# ----------------------------
# LOAD DATA (authoritative outputs only)
# ----------------------------
scored_picks <- read_csv(
  file.path(out_dir, sprintf("scored_picks_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  mutate(
    email      = norm_email(email),
    game_id    = norm_gameid(game_id),
    market     = norm_market(market),
    pick_value = norm_pick(pick_value),
    is_win     = as.logical(is_win)
  )

lock_scoring <- read_csv(
  file.path(out_dir, sprintf("lock_scoring_%s.csv", round_code)),
  show_col_types = FALSE
)

weekly_totals <- read_csv(
  file.path(out_dir, sprintf("weekly_totals_with_locks_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  mutate(email = norm_email(email))

games <- read_csv(
  file.path(out_dir, sprintf("games_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  mutate(game_id = norm_gameid(game_id))

players <- read_csv(
  file.path(out_dir, sprintf("players_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  transmute(
    email = norm_email(email),
    name  = na_if(str_trim(name), "")
  )

# Ensure TB fields exist (Conference only)
if (round_code != "con") {
  weekly_totals <- weekly_totals |>
    mutate(
      tiebreaker_guess = NA_real_,
      tiebreaker_valid = NA,
      tiebreaker_diff  = NA_real_
    )
} else {
  if (!("tiebreaker_diff" %in% names(weekly_totals))) {
    weekly_totals <- weekly_totals |>
      mutate(
        tiebreaker_guess = NA_real_,
        tiebreaker_valid = NA,
        tiebreaker_diff  = NA_real_
      )
  }
}

# Optional: used only for stable column ordering
results_path <- file.path(out_dir, sprintf("results_%s.csv", round_code))
results_order <- NULL
if (use_results_for_order && file.exists(results_path)) {
  results_order <- read_csv(results_path, show_col_types = FALSE) |>
    mutate(
      game_id = norm_gameid(game_id),
      market  = norm_market(market)
    ) |>
    filter(market %in% c("spread", "total")) |>
    mutate(order_row = row_number()) |>
    select(order_row, game_id, market)
}

# ----------------------------
# LOCK SCORING NORMALIZATION (no recompute, just keys)
# ----------------------------
matched_pick_col <- intersect(
  names(lock_scoring),
  c("matched_pick", "matched_pick_value", "matched_value", "matched_pick_text", "matched_selection")
)

if (length(matched_pick_col) == 0) {
  stop(
    "lock_scoring is missing a matched pick column. Expected one of: ",
    "matched_pick, matched_pick_value, matched_value, matched_pick_text, matched_selection"
  )
}
matched_pick_col <- matched_pick_col[1]

lock_scoring <- lock_scoring |>
  mutate(
    email             = norm_email(email),
    game_id           = norm_gameid(matched_game_id),
    market            = norm_market(matched_market),
    matched           = as.logical(matched),
    matched_pick_norm = norm_pick(.data[[matched_pick_col]])
  )

# ----------------------------
# GAME HEADERS (FIXED)
# Build titles strictly from games table: game_id -> game_label -> pretty
# ----------------------------
game_headers <- games |>
  transmute(
    game_id,
    game_title = pretty_label_from_game(game_label)
  )

# ----------------------------
# LOCK TAGS (keyed by specific pick, not just game+market)
# ----------------------------
lock_tags <- lock_scoring |>
  filter(matched) |>
  filter(
    !is.na(email), email != "",
    !is.na(game_id), game_id != "",
    !is.na(market), market != "",
    !is.na(matched_pick_norm), matched_pick_norm != ""
  ) |>
  mutate(
    lock_order = case_when(
      lock_slot %in% c("bonus_lock_1", 1, "1") ~ 1L,
      lock_slot %in% c("bonus_lock_2", 2, "2") ~ 2L,
      TRUE ~ 99L
    ),
    lock_tag_piece = case_when(
      lock_style == "plain" ~ " 🔒",
      lock_slot %in% c("bonus_lock_1", 1, "1") ~ " 🔒1",
      lock_slot %in% c("bonus_lock_2", 2, "2") ~ " 🔒2",
      TRUE ~ ""
    )
  ) |>
  arrange(email, game_id, market, matched_pick_norm, lock_order) |>
  group_by(email, game_id, market, matched_pick_norm) |>
  summarize(lock_tag = paste0(lock_tag_piece, collapse = ""), .groups = "drop") |>
  rename(pick_value = matched_pick_norm)

# ----------------------------
# BUILD WEEKLY CELLS (pick + ✅/❌ + lock tag)
# ----------------------------
weekly_cells <- scored_picks |>
  left_join(game_headers, by = "game_id") |>
  left_join(lock_tags,   by = c("email", "game_id", "market", "pick_value")) |>
  mutate(
    col_key    = paste0(game_id, "__", market),
    col_header = paste0(game_title, " (", str_to_title(market), ")"),
    win_icon   = if_else(coalesce(is_win, FALSE), " ✅", " ❌"),
    cell       = paste0(coalesce(pick_value, ""), win_icon, coalesce(lock_tag, ""))
  ) |>
  select(email, col_key, col_header, cell) |>
  filter(!is.na(email), email != "")

# ----------------------------
# STABLE COLUMN PLAN
# ----------------------------
if (!is.null(results_order)) {
  col_plan <- results_order |>
    left_join(game_headers, by = "game_id") |>
    mutate(
      col_key    = paste0(game_id, "__", market),
      col_header = paste0(game_title, " (", str_to_title(market), ")")
    ) |>
    arrange(order_row) |>
    select(col_key, col_header)
} else {
  col_plan <- weekly_cells |>
    distinct(col_key, col_header) |>
    arrange(col_header)
}

col_keys <- col_plan$col_key
game_labels <- setNames(col_plan$col_header, col_plan$col_key)

# ----------------------------
# WIDE TABLE OF CELLS
# ----------------------------
weekly_wide <- weekly_cells |>
  distinct(email, col_key, cell) |>
  pivot_wider(
    id_cols = email,
    names_from = col_key,
    values_from = cell,
    values_fill = ""
  )

missing_cols <- setdiff(col_keys, names(weekly_wide))
if (length(missing_cols) > 0) weekly_wide[missing_cols] <- ""
weekly_wide <- weekly_wide |>
  select(email, all_of(col_keys))

# ----------------------------
# LEADERBOARD (use totals file, no recompute)
# Conference ordering includes TB validity + diff
# ----------------------------
summary <- weekly_totals |>
  mutate(
    lock_points  = coalesce(lock_points, 0),
    total_points = coalesce(total_points, 0),
    wins   = coalesce(wins, 0),
    losses = coalesce(losses, 0)
  ) |>
  left_join(players, by = "email") |>
  mutate(
    player   = coalesce(name, email),
    record   = paste0(wins, "-", losses),
    win_pct  = safe_div(wins, wins + losses),
    points   = total_points,
    lock_pts = lock_points
  ) |>
  arrange(
    desc(points),
    desc(wins),
    desc(coalesce(tiebreaker_valid, FALSE)),
    coalesce(tiebreaker_diff, Inf),
    player
  ) |>
  mutate(
    rank = if (round_code == "con") as.character(row_number())
           else {
             rn <- min_rank(desc(points))
             tied <- duplicated(rn) | duplicated(rn, fromLast = TRUE)
             if_else(tied, paste0("T", rn), as.character(rn))
           }
  ) |>
  transmute(
    email, player, rank,
    points, record, win_pct, lock_pts,
    tiebreaker_guess, tiebreaker_valid, tiebreaker_diff
  )

# ----------------------------
# ONE DISPLAY COLUMN: "Tiebreaker" (after games)
# ----------------------------
summary <- summary |>
  mutate(
    tiebreaker_display = case_when(
      round_code != "con" ~ NA_character_,
      is.na(tiebreaker_guess) ~ "",
      isTRUE(tiebreaker_valid) ~ paste0(as.integer(round(tiebreaker_guess)), " (OK, diff ", as.integer(round(tiebreaker_diff)), ")"),
      isFALSE(tiebreaker_valid) ~ paste0(as.integer(round(tiebreaker_guess)), " (OVER)"),
      TRUE ~ paste0(as.integer(round(tiebreaker_guess)))
    )
  )

# ----------------------------
# FINAL TABLE DATA
# Put "Tiebreaker" AFTER game columns
# ----------------------------
weekly_results <- summary |>
  left_join(weekly_wide, by = "email") |>
  mutate(
    # TRUE means "overbid" (invalid). Treat NA as not-over.
    tb_is_over = (round_code == "con") & (coalesce(tiebreaker_valid, TRUE) == FALSE)
  ) |>
  select(-email) |>
  select(
    player, rank, points, record, win_pct, lock_pts,
    all_of(col_keys),
    tiebreaker_display,
    tb_is_over
  )


# ----------------------------
# GT TABLE
# ----------------------------
game_labels_in_table <- game_labels[names(game_labels) %in% names(weekly_results)]

weekly_gt <- weekly_results |>
  gt() |>
  tab_header(
    title = "Weekly Pick Results",
    subtitle = glue("Round: {toupper(round_code)}")
  ) |>
  cols_label(
    player  = "Player",
    rank    = "Rank",
    points  = "Points",
    record  = "W-L",
    win_pct = "Win %",
    lock_pts = "Lock Pts",
    tiebreaker_display = "Tiebreaker"
  ) |>
  cols_label(.list = as.list(game_labels_in_table)) |>
  cols_hide(columns = "tb_is_over") |>
  fmt_number(columns = c(points, lock_pts), decimals = 0) |>
  fmt_percent(columns = win_pct, decimals = 1) |>
  cols_align(align = "center", columns = everything()) |>
  cols_align(align = "left", columns = player) |>
  cols_width(
    player ~ px(220),
    rank ~ px(70),
    points ~ px(70),
    record ~ px(70),
    win_pct ~ px(80),
    lock_pts ~ px(80),
    everything() ~ px(150),
    tiebreaker_display ~ px(180)
  ) |>
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels(everything())
  ) |>
  # Highlight top 3 rows (rank values 1/2/3)
  tab_style(
    style = cell_fill(color = "#C8E6C9"),
    locations = cells_body(rows = as.integer(str_remove(rank, "^T")) == 1)
  ) |>
  tab_style(
    style = cell_fill(color = "#E6EE9C"),
    locations = cells_body(rows = as.integer(str_remove(rank, "^T")) == 2)
  ) |>
  tab_style(
    style = cell_fill(color = "#FFE082"),
    locations = cells_body(rows = as.integer(str_remove(rank, "^T")) == 3)
  ) |>
  # Make tiebreaker red if OVER (conference only)
  tab_style(
    style = cell_text(color = "red", weight = "bold"),
    locations = cells_body(
      columns = "tiebreaker_display",
      rows = tb_is_over
    )
  )|>
  tab_options(
    table.font.size = px(12),
    data_row.padding = px(2)
  )


# ----------------------------
# EXPORT
# ----------------------------
out_html <- file.path(out_dir, sprintf("weekly_results_%s.html", round_code))
gtsave(weekly_gt, out_html)
message("Wrote: ", out_html)

# ----------------------------
# SANITY CHECKS
# ----------------------------
message("Pick rows graded: ", nrow(scored_picks))
message("Locks matched rows: ", sum(lock_scoring$matched, na.rm = TRUE), " / ", nrow(lock_scoring))
message("Lock tag rows: ", nrow(lock_tags))
