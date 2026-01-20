#### Create the Round Picks Sheet (Pre-Results) ####
# ============================================================
# Script 3: Build Follow-Along Picks Sheet (HTML)
#
# Purpose:
#   Create an HTML table showing every player's picks (spread + total) before grading,
#   with lock markers on matched locked picks, and raw Lock 1/Lock 2 text at the end.
#
# Required inputs (from Script 1, inside out_dir):
#   - picks_clean_<round>.csv   (pick_value + pick_norm preferred)
#   - games_<round>.csv
#   - players_<round>.csv
#
# Optional input (from Script 2, inside out_dir):
#   - results_<round>.csv       (used only to drive stable column order)
#
# Output:
#   - round_picks_<round>.html
#
# Shared helpers:
#   This script expects you created:
#     R/lock_matching_helpers.R
#   and will source() it for the lock matching logic.
# ============================================================

library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(tibble)
library(gt)
library(glue)

# ----------------------------
# CONFIG
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


# "plain" -> 🔒 , "numbered" -> 🔒1 / 🔒2
lock_style <- "plain"

# Canonical lock markets from Script 1
lock_markets <- c("bonus_lock_1", "bonus_lock_2")

# Use results_<round>.csv to drive column ordering if present
use_results_for_order <- TRUE

# ----------------------------
# SOURCE SHARED LOCK HELPERS
# ----------------------------
helper_path <- file.path(root_dir, "scripts", "lock_matching_helpers.R")

if (!file.exists(helper_path)) {
  stop("Could not find lock helper at: ", helper_path)
}

source(helper_path)

# ----------------------------
# Local helpers (display only)
# ----------------------------
pretty_label_from_game <- function(game_label) {
  x <- game_label |>
    str_replace_all("_", " ") |>
    str_squish() |>
    str_to_title()
  
  # Preserve common NFL acronyms that Title Case breaks
  x <- x |>
    str_replace_all("\\bAfc\\b", "AFC") |>
    str_replace_all("\\bNfc\\b", "NFC") |>
    str_replace_all("\\bNfl\\b", "NFL")
  
  x
}


escape_html <- function(x) htmltools::htmlEscape(dplyr::coalesce(x, ""), attribute = FALSE)

strip_markers <- function(x) {
  x %||% "" |>
    str_replace_all("🔒\\s*\\d*", "") |>
    str_replace_all("⚠️", "") |>
    str_squish()
}

# ----------------------------
# LOAD DATA
# ----------------------------
picks_clean <- read_csv(file.path(out_dir, sprintf("picks_clean_%s.csv", round_code)), show_col_types = FALSE) |>
  mutate(
    season = as.character(season),
    round  = as.character(round),
    game_id = as.character(game_id)
  )

# Only include lock 2 in rounds where it exists in the data
lock_markets <- c("bonus_lock_1")
if (any(picks_clean$market == "bonus_lock_2", na.rm = TRUE)) {
  lock_markets <- c("bonus_lock_1", "bonus_lock_2")
}

games <- read_csv(file.path(out_dir, sprintf("games_%s.csv", round_code)), show_col_types = FALSE) |>
  mutate(
    season = as.character(season),
    round  = as.character(round),
    game_id = as.character(game_id)
  )

players <- read_csv(file.path(out_dir, sprintf("players_%s.csv", round_code)), show_col_types = FALSE) |>
  select(email, name) |>
  mutate(name = na_if(str_squish(name), ""))

if (nrow(games) == 0) stop("games_<round>.csv is empty. Run Script 1 and confirm mapping.")
if (nrow(picks_clean) == 0) stop("picks_clean_<round>.csv is empty. Run Script 1 first.")

# Ensure pick_norm exists (Script 1 should provide it)
if (!("pick_norm" %in% names(picks_clean))) {
  picks_clean <- picks_clean |>
    mutate(
      pick_norm = pick_value |>
        str_replace_all("\u00A0", " ") |>
        str_squish() |>
        str_to_lower()
    )
}

game_info <- games |>
  transmute(
    season, round, game_id,
    game_label,
    game_title = pretty_label_from_game(game_label)
  )

# ----------------------------
# COLUMN ORDER (optionally driven by results_<round>.csv)
# ----------------------------
results_path <- file.path(out_dir, sprintf("results_%s.csv", round_code))

if (use_results_for_order && file.exists(results_path)) {
  results_order <- read_csv(results_path, show_col_types = FALSE) |>
    filter(market %in% c("spread", "total")) |>
    mutate(
      game_id = as.character(game_id),
      market  = as.character(market)
    ) |>
    mutate(order_row = row_number()) |>
    select(order_row, game_id, market)
} else {
  results_order <- NULL
}

if (!is.null(results_order)) {
  col_plan <- results_order |>
    left_join(game_info, by = "game_id") |>
    mutate(
      game_title = coalesce(game_title, pretty_label_from_game(coalesce(game_label, ""))),
      col_key = paste0(game_id, "__", market)
    ) |>
    arrange(order_row) |>
    select(game_id, market, game_title, col_key)
} else {
  col_plan <- game_info |>
    tidyr::crossing(market = c("spread", "total")) |>
    mutate(
      market = factor(market, levels = c("spread", "total")),
      col_key = paste0(game_id, "__", as.character(market))
    ) |>
    arrange(game_id, market) |>
    mutate(market = as.character(market)) |>
    select(game_id, market, game_title, col_key)
}

col_keys <- col_plan$col_key

# ----------------------------
# LOCK MATCHING (pre-results) using shared helpers
# ----------------------------
team_to_game <- build_team_to_game(games)
pair_to_game <- build_pair_to_game(games)

candidates_feat <- picks_clean |>
  filter(market %in% c("spread", "total")) |>
  build_candidate_features()

locks <- picks_clean |>
  filter(market %in% lock_markets) |>
  mutate(
    lock_slot = case_when(
      market == "bonus_lock_1" ~ 1L,
      market == "bonus_lock_2" ~ 2L,
      TRUE ~ NA_integer_
    )
  ) |>
  filter(!is.na(lock_slot)) |>
  arrange(season, round, email, lock_slot) |>
  group_by(season, round, email, lock_slot) |>
  slice_head(n = 1) |>
  ungroup() |>
  transmute(
    season, round, email, lock_slot,
    lock_text = pick_value,
    lock_norm = normalize_txt(coalesce(pick_value, "")),
    lock_team_tokens = map(str_split(lock_norm, "\\s+"), canonicalize_teams),
    lock_nums = map(lock_norm, extract_numbers)
  ) |>
  rowwise() |>
  mutate(
    intent = list(detect_intent(lock_norm, lock_team_tokens)),
    market_hint = intent$market,
    side_hint = intent$side,
    game_infer = list(infer_game_id(lock_team_tokens, team_to_game, pair_to_game, season, round)),
    game_hint = game_infer$game_id,
    game_hint_method = game_infer$method
  ) |>
  ungroup() |>
  select(-intent, -game_infer)


lock_scoring_pre <- locks |>
  group_by(season, round, email, lock_slot) |>
  group_modify(function(df_lock, key) {
    
    lock_team_tokens <- df_lock$lock_team_tokens[[1]]
    lock_nums        <- df_lock$lock_nums[[1]]
    market_hint      <- df_lock$market_hint[1]
    side_hint        <- df_lock$side_hint[1]
    game_hint        <- df_lock$game_hint[1]
    
    cand <- candidates_feat |>
      filter(season == key$season, round == key$round, email == key$email)
    
    # Restrict by inferred game first
    if (!is.na(game_hint) && game_hint != "") {
      cand <- cand |> filter(game_id == game_hint)
    }
    
    # Restrict by market if we have an intent
    if (!is.na(market_hint) && market_hint != "") {
      cand <- cand |> filter(market == market_hint)
    }
    
    if (nrow(cand) == 0) {
      return(df_lock |>
               mutate(
                 matched = FALSE,
                 match_score = NA_integer_,
                 matched_reason = "no candidates after restrictions",
                 matched_game_id = NA_character_,
                 matched_market = dplyr::coalesce(.env$market_hint, NA_character_),
                 matched_pick = NA_character_
               ))
    }
    
    scored_cand <- score_candidates(lock_team_tokens, lock_nums, side_hint, cand)
    
    # Fallback: if game_hint is missing but this looks like a total with a number,
    # and exactly one TOTAL candidate matches (number + over/under), take it.
    fallback_best <- NULL
    if ((is.na(game_hint) || game_hint == "") &&
        length(lock_nums) > 0 &&
        !is.na(side_hint)) {
      
      fb <- scored_cand |>
        filter(market == "total", num_match == 1L, side_match == 1L) |>
        arrange(desc(team_overlap), desc(score))
      
      if (nrow(fb) == 1) fallback_best <- fb |> slice(1)
    }
    
    best <- if (!is.null(fallback_best)) fallback_best else pick_best_candidate(scored_cand, lock_nums)
    reason_override <- if (!is.null(fallback_best)) "match_fallback_total_num_side" else NA_character_
    
    
    if (all(scored_cand$score == 0L, na.rm = TRUE)) {
      return(df_lock |>
               mutate(
                 matched = FALSE,
                 match_score = 0L,
                 matched_reason = "no signal",
                 matched_game_id = NA_character_,
                 matched_market = dplyr::coalesce(.env$market_hint, scored_cand$market[1]),
                 matched_pick = NA_character_
               ))
    }
    
    if (is.null(best)) {
      return(df_lock |>
               mutate(
                 matched = FALSE,
                 match_score = max(scored_cand$score, na.rm = TRUE),
                 matched_reason = "ambiguous tie",
                 matched_game_id = NA_character_,
                 matched_market = dplyr::coalesce(.env$market_hint, scored_cand$market[1]),
                 matched_pick = NA_character_
               ))
    }
    
    reason <- dplyr::case_when(
      !is.na(reason_override) ~ reason_override,
      best$num_match[1] == 1 ~ "match_by_number",
      !is.na(side_hint) & best$side_match[1] == 1 ~ "match_by_ou_side",
      best$team_overlap[1] > 0 ~ "match_by_team_tokens",
      TRUE ~ "match_other"
    )
    
    df_lock |>
      mutate(
        matched = TRUE,
        match_score = as.integer(best$score[1]),
        matched_reason = reason,
        matched_game_id = best$game_id[1],
        matched_market = best$market[1],
        matched_pick = best$pick_value[1]
      )
  }) |>
  ungroup()

lock_tags <- lock_scoring_pre |>
  filter(matched) |>
  transmute(
    season, round, email,
    game_id = matched_game_id,
    market  = matched_market,
    pick_value = matched_pick,
    lock_tag_piece = if (lock_style == "plain") " 🔒" else paste0(" 🔒", lock_slot)
  ) |>
  group_by(season, round, email, game_id, market, pick_value) |>
  summarize(lock_tag = paste0(lock_tag_piece, collapse = ""), .groups = "drop")


# ----------------------------
# PICKS DISPLAY (wide) with lock overlay
# ----------------------------
picks_cells <- picks_clean |>
  filter(market %in% c("spread", "total")) |>
  left_join(game_info, by = c("season", "round", "game_id")) |>
  mutate(
    col_key = paste0(game_id, "__", market),
    cell = coalesce(pick_value, "")
  ) |>
  left_join(lock_tags, by = c("season", "round", "email", "game_id", "market", "pick_value")) |>
  mutate(cell = paste0(cell, coalesce(lock_tag, ""))) |>
  select(email, col_key, cell) |>
  distinct()

picks_wide <- picks_cells |>
  pivot_wider(
    names_from = col_key,
    values_from = cell,
    values_fill = ""
  )

# Ensure all planned columns exist
missing_cols <- setdiff(col_keys, names(picks_wide))
if (length(missing_cols) > 0) {
  picks_wide[missing_cols] <- ""
}
picks_wide <- picks_wide |>
  select(email, all_of(col_keys))

# ----------------------------
# LOCK RAW TEXT (last columns) with ⚠️ if unmatched
# ----------------------------
lock_unmatched_flag <- lock_scoring_pre |>
  transmute(
    season, round, email, lock_slot,
    warn = if_else(coalesce(matched, FALSE), "", " ⚠️")
  )

locks_text <- picks_clean |>
  filter(market %in% lock_markets) |>
  mutate(
    lock_slot = case_when(
      market == "bonus_lock_1" ~ 1L,
      market == "bonus_lock_2" ~ 2L,
      TRUE ~ NA_integer_
    )
  ) |>
  filter(!is.na(lock_slot)) |>
  left_join(lock_unmatched_flag, by = c("season", "round", "email", "lock_slot")) |>
  transmute(
    email,
    lock_col  = paste0("Lock ", lock_slot, " (raw)"),
    lock_text = paste0(stringr::str_squish(dplyr::coalesce(pick_value, "")), dplyr::coalesce(warn, ""))
  ) |>
  group_by(email, lock_col) |>
  summarize(lock_text = lock_text[1], .groups = "drop") |>
  pivot_wider(names_from = lock_col, values_from = lock_text, values_fill = "")

# ----------------------------
# CONFERENCE TIEBREAKER (display only; Script 3 is pre-results)
# Pull from explicit market emitted by Script 1
# Display AFTER lock raw columns
# ----------------------------
# ----------------------------
# CONFERENCE TIEBREAKER (display only; Script 3 is pre-results)
# Display AFTER lock raw columns
# ----------------------------
tiebreaker_text <- NULL

if (round_code == "con") {
  tiebreaker_text <- picks_clean |>
    filter(market == "conf_tiebreaker_total_points") |>
    transmute(
      email,
      Tiebreaker = str_squish(coalesce(pick_value_raw, pick_value, ""))
    ) |>
    group_by(email) |>
    summarize(Tiebreaker = Tiebreaker[1], .groups = "drop")
}


# Drop Lock 2 (raw) column if lock 2 isn't in play this round
if (!("bonus_lock_2" %in% lock_markets) && ("Lock 2 (raw)" %in% names(locks_text))) {
  locks_text <- locks_text |> select(-`Lock 2 (raw)`)
}

# ----------------------------
# FINAL TABLE DATA
# ----------------------------
summary <- players |>
  mutate(player = coalesce(name, email)) |>
  arrange(player) |>
  select(email, player)

round_picks <- summary |>
  left_join(picks_wide, by = "email") |>
  left_join(locks_text, by = "email")

# Add tiebreaker AFTER lock raw columns (conference only)
if (round_code == "con" && !is.null(tiebreaker_text)) {
  round_picks <- round_picks |>
    left_join(tiebreaker_text, by = "email")
}

round_picks <- round_picks |>
  select(-email)


# ----------------------------
# STYLING
# Edit these team colors as you want.
# ----------------------------
team_style <- list(
  # ----------------------------
  # NFC WEST
  # ----------------------------
  "rams"       = list(fill = "#003594", text = "#FFD100"),  # keep
  "49ers"      = list(fill = "#AA0000", text = "#B3995D"),  # keep
  "seahawks"   = list(fill = "#69BE28", text = "#FFFFFF"),  # keep
  "cardinals"  = list(fill = "#97233F", text = "#FFFFFF"),
  
  # ----------------------------
  # NFC NORTH
  # ----------------------------
  "packers"    = list(fill = "#203731", text = "#FFB612"),  # keep
  "bears"      = list(fill = "#C83803", text = "#FFFFFF"),  # keep
  "lions"      = list(fill = "#0076B6", text = "#FFFFFF"),
  "vikings"    = list(fill = "#4F2683", text = "#FFC62F"),
  
  # ----------------------------
  # NFC EAST
  # ----------------------------
  "eagles"     = list(fill = "#004C54", text = "#FFFFFF"),  # keep
  "cowboys"    = list(fill = "#002244", text = "#FFFFFF"),
  "giants"     = list(fill = "#0B2265", text = "#FFFFFF"),
  "commanders" = list(fill = "#5A1414", text = "#FFB612"),
  
  # ----------------------------
  # NFC SOUTH
  # ----------------------------
  "panthers"   = list(fill = "#BFC0BF", text = "#0085CA"),  # keep
  "saints"     = list(fill = "#101820", text = "#D3BC8D"),
  "buccaneers" = list(fill = "#D50A0A", text = "#FFFFFF"),
  "falcons"    = list(fill = "#A71930", text = "#FFFFFF"),
  
  # ----------------------------
  # AFC WEST
  # ----------------------------
  "chargers"   = list(fill = "#0080C6", text = "#FFC20E"),  # keep
  "broncos"    = list(fill = "#FB4F14", text = "#FFFFFF"),  # keep
  "chiefs"     = list(fill = "#E31837", text = "#FFB81C"),
  "raiders"    = list(fill = "#000000", text = "#A5ACAF"),
  
  # ----------------------------
  # AFC NORTH
  # ----------------------------
  "steelers"   = list(fill = "#FFB612", text = "#101820"),  # keep
  "ravens"     = list(fill = "#241773", text = "#FFFFFF"),
  "browns"     = list(fill = "#311D00", text = "#FF3C00"),
  "bengals"    = list(fill = "#FB4F14", text = "#000000"),
  
  # ----------------------------
  # AFC EAST
  # ----------------------------
  "bills"      = list(fill = "#00338D", text = "#FFFFFF"),  # keep
  "patriots"   = list(fill = "#002244", text = "#C60C30"),  # keep
  "dolphins"   = list(fill = "#008E97", text = "#FC4C02"),
  "jets"       = list(fill = "#125740", text = "#FFFFFF"),
  
  # ----------------------------
  # AFC SOUTH
  # ----------------------------
  "texans"     = list(fill = "#A71930", text = "#FFFFFF"),  # keep
  "jaguars"    = list(fill = "#006778", text = "#D7A22A"),  # keep
  "colts"      = list(fill = "#002C5F", text = "#FFFFFF"),
  "titans"     = list(fill = "#0C2340", text = "#4B92DB")
)


ou_style <- list(
  over  = list(fill = "#B0B0B0", text = "#000000"),
  under = list(fill = "#E0E0E0", text = "#000000")
)

fallback_spread <- list(fill = "#E6E6E6", text = "#000000")
fallback_total  <- list(fill = "#E6E6E6", text = "#000000")

style_pick_cell <- function(x, market = c("spread", "total")) {
  market <- match.arg(market)
  
  x0 <- ifelse(is.na(x), "", x)
  x_clean <- strip_markers(x0)
  xl <- str_to_lower(x_clean)
  
  if (market == "total") {
    st <- if (str_detect(xl, "\\bover\\b")) {
      ou_style$over
    } else if (str_detect(xl, "\\bunder\\b")) {
      ou_style$under
    } else {
      fallback_total
    }
    
    return(gt::html(sprintf(
      "<span style='display:block;padding:2px 6px;border-radius:4px;background:%s;color:%s;font-weight:700;'>%s</span>",
      st$fill, st$text, escape_html(x0)
    )))
  }
  
  team_keys <- names(team_style)
  hit <- team_keys[vapply(
    team_keys,
    function(k) str_detect(xl, paste0("\\b", k, "\\b")),
    logical(1)
  )]
  
  st <- if (length(hit) >= 1) team_style[[hit[1]]] else fallback_spread
  
  gt::html(sprintf(
    "<span style='display:block;padding:2px 6px;border-radius:4px;background:%s;color:%s;font-weight:700;'>%s</span>",
    st$fill, st$text, escape_html(x0)
  ))
}

# ----------------------------
# BUILD GT TABLE (with spanners)
# ----------------------------
sub_labels <- setNames(
  ifelse(endsWith(col_plan$col_key, "__spread"), "Spread", "Total"),
  col_plan$col_key
)

round_picks_gt <- round_picks |>
  gt() |>
  cols_label(player = "Player") |>
  cols_label(.list = sub_labels) |>
  tab_header(
    title = "Round Picks (Follow Along)",
    subtitle = glue("Round: {toupper(round_code)}")
  ) |>
  cols_align(align = "center", columns = everything()) |>
  cols_align(align = "left", columns = player) |>
  cols_width(
    player ~ px(220),
    starts_with("Lock") ~ px(260),
    everything() ~ px(150)
  ) |>
  tab_style(
    style = cell_text(weight = "bold", size = px(14)),
    locations = cells_column_labels(everything())
  )

# Add a spanner per game to group Spread and Total
for (gid in unique(col_plan$game_id)) {
  cols_for_game <- col_plan |>
    filter(game_id == gid) |>
    arrange(match(market, c("spread", "total"))) |>
    pull(col_key)
  
  if (length(cols_for_game) >= 1) {
    game_title <- col_plan |>
      filter(game_id == gid) |>
      slice(1) |>
      pull(game_title)
    
    round_picks_gt <- round_picks_gt |>
      tab_spanner(
        label = dplyr::coalesce(game_title, gid),
        columns = all_of(cols_for_game)
      )
  }
}

round_picks_gt <- round_picks_gt |>
  text_transform(
    locations = cells_body(columns = ends_with("__spread")),
    fn = function(x) vapply(x, style_pick_cell, character(1), market = "spread")
  ) |>
  text_transform(
    locations = cells_body(columns = ends_with("__total")),
    fn = function(x) vapply(x, style_pick_cell, character(1), market = "total")
  ) |>
  tab_options(
    table.font.size = px(12),
    data_row.padding = px(2)
  )

# ----------------------------
# EXPORT
# ----------------------------
out_html <- file.path(out_dir, sprintf("round_picks_%s.html", round_code))
gtsave(round_picks_gt, out_html)
message("Wrote: ", out_html)

# ----------------------------
# SANITY CHECK
# ----------------------------
total_locks <- nrow(lock_scoring_pre)
matched_locks <- sum(lock_scoring_pre$matched, na.rm = TRUE)
message("Lock matching (pre-results): ", matched_locks, " / ", total_locks, " matched")

if (matched_locks < total_locks) {
  message("Unmatched locks detected: ", total_locks - matched_locks)
  # Uncomment to inspect:
  # print(lock_scoring_pre %>%
  #   filter(!matched) %>%
  #   select(email, lock_slot, lock_text, market_hint, side_hint, game_hint, game_hint_method, matched_reason))
}
