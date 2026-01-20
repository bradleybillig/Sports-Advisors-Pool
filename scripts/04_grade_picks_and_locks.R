#### Script 4: Grade Picks + Locks (Post-Results) ####
# ============================================================
# Purpose:
#   Score spread/total picks using results_<round>.csv, then match + grade locks
#   using scripts/lock_matching_helpers.R.
#
# Inputs (in out_dir):
#   - picks_clean_<round>.csv
#   - games_<round>.csv
#   - results_<round>.csv  (fill correct_pick)
#
# Outputs (to out_dir):
#   - scored_picks_<round>.csv
#   - lock_scoring_<round>.csv
#   - unmatched_locks_<round>.csv
#   - weekly_totals_with_locks_<round>.csv
# ============================================================

library(readr)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(tibble)

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


base_point_value <- dplyr::case_when(
  round_code %in% c("wc", "dr") ~ 1L,
  round_code %in% c("con", "sb") ~ 2L,
  TRUE ~ 1L
)

lock_bonus <- dplyr::case_when(
  round_code %in% c("wc", "dr") ~ 1L,
  round_code %in% c("con", "sb") ~ 2L,
  TRUE ~ 1L
)

lock_markets <- c("bonus_lock_1")
if (any(picks_clean$market == "bonus_lock_2", na.rm = TRUE)) {
  lock_markets <- c("bonus_lock_1", "bonus_lock_2")
}

tiebreaker_actual <- 60  # fill in ONLY for con when known

# ----------------------------
# SOURCE SHARED LOCK HELPERS
# ----------------------------
helper_path <- file.path(root_dir, "scripts", "lock_matching_helpers.R")
if (!file.exists(helper_path)) stop("Could not find lock helper at: ", helper_path)
source(helper_path)


# ----------------------------
# Local helpers (scoring-specific)
# ----------------------------
normalize_pick <- function(x) {
  x |>
    tolower() |>
    str_replace_all("\u00A0", " ") |>
    str_replace_all("½", ".5") |>
    str_replace_all("–|—", "-") |>
    str_replace_all("\\s+", " ") |>
    str_trim()
}

norm_email <- function(x) str_to_lower(str_trim(x))

# ============================================================
# LOAD INPUTS
# ============================================================
picks_clean <- read_csv(
  file.path(out_dir, sprintf("picks_clean_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  mutate(
    season  = as.character(season),
    round   = as.character(round),
    game_id = as.character(game_id),
    email   = norm_email(email)
  )

lock_markets <- c("bonus_lock_1")
if (any(picks_clean$market == "bonus_lock_2", na.rm = TRUE)) {
  lock_markets <- c("bonus_lock_1", "bonus_lock_2")
}

# ============================================================
# TIEBREAKER (Conference round only)
# Pull from explicit market emitted by Script 1
# ============================================================

tiebreaker_pred <- NULL

if (round_code == "con") {
  
  tb_rows <- picks_clean |>
    filter(market == "conf_tiebreaker_total_points") |>
    transmute(
      season, round, email,
      tiebreaker_guess_raw = coalesce(pick_value_raw, pick_value, "")
    )
  
  if (nrow(tb_rows) > 0) {
    tiebreaker_pred <- tb_rows |>
      mutate(
        tiebreaker_guess = suppressWarnings(
          as.numeric(str_extract(tiebreaker_guess_raw, "\\d+(\\.\\d+)?"))
        )
      ) |>
      group_by(season, round, email) |>
      summarize(tiebreaker_guess = tiebreaker_guess[1], .groups = "drop")
  }
}

# --- FORCE pick_norm to exist, then normalize/fill (never errors) ---
if (!("pick_norm" %in% names(picks_clean))) {
  picks_clean$pick_norm <- NA_character_
}

picks_clean <- picks_clean |>
  mutate(pick_norm = normalize_pick(coalesce(pick_norm, pick_value, "")))

games <- read_csv(
  file.path(out_dir, sprintf("games_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  mutate(
    season  = as.character(season),
    round   = as.character(round),
    game_id = as.character(game_id)
  )

results <- read_csv(
  file.path(out_dir, sprintf("results_%s.csv", round_code)),
  show_col_types = FALSE
) |>
  mutate(
    season  = as.character(season),
    round   = as.character(round),
    game_id = as.character(game_id)
  )

# ============================================================
# RESULTS FILLED GUARD
# ============================================================
results_filled <- results |>
  filter(market %in% c("spread", "total")) |>
  filter(!is.na(correct_pick), correct_pick != "") |>
  mutate(correct_norm = normalize_pick(correct_pick))

if (nrow(results_filled) == 0) {
  stop("No correct_pick values filled yet in results file.")
}

# ============================================================
# SCORE SPREAD/TOTAL PICKS
# ============================================================
picks_main <- picks_clean |>
  filter(market %in% c("spread", "total")) |>
  mutate(pick_norm_scoring = normalize_pick(pick_norm))

scored_picks <- picks_main |>
  inner_join(results_filled, by = c("season", "round", "game_id", "market")) |>
  mutate(
    is_win = pick_norm_scoring == correct_norm,
    points = if_else(is_win, base_point_value, 0L)
  ) |>
  select(-pick_norm_scoring)

write_csv(scored_picks, file.path(out_dir, sprintf("scored_picks_%s.csv", round_code)))

# ============================================================
# LOCKS: MATCH -> GRADED PICK ROW -> LOCK BONUS
# ============================================================
team_to_game <- build_team_to_game(games)
pair_to_game <- build_pair_to_game(games)

# Candidates come from SCORED pick rows so best$is_win exists
candidates_feat <- scored_picks |>
  mutate(pick_norm_match = normalize_txt(dplyr::coalesce(pick_value, ""))) |>
  select(season, round, email, game_id, market, pick_value, pick_norm_match, is_win, points) |>
  rename(pick_norm = pick_norm_match) |>
  build_candidate_features()



# Keep the first per lock slot (defensive)
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

lock_scoring <- locks |>
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
    
    # Restrict by market if we have intent
    if (!is.na(market_hint) && market_hint != "") {
      cand <- cand |> filter(market == market_hint)
    } else {
      cand <- cand |> filter(market %in% c("spread", "total"))
    }
    
    if (nrow(cand) == 0) {
      return(df_lock |>
               mutate(
                 matched = FALSE,
                 match_score = NA_integer_,
                 matched_reason = "no_candidates_after_filters",
                 matched_game_id = NA_character_,
                 matched_market  = dplyr::coalesce(.env$market_hint, NA_character_),
                 matched_pick    = NA_character_,
                 lock_hit = FALSE,
                 lock_points = 0L
               ))
    }
    
    scored_cand <- score_candidates(lock_team_tokens, lock_nums, side_hint, cand)
    
    if (all(scored_cand$score == 0L, na.rm = TRUE)) {
      return(df_lock |>
               mutate(
                 matched = FALSE,
                 match_score = 0L,
                 matched_reason = "no_signal",
                 matched_game_id = NA_character_,
                 matched_market  = dplyr::coalesce(.env$market_hint, scored_cand$market[1]),
                 matched_pick    = NA_character_,
                 lock_hit = FALSE,
                 lock_points = 0L
               ))
    }
    
    # Fallback: if game_hint missing AND looks like TOTAL with a number + explicit OU,
    # and exactly one TOTAL candidate matches.
    fallback_best <- NULL
    if ((is.na(game_hint) || game_hint == "") &&
        length(lock_nums) > 0 &&
        !is.na(side_hint) && side_hint != "") {
      
      fb <- scored_cand |>
        filter(market == "total", num_match == 1L, side_match == 1L) |>
        arrange(desc(team_overlap), desc(score))
      
      if (nrow(fb) == 1) fallback_best <- fb |> slice(1)
    }
    
    best <- if (!is.null(fallback_best)) {
      fallback_best
    } else {
      pick_best_candidate(scored_cand, lock_nums)
    }
    
    if (is.null(best)) {
      return(df_lock |>
               mutate(
                 matched = FALSE,
                 match_score = max(scored_cand$score, na.rm = TRUE),
                 matched_reason = "tie_ambiguous",
                 matched_game_id = NA_character_,
                 matched_market  = dplyr::coalesce(.env$market_hint, scored_cand$market[1]),
                 matched_pick    = NA_character_,
                 lock_hit = FALSE,
                 lock_points = 0L
               ))
    }
    
    reason_override <- if (!is.null(fallback_best)) "match_fallback_total_num_side" else NA_character_
    
    reason <- dplyr::case_when(
      !is.na(reason_override) ~ reason_override,
      best$num_match[1] == 1 ~ "match_by_number",
      !is.na(side_hint) && best$side_match[1] == 1 ~ "match_by_ou_side",
      best$team_overlap[1] > 0 ~ "match_by_team_tokens",
      TRUE ~ "match_other"
    )
    
    lock_hit <- isTRUE(best$is_win[1])
    lock_pts <- if_else(lock_hit, lock_bonus, 0L)
    
    df_lock |>
      mutate(
        matched = TRUE,
        match_score = as.integer(best$score[1]),
        matched_reason = reason,
        matched_game_id = best$game_id[1],
        matched_market  = best$market[1],
        matched_pick    = best$pick_value[1],
        lock_hit = lock_hit,
        lock_points = lock_pts
      )
  }) |>
  ungroup()

# ============================================================
# DUPLICATE LOCK GUARD
# If both locks map to the same exact pick, only the first counts.
# ============================================================
lock_scoring <- lock_scoring |>
  mutate(
    matched_pick_key = if_else(
      matched,
      normalize_pick(coalesce(matched_pick, "")),
      NA_character_
    )
  ) |>
  arrange(season, round, email, lock_slot) |>
  group_by(season, round, email) |>
  mutate(
    match_key = if_else(
      matched,
      paste0(coalesce(matched_game_id, ""), "||",
             coalesce(matched_market, ""), "||",
             coalesce(matched_pick_key, "")),
      NA_character_
    ),
    is_dup_match = matched & duplicated(match_key),
    
    matched = if_else(is_dup_match, FALSE, matched),
    matched_reason = if_else(is_dup_match, "dup_match_ignored", matched_reason),
    lock_hit = if_else(is_dup_match, FALSE, lock_hit),
    lock_points = if_else(is_dup_match, 0L, lock_points)
  ) |>
  ungroup() |>
  select(-matched_pick_key, -match_key, -is_dup_match)

write_csv(lock_scoring, file.path(out_dir, sprintf("lock_scoring_%s.csv", round_code)))

unmatched_locks <- lock_scoring |>
  filter(!matched) |>
  select(season, round, email, lock_slot, lock_text, market_hint, side_hint, game_hint, game_hint_method, match_score, matched_reason)

write_csv(unmatched_locks, file.path(out_dir, sprintf("unmatched_locks_%s.csv", round_code)))

# ============================================================
# WEEKLY TOTALS
# ============================================================
weekly_totals_with_locks <- scored_picks |>
  group_by(season, round, email) |>
  summarize(
    base_points = sum(points),
    wins = sum(is_win),
    losses = n() - wins,
    .groups = "drop"
  ) |>
  left_join(
    lock_scoring |>
      group_by(season, round, email) |>
      summarize(
        lock_hits = sum(lock_hit, na.rm = TRUE),
        lock_points = sum(lock_points, na.rm = TRUE),
        .groups = "drop"
      ),
    by = c("season", "round", "email")
  ) |>
  mutate(
    lock_hits = coalesce(lock_hits, 0L),
    lock_points = coalesce(lock_points, 0L),
    total_points = base_points + lock_points
  ) |>
  arrange(desc(total_points), desc(wins), email)

# ============================================================
# Attach + score tiebreaker (Conference only)
# ============================================================
if (round_code == "con") {
  
  if (is.null(tiebreaker_pred)) {
    
    weekly_totals_with_locks <- weekly_totals_with_locks |>
      mutate(
        tiebreaker_guess = NA_real_,
        tiebreaker_valid = NA,
        tiebreaker_diff  = NA_real_
      )
    
  } else {
    
    weekly_totals_with_locks <- weekly_totals_with_locks |>
      left_join(tiebreaker_pred, by = c("season", "round", "email")) |>
      mutate(
        tiebreaker_guess = suppressWarnings(as.numeric(tiebreaker_guess)),
        
        # TRUE if guess exists and is <= actual; NA if actual/guess missing
        tiebreaker_valid = if_else(
          !is.na(tiebreaker_actual) & !is.na(tiebreaker_guess),
          tiebreaker_guess <= tiebreaker_actual,
          NA
        ),
        
        # Price-is-right diff: smaller is better, only if not over
        tiebreaker_diff = if_else(
          !is.na(tiebreaker_actual) & !is.na(tiebreaker_guess) & tiebreaker_guess <= tiebreaker_actual,
          tiebreaker_actual - tiebreaker_guess,
          NA_real_
        )
      )
  }
  
  # ----------------------------------------------------------
  # CONFERENCE ROUND ORDERING: use tiebreaker for payout order
  # ----------------------------------------------------------
  weekly_totals_with_locks <- weekly_totals_with_locks |>
    arrange(
      desc(total_points),
      desc(wins),
      
      # valid (not over) beats invalid/missing
      desc(coalesce(tiebreaker_valid, FALSE)),
      
      # smaller diff is better; NA goes to bottom
      coalesce(tiebreaker_diff, Inf),
      
      email
    )
  
} else {
  
  # Default ordering for all other rounds
  weekly_totals_with_locks <- weekly_totals_with_locks |>
    arrange(desc(total_points), desc(wins), email)
  
}


write_csv(
  weekly_totals_with_locks,
  file.path(out_dir, sprintf("weekly_totals_with_locks_%s.csv", round_code))
)

# ============================================================
# CONSOLE SUMMARY
# ============================================================
message("Graded pick rows (spread+total): ", nrow(scored_picks))
message("Locks matched: ", sum(lock_scoring$matched, na.rm = TRUE), " / ", nrow(lock_scoring))

total_players <- picks_clean |>
  distinct(email) |>
  nrow()

scored_players <- scored_picks |>
  distinct(email) |>
  nrow()

message("Total players scored: ", scored_players, " / ", total_players)
