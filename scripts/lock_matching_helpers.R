# ============================================================
# R/lock_matching_helpers.R
#
# Purpose:
#   Shared helper functions for matching free-response lock text
#   (bonus_lock_1 / bonus_lock_2) to a specific pick cell (spread or total).
#
# How to use:
#   1) Put this file at: <project_root>/R/lock_matching_helpers.R
#   2) In Script 3 and Script 4 (after libraries), add:
#        source(file.path("R", "lock_matching_helpers.R"))
#      If your scripts live in /scripts, use:
#        source(file.path("..", "R", "lock_matching_helpers.R"))
#
# Requirements:
#   This file uses namespace calls (dplyr::, stringr::, purrr::, tibble::, tidyr::),
#   so you do not have to library() these here, but they must be installed.
# ============================================================

`%||%` <- function(x, y) {
  if (!is.null(x) && length(x) > 0 && !is.na(x) && x != "") x else y
}

normalize_txt <- function(x) {
  x |>
    as.character() |>
    stringr::str_replace_all("\u00A0", " ") |>
    tolower() |>
    stringr::str_replace_all("½", ".5") |>
    stringr::str_replace_all("–|—", "-") |>
    stringr::str_replace_all("[/\\\\]", " ") |>
    stringr::str_replace_all("[^a-z0-9\\s\\./\\+\\-\\.]", " ") |>
    stringr::str_replace_all("\\s+", " ") |>
    stringr::str_trim()
}

extract_numbers <- function(x_norm) {
  m <- stringr::str_extract_all(x_norm, "(?<![a-z])\\d+(?:\\.\\d+)?")[[1]]
  if (length(m) == 0) return(numeric())
  suppressWarnings(as.numeric(m))
}

# ------------------------------------------------------------
# TEAM ALIASES
# Keep lowercase. Add as many as you want.
# canon should match the same canonical names you use elsewhere.
# ------------------------------------------------------------
team_aliases <- tibble::tribble(
  ~alias, ~canon,
  
  # NFC
  "rams","los angeles rams", "lar","los angeles rams", "la rams","los angeles rams", "losangelesrams","los angeles rams",
  "49ers","san francisco 49ers", "niners","san francisco 49ers", "sf","san francisco 49ers", "sanfrancisco","san francisco 49ers",
  "seahawks","seattle seahawks", "sea","seattle seahawks", "seattle","seattle seahawks",
  "eagles","philadelphia eagles", "phi","philadelphia eagles", "philadelphia","philadelphia eagles",
  "packers","green bay packers", "gb","green bay packers", "greenbay","green bay packers",
  "bears","chicago bears", "chi","chicago bears", "chicago","chicago bears",
  "panthers","carolina panthers", "carolina","carolina panthers", "car","carolina panthers",
  "vikings","minnesota vikings", "min","minnesota vikings", "minnesota","minnesota vikings",
  "lions","detroit lions", "det","detroit lions", "detroit","detroit lions",
  "cowboys","dallas cowboys", "dal","dallas cowboys", "dallas","dallas cowboys",
  "giants","new york giants", "nyg","new york giants", "newyorkgiants","new york giants",
  "commanders","washington commanders", "was","washington commanders", "washington","washington commanders",
  "buccaneers","tampa bay buccaneers", "bucs","tampa bay buccaneers", "tb","tampa bay buccaneers", "tampabay","tampa bay buccaneers",
  "saints","new orleans saints", "no","new orleans saints", "neworleans","new orleans saints",
  "falcons","atlanta falcons", "atl","atlanta falcons", "atlanta","atlanta falcons",
  "cardinals","arizona cardinals", "ari","arizona cardinals", "arizona","arizona cardinals",
  
  # AFC
  "bills","buffalo bills", "buf","buffalo bills", "buffalo","buffalo bills",
  "chiefs","kansas city chiefs", "kc","kansas city chiefs", "kansascity","kansas city chiefs",
  "ravens","baltimore ravens", "bal","baltimore ravens", "baltimore","baltimore ravens",
  "bengals","cincinnati bengals", "cin","cincinnati bengals", "cincinnati","cincinnati bengals",
  "browns","cleveland browns", "cle","cleveland browns", "cleveland","cleveland browns",
  "steelers","pittsburgh steelers", "pit","pittsburgh steelers", "pittsburgh","pittsburgh steelers",
  "texans","houston texans", "hou","houston texans", "houston","houston texans",
  "colts","indianapolis colts", "ind","indianapolis colts", "indy","indianapolis colts", "indianapolis","indianapolis colts",
  "jaguars","jacksonville jaguars", "jax","jacksonville jaguars", "jags","jacksonville jaguars", "jacksonville","jacksonville jaguars",
  "titans","tennessee titans", "ten","tennessee titans", "tennessee","tennessee titans",
  "chargers","los angeles chargers", "lac","los angeles chargers", "la chargers","los angeles chargers", "losangeleschargers","los angeles chargers",
  "broncos","denver broncos", "den","denver broncos", "denver","denver broncos",
  "raiders","las vegas raiders", "lv","las vegas raiders", "lasvegas","las vegas raiders",
  "dolphins","miami dolphins", "mia","miami dolphins", "miami","miami dolphins",
  "jets","new york jets", "nyj","new york jets", "newyorkjets","new york jets",
  "patriots","new england patriots", "pats","new england patriots", "ne","new england patriots", "newengland","new england patriots",
  
  #Amendments
  "philly","philadelphia eagles",
  "new england","new england patriots",
  "green bay","green bay packers",
  "san francisco","san francisco 49ers",
  "los angeles","los angeles rams",
  "la","los angeles rams",
  "san fran", "san francisco 49ers"
  
) |>
  dplyr::distinct()

betting_words <- c("over", "under", "spread", "total", "o", "u")
general_stopwords <- c(
  "at","vs","v","the","and","of","in","on",
  "night","game","football",
  "saturday","sunday","monday",
  "early","afternoon","morning",
  "pick","lock",
  betting_words
)

add_ngrams <- function(tok) {
  tok <- tok[tok != ""]
  if (length(tok) < 2) return(tok)
  
  bigrams <- paste0(tok[-length(tok)], tok[-1])
  
  trigrams <- character()
  if (length(tok) >= 3) {
    trigrams <- paste0(tok[-c(length(tok)-1, length(tok))],
                       tok[-c(1, length(tok))],
                       tok[-c(1, 2)])
  }
  
  unique(c(tok, bigrams, trigrams))
}

canonicalize_teams <- function(tokens_chr) {
  tok <- tokens_chr
  tok <- tok[!(tok %in% general_stopwords)]
  tok <- tok[tok != ""]
  
  # NEW: include joined bigrams/trigrams so "green bay" -> "greenbay", etc.
  tok2 <- add_ngrams(tok)
  
  canon <- ifelse(
    tok2 %in% team_aliases$alias,
    team_aliases$canon[match(tok2, team_aliases$alias)],
    NA_character_
  )
  
  unique(stats::na.omit(canon))
}



# Determine whether a lock looks like a spread (team-only or +/- number) or total (over/under)
detect_intent <- function(lock_norm, lock_team_tokens) {
  has_ou_word <- stringr::str_detect(lock_norm, "\\bover\\b|\\bunder\\b|\\btotal\\b|\\bo\\s*\\d|\\bu\\s*\\d")
  has_signed  <- stringr::str_detect(lock_norm, "[\\+\\-]\\s*\\d")
  has_spread_word <- stringr::str_detect(lock_norm, "\\bspread\\b")
  
  nums <- extract_numbers(lock_norm)
  has_num <- length(nums) > 0
  
  side <- dplyr::case_when(
    stringr::str_detect(lock_norm, "\\bover\\b|\\bo\\s*\\d") ~ "over",
    stringr::str_detect(lock_norm, "\\bunder\\b|\\bu\\s*\\d") ~ "under",
    TRUE ~ NA_character_
  )
  
  team_only <- (length(lock_team_tokens) >= 1) &&
    (!has_ou_word) && (!has_num) && (!has_spread_word) && (!has_signed)
  
  market <- dplyr::case_when(
    has_ou_word ~ "total",
    has_spread_word ~ "spread",
    has_signed ~ "spread",
    team_only ~ "spread",
    TRUE ~ NA_character_
  )
  
  list(market = market, side = side, nums = nums, team_only = team_only)
}

build_team_to_game <- function(games_df) {
  games_df |>
    dplyr::transmute(
      season, round, game_id,
      game_norm = normalize_txt(dplyr::coalesce(game_label, ""))
    ) |>
    dplyr::mutate(
      toks = stringr::str_split(game_norm, "\\s+"),
      canon_teams = purrr::map(toks, canonicalize_teams)
    ) |>
    dplyr::select(-game_norm, -toks) |>
    tidyr::unnest(canon_teams) |>
    dplyr::distinct(season, round, canon_team = canon_teams, game_id)
}

build_pair_to_game <- function(games_df) {
  games_df |>
    dplyr::transmute(
      season, round, game_id,
      game_norm = normalize_txt(dplyr::coalesce(game_label, ""))
    ) |>
    dplyr::mutate(
      toks = stringr::str_split(game_norm, "\\s+"),
      canon_teams = purrr::map(toks, canonicalize_teams),
      canon_teams = purrr::map(canon_teams, unique)
    ) |>
    dplyr::filter(purrr::map_int(canon_teams, length) >= 2) |>
    dplyr::mutate(
      pair_key = purrr::map_chr(canon_teams, function(x) {
        x <- sort(x)[1:2]
        paste(x, collapse = "||")
      })
    ) |>
    dplyr::distinct(season, round, pair_key, game_id)
}

infer_game_id <- function(lock_team_tokens, team_to_game, pair_to_game, season_val, round_val) {
  teams <- unique(lock_team_tokens)
  
  if (length(teams) == 0) return(list(game_id = NA_character_, method = "none"))
  
  if (length(teams) >= 2) {
    pairs <- t(combn(sort(teams), 2))
    pair_keys <- apply(pairs, 1, function(x) paste(x[1], x[2], sep = "||"))
    
    hits <- pair_to_game |>
      dplyr::filter(season == season_val, round == round_val, pair_key %in% pair_keys)
    
    gids <- unique(hits$game_id)
    if (length(gids) == 1) return(list(game_id = gids[1], method = "pair_lookup"))
    if (length(gids) > 1) return(list(game_id = NA_character_, method = "pair_ambiguous"))
  }
  
  df <- team_to_game |>
    dplyr::filter(season == season_val, round == round_val, canon_team %in% teams)
  
  if (nrow(df) == 0) return(list(game_id = NA_character_, method = "lookup_none"))
  
  gids <- unique(df$game_id)
  if (length(gids) == 1) return(list(game_id = gids[1], method = "lookup_one_team"))
  
  list(game_id = NA_character_, method = "lookup_ambiguous")
}

# Add structured features to pick candidates.
# candidates_df must include: season, round, email, game_id, market, pick_value, and ideally pick_norm.
build_candidate_features <- function(candidates_df) {
  candidates_df |>
    dplyr::mutate(
      pick_norm = normalize_txt(dplyr::coalesce(pick_norm, pick_value, "")),
      pick_tokens = stringr::str_split(pick_norm, "\\s+"),
      pick_team_tokens = purrr::map(pick_tokens, canonicalize_teams),
      pick_nums = purrr::map(pick_norm, extract_numbers),
      pick_ou = dplyr::case_when(
        market == "total" & stringr::str_detect(pick_norm, "\\bover\\b|\\bo\\s*\\d") ~ "over",
        market == "total" & stringr::str_detect(pick_norm, "\\bunder\\b|\\bu\\s*\\d") ~ "under",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(-pick_tokens)
}

# Score each candidate. Higher is better.
# num_match dominates, then over/under side, then team overlap.
score_candidates <- function(lock_team_tokens, lock_nums, side_hint, candidates) {
  candidates |>
    dplyr::mutate(
      team_overlap = purrr::map_int(pick_team_tokens, ~ length(intersect(.x, lock_team_tokens))),
      side_match = dplyr::if_else(
        !is.na(side_hint) & market == "total",
        as.integer(pick_ou == side_hint),
        0L
      ),
      num_match = purrr::map_int(pick_nums, function(pn) {
        if (length(lock_nums) == 0 || length(pn) == 0) return(0L)
        as.integer(any(abs(outer(lock_nums, pn, `-`)) < 1e-9))
      }),
      score = (num_match * 100L) + (side_match * 25L) + (team_overlap * 10L)
    )
}

# Pick the best candidate. Returns NULL if ambiguous.
pick_best_candidate <- function(scored_cand, lock_nums) {
  if (nrow(scored_cand) == 0) return(NULL)
  
  ranked <- scored_cand |>
    dplyr::arrange(dplyr::desc(score), dplyr::desc(num_match), dplyr::desc(side_match), dplyr::desc(team_overlap))
  
  best <- ranked |>
    dplyr::slice(1)
  
  tied <- ranked |>
    dplyr::filter(
      score == best$score[1],
      num_match == best$num_match[1],
      side_match == best$side_match[1],
      team_overlap == best$team_overlap[1]
    )
  
  if (nrow(tied) == 1) return(best)
  
  # Try breaking ties by numeric closeness if we have numbers
  if (length(lock_nums) > 0) {
    tied2 <- tied |>
      dplyr::mutate(
        num_dist = purrr::map_dbl(pick_nums, function(pn) {
          if (length(pn) == 0) return(Inf)
          min(abs(outer(lock_nums, pn, `-`)))
        })
      ) |>
      dplyr::arrange(num_dist)
    
    if (nrow(tied2) >= 1) {
      if (sum(tied2$num_dist == tied2$num_dist[1]) == 1) return(tied2 |> dplyr::slice(1))
    }
  }
  
  NULL
}
