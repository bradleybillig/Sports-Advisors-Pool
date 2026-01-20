# ============================================================
# TD PARLAY (Divisional Round only) - Extract player + 3 legs
# Output: dr out/td_parlay_legs_dr.csv
# ============================================================

library(readxl)
library(dplyr)
library(stringr)
library(purrr)
library(readr)

# ----------------------------
# CONFIG
# ----------------------------
round_code <- "dr"
in_path    <- "2026 Football Responses.xlsx"   # change if needed
sheet_name <- "Divisional Round"

out_dir <- "parlay out"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)


# ----------------------------
# HELPERS
# ----------------------------
normalize_sep <- function(x) {
  x <- as.character(x)
  x <- ifelse(is.na(x), "", x)
  
  x |>
    str_replace_all("[\r\n]+", ",") |>
    str_replace_all("\\s*(/|\\||;|\\+|&)\\s*", ",") |>
    str_replace_all("\\s+and\\s+", ",") |>
    str_replace_all("\\s*,\\s*", ",") |>
    str_replace_all("^,+|,+$", "")
}

extract_3_legs <- function(x) {
  toks <- normalize_sep(x) |>
    str_split(",", simplify = TRUE) |>
    as.character() |>
    str_trim()
  
  toks <- toks[toks != ""]
  toks <- c(toks, rep(NA_character_, 3))  # pad
  toks <- toks[1:3]
  
  tibble(
    leg1 = toks[1],
    leg2 = toks[2],
    leg3 = toks[3]
  )
}

# ----------------------------
# READ + DETECT COLUMNS
# ----------------------------
df <- read_excel(in_path, sheet = sheet_name)

email_col <- names(df)[str_detect(names(df), regex("^email", ignore_case = TRUE))][1]
name_col  <- names(df)[str_detect(names(df), regex("full name|^name$", ignore_case = TRUE))][1]
td_col    <- names(df)[str_detect(names(df), regex("TD\\s*Parlay", ignore_case = TRUE))][1]

if (is.na(email_col) || is.na(td_col)) {
  stop("Could not find required columns (Email Address and TD Parlay). Check sheet headers.")
}
if (is.na(name_col)) {
  # fallback: if no name column found, we will just use email as player label
  name_col <- email_col
}

# ----------------------------
# EXTRACT PLAYER + LEGS
# ----------------------------
out <- df |>
  transmute(
    email  = as.character(.data[[email_col]]),
    player = as.character(.data[[name_col]]),
    td_raw = .data[[td_col]]
  ) |>
  mutate(
    player = if_else(is.na(player) | str_trim(player) == "", email, player)
  ) |>
  mutate(legs = map(td_raw, extract_3_legs)) |>
  select(email, player, legs) |>
  tidyr::unnest(legs)

# ----------------------------
# WRITE
# ----------------------------
out_file <- file.path(out_dir, sprintf("td_parlay_legs_%s.csv", round_code))
write_csv(out, out_file, na = "")

message("Wrote: ", out_file)





# ============================================================
# TD PARLAY (Divisional Round only)
# Tokenize legs using editable tables you fill in
#
# INPUT:
#   parlay out/td_parlay_legs_dr.csv
#
# EDITABLE TABLES (auto-created, you fill them in):
#   parlay out/parlay_token_table.csv         # main mapping table
#   parlay out/parlay_token_words.csv         # helper, word frequency
#
# OUTPUTS:
#   parlay out/td_parlay_picks_clean_dr.csv   # long, joinable
#   parlay out/td_parlay_unmatched_dr.csv     # anything not mapped
#   parlay out/td_parlay_picks_dr.html        # pretty table
# ============================================================

library(readr)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(gt)
library(glue)

# ----------------------------
# CONFIG
# ----------------------------
round_code <- "dr"
out_dir <- "parlay out"

in_legs <- file.path(out_dir, sprintf("td_parlay_legs_%s.csv", round_code))

token_table_path <- file.path(out_dir, "parlay_token_table.csv")
token_words_path <- file.path(out_dir, "parlay_token_words.csv")

out_picks_long <- file.path(out_dir, sprintf("td_parlay_picks_clean_%s.csv", round_code))
out_unmatched  <- file.path(out_dir, sprintf("td_parlay_unmatched_%s.csv", round_code))
out_html       <- file.path(out_dir, sprintf("td_parlay_picks_%s.html", round_code))

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# ----------------------------
# HELPERS
# ----------------------------
norm_text <- function(x) {
  x <- as.character(x)
  x <- ifelse(is.na(x), "", x)
  
  x |>
    str_to_lower() |>
    str_replace_all("[^a-z0-9\\s]", " ") |>
    str_squish()
}

snake_key <- function(x) {
  x |>
    str_to_lower() |>
    str_replace_all("[^a-z0-9]+", "_") |>
    str_replace_all("^_|_$", "")
}

pill_html <- function(text, matched = TRUE) {
  txt <- ifelse(is.na(text) | str_trim(text) == "", "", as.character(text))
  matched <- ifelse(is.na(matched), FALSE, matched)
  
  bg <- ifelse(matched, "#E0E0E0", "#F7C6C6")  # gray if matched, red-ish if not
  fg <- "#000000"
  
  sprintf(
    "<span style='display:block;padding:2px 6px;border-radius:4px;background:%s;color:%s;font-weight:700;'>%s</span>",
    bg, fg, txt
  )
}

# ----------------------------
# READ EXTRACTED LEGS
# ----------------------------
legs_wide <- read_csv(in_legs, show_col_types = FALSE)

needed <- c("email", "player", "leg1", "leg2", "leg3")
miss <- setdiff(needed, names(legs_wide))
if (length(miss) > 0) stop("Missing columns in td_parlay_legs file: ", paste(miss, collapse = ", "))

picks_long_raw <- legs_wide |>
  mutate(round_code = round_code) |>
  pivot_longer(
    cols = c(leg1, leg2, leg3),
    names_to = "leg",
    values_to = "pick_raw"
  ) |>
  mutate(
    leg_num = readr::parse_number(leg),
    market = "td_parlay",
    pick_clean = norm_text(pick_raw)
  ) |>
  select(round_code, email, player, market, leg_num, pick_raw, pick_clean) |>
  arrange(player, leg_num)

# ----------------------------
# BUILD OR UPDATE TOKEN TABLE YOU EDIT
# ----------------------------
# This table is exact-match on pick_clean.
# You fill in pick_key and pick_display for each pick_clean you want to map.
# Example:
#   pick_clean = "aj brown"
#   pick_key = "aj_brown"
#   pick_display = "A.J. Brown"
#
# If new pick_clean values appear, they will be appended automatically.

unique_tokens <- picks_long_raw |>
  filter(pick_clean != "") |>
  count(pick_clean, name = "count") |>
  arrange(desc(count), pick_clean)

if (!file.exists(token_table_path)) {
  token_table <- unique_tokens |>
    transmute(
      pick_clean,
      count,
      pick_key = "",
      pick_display = "",
      notes = ""
    )
  
  write_csv(token_table, token_table_path, na = "")
  message("Created token table for you to fill in: ", token_table_path)
} else {
  token_table_existing <- read_csv(token_table_path, show_col_types = FALSE)
  
  # Ensure required columns exist (in case you accidentally delete one)
  required_cols <- c("pick_clean", "count", "pick_key", "pick_display", "notes")
  for (cc in required_cols) {
    if (!cc %in% names(token_table_existing)) token_table_existing[[cc]] <- ""
  }
  
  # Keep existing filled rows, update counts, append any new pick_clean values
  token_table <- token_table_existing |>
    select(all_of(required_cols)) |>
    right_join(unique_tokens, by = "pick_clean") |>
    mutate(
      count = coalesce(count.y, count.x),
      pick_key = coalesce(pick_key, ""),
      pick_display = coalesce(pick_display, ""),
      notes = coalesce(notes, "")
    ) |>
    select(pick_clean, count, pick_key, pick_display, notes) |>
    arrange(desc(count), pick_clean)
  
  write_csv(token_table, token_table_path, na = "")
  message("Updated token table with any new tokens: ", token_table_path)
}

# ----------------------------
# HELPER WORD TABLE (OPTIONAL, FOR QUICK SCANNING)
# ----------------------------
# This does NOT drive matching, it just helps you see common words.
token_words <- picks_long_raw |>
  filter(pick_clean != "") |>
  mutate(words = str_split(pick_clean, "\\s+")) |>
  unnest(words) |>
  filter(words != "") |>
  count(words, name = "count") |>
  arrange(desc(count), words)

write_csv(token_words, token_words_path, na = "")

# ----------------------------
# APPLY TOKEN TABLE
# ----------------------------
token_table_use <- read_csv(token_table_path, show_col_types = FALSE) |>
  mutate(
    pick_clean = as.character(pick_clean),
    pick_key = ifelse(is.na(pick_key), "", as.character(pick_key)),
    pick_display = ifelse(is.na(pick_display), "", as.character(pick_display))
  )

picks_long <- picks_long_raw |>
  left_join(
    token_table_use |> select(pick_clean, pick_key, pick_display),
    by = "pick_clean"
  ) |>
  mutate(
    # If you filled mapping, use it
    mapped = pick_clean != "" & str_trim(coalesce(pick_key, "")) != "" & str_trim(coalesce(pick_display, "")) != "",
    # Fallbacks if not mapped yet
    pick_display_final = if_else(mapped, pick_display, str_to_title(str_squish(as.character(pick_raw)))),
    pick_key_final     = if_else(mapped, pick_key, snake_key(pick_display_final))
  ) |>
  mutate(
    matched = mapped
  ) |>
  select(
    round_code, email, player, market, leg_num,
    pick_raw, pick_clean,
    pick_key = pick_key_final,
    pick_display = pick_display_final,
    matched
  ) |>
  arrange(player, leg_num)

write_csv(picks_long, out_picks_long, na = "")

unmatched <- picks_long |>
  filter(!matched & pick_clean != "") |>
  select(email, player, leg_num, pick_raw, pick_clean, pick_key, pick_display) |>
  arrange(player, leg_num)

write_csv(unmatched, out_unmatched, na = "")

# ----------------------------
# HTML TABLE (WIDE)
# ----------------------------
picks_wide <- picks_long |>
  mutate(
    leg_col = paste0("Leg ", leg_num),
    cell = pill_html(pick_display, matched = matched)
  ) |>
  select(player, leg_col, cell) |>
  pivot_wider(names_from = leg_col, values_from = cell) |>
  arrange(player)

gt_tbl <- picks_wide |>
  gt() |>
  tab_header(
    title = "TD Parlay Picks (Follow Along)",
    subtitle = glue("Round: {toupper(round_code)}")
  ) |>
  cols_align(align = "left", columns = player) |>
  cols_align(align = "center", columns = starts_with("Leg ")) |>
  cols_width(
    player ~ px(220),
    starts_with("Leg ") ~ px(260)
  ) |>
  tab_style(
    style = cell_text(weight = "bold", size = px(14)),
    locations = cells_column_labels(everything())
  ) |>
  fmt_markdown(columns = starts_with("Leg "))

gtsave(gt_tbl, out_html)

message("Wrote picks: ", out_picks_long)
message("Wrote HTML:  ", out_html)
message("Token table to edit: ", token_table_path)
message("Helper word table: ", token_words_path)
message("Unmatched report: ", out_unmatched)

# ============================================================
# BUILD TD-PARLAY GRADING LIST (one row per canonical token)
# No submitters column
# ============================================================

library(readr)
library(dplyr)
library(stringr)

round_code <- "dr"
out_dir <- "parlay out"

in_picks  <- file.path(out_dir, sprintf("td_parlay_picks_clean_%s.csv", round_code))
out_grade <- file.path(out_dir, sprintf("td_parlay_grade_list_%s.csv", round_code))

picks <- read_csv(in_picks, show_col_types = FALSE)

grade_list <- picks |>
  filter(!is.na(pick_key), str_trim(pick_key) != "") |>
  group_by(pick_key, pick_display) |>
  summarise(
    n_picks = n(),
    raw_variants = paste(sort(unique(str_squish(as.character(pick_raw)))), collapse = " | "),
    .groups = "drop"
  ) |>
  arrange(desc(n_picks), pick_display) |>
  mutate(
    scored_td = "",   # fill with 1/0 later
    notes = ""
  ) |>
  select(pick_display, pick_key, n_picks, raw_variants, scored_td, notes)

write_csv(grade_list, out_grade, na = "")
message("Wrote grading list: ", out_grade)

