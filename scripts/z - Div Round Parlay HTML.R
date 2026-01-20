# ============================================================
# GRADED HTML SHEET
# Rules
# - Each leg is colored independently:
#     1 -> green, 0 -> red, blank -> gray
# - Player name:
#     green only if ALL 3 legs are 1
#     red if ANY leg is 0
#     plain text if still pending (no 0, not all 1)
# - Row sorting:
#     primary: winner, pending, loser
#     secondary: alphabetical by player
# ============================================================

library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(gt)
library(glue)
library(htmltools)

round_code <- "dr"
out_dir <- "parlay out"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

picks_path <- file.path(out_dir, sprintf("td_parlay_picks_clean_%s.csv", round_code))
grade_path <- file.path(out_dir, sprintf("td_parlay_grade_list_%s.csv", round_code))
out_html   <- file.path(out_dir, sprintf("td_parlay_picks_graded_%s.html", round_code))

pill_status <- function(label, status) {
  label <- htmlEscape(coalesce(label, ""))
  
  status <- as.character(status)
  status <- ifelse(is.na(status), "", str_trim(status))
  status <- ifelse(status %in% c("1","0"), status, "")
  
  bg <- dplyr::case_when(
    status == "1" ~ "#CFEAD6",
    status == "0" ~ "#F4C7C3",
    TRUE          ~ "#E0E0E0"
  )
  
  sprintf(
    "<span style='display:block;padding:2px 6px;border-radius:4px;background:%s;color:#000000;font-weight:700;'>%s</span>",
    bg, label
  )
}

pill_player <- function(player_name, any_red, all_green) {
  player_name <- htmlEscape(coalesce(player_name, ""))
  
  # Red if ANY leg is 0 (highest priority)
  if (isTRUE(any_red)) {
    return(sprintf(
      "<span style='display:block;padding:2px 6px;border-radius:4px;background:%s;color:#000000;font-weight:700;'>%s</span>",
      "#F4C7C3", player_name
    ))
  }
  
  # Green only if ALL 3 legs are 1
  if (isTRUE(all_green)) {
    return(sprintf(
      "<span style='display:block;padding:2px 6px;border-radius:4px;background:%s;color:#000000;font-weight:700;'>%s</span>",
      "#CFEAD6", player_name
    ))
  }
  
  # Pending: plain text (no highlight, not bold)
  player_name

}

picks_long <- read_csv(picks_path, show_col_types = FALSE)

grades <- read_csv(grade_path, show_col_types = FALSE) |>
  transmute(
    pick_key  = str_trim(as.character(pick_key)),
    scored_td = str_trim(as.character(scored_td))
  ) |>
  mutate(
    scored_td = ifelse(is.na(scored_td), "", scored_td),
    scored_td = ifelse(scored_td %in% c("1","0"), scored_td, "")
  ) |>
  distinct(pick_key, .keep_all = TRUE)

picks_g <- picks_long |>
  mutate(pick_key = str_trim(as.character(pick_key))) |>
  left_join(grades, by = "pick_key") |>
  mutate(scored_td = ifelse(is.na(scored_td), "", scored_td))

# Per-player status flags for coloring and sorting
player_flags <- picks_g |>
  group_by(player) |>
  summarise(
    any_red      = any(scored_td == "0"),
    all_green    = n() >= 3 && all(scored_td == "1"),
    has_any_pick = any(str_trim(coalesce(as.character(pick_raw), "")) != ""),
    status_group = case_when(
      all_green ~ "winner",
      any_red   ~ "loser",
      TRUE      ~ "pending"
    ),
    status_rank = case_when(
      status_group == "winner"  ~ 1L,
      status_group == "pending" ~ 2L,
      TRUE                      ~ 3L
    ),
    .groups = "drop"
  )

# ----------------------------
# BUILD WIDE TABLE (KEEP ORIGINAL LEG ORDER)
# ----------------------------
picks_wide <- picks_g |>
  mutate(
    leg_col = paste0("Leg ", leg_num),
    cell    = pill_status(pick_display, scored_td)
  ) |>
  select(player, leg_col, cell) |>
  pivot_wider(names_from = leg_col, values_from = cell) |>
  left_join(player_flags, by = "player") |>
  mutate(player_cell = pill_player(player, any_red = any_red, all_green = all_green)) |>
  arrange(desc(coalesce(has_any_pick, FALSE)), status_rank, player) |>
  select(player_cell, `Leg 1`, `Leg 2`, `Leg 3`)


gt_tbl <- picks_wide |>
  gt() |>
  cols_label(player_cell = "Player") |>
  tab_header(
    title = "TD Parlay Picks (Graded)",
    subtitle = glue("Round: {toupper(round_code)}")
  ) |>
  cols_align(align = "left", columns = player_cell) |>
  cols_align(align = "center", columns = starts_with("Leg ")) |>
  cols_width(
    player_cell ~ px(220),
    starts_with("Leg ") ~ px(260)
  ) |>
  tab_style(
    style = cell_text(weight = "bold", size = px(14)),
    locations = cells_column_labels(everything())
  ) |>
  fmt_markdown(columns = c(player_cell, starts_with("Leg ")))

gtsave(gt_tbl, out_html)
message("Wrote: ", out_html)
