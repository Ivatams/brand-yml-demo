# ============================================================
# MARKETING DATA ANALYST HOME ASSIGNMENT
# FULL END-TO-END R SCRIPT
# Questions 1 to 5
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glue)
  library(ggplot2)
  library(scales)
  library(tidyr)
  library(forcats)
  library(stringr)
  library(tibble)
})

# ============================================================
# GLOBAL SETUP
# ============================================================

repo_root <- normalizePath(".", mustWork = TRUE)
out_dir <- file.path(repo_root, "analysis", "output")
chart_dir <- file.path(out_dir, "charts")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(chart_dir, recursive = TRUE, showWarnings = FALSE)

resolve_zip_path <- function(repo_root) {
  env_zip <- Sys.getenv("MMM_ZIP_PATH", unset = "")
  env_dir <- Sys.getenv("MMM_DATA_DIR", unset = "")
  
  candidates <- c(
    if (nzchar(env_zip)) env_zip else NULL,
    if (nzchar(env_dir)) file.path(env_dir, "Marketing_Data.zip") else NULL,
    file.path(repo_root, "Marketing_Data.zip"),
    file.path(repo_root, "mmm_practise", "Marketing_Data.zip"),
    "C:/Users/HP/Desktop/MMM/Marketing_Data.zip",
    "C:/Users/HP/Desktop/MMM/mmm_practise/Marketing_Data.zip"
  )
  
  existing <- unique(candidates)[file.exists(unique(candidates))]
  if (length(existing) == 0) stop("Marketing_Data.zip not found; set MMM_ZIP_PATH")
  normalizePath(existing[[1]], winslash = "/", mustWork = TRUE)
}

read_csv_from_zip <- function(zip_file, member, col_types = NULL) {
  con <- unz(zip_file, member, open = "rb")
  on.exit(close(con), add = TRUE)
  readr::read_csv(con, show_col_types = FALSE, col_types = col_types)
}

weighted_sd <- function(x, w) {
  if (sum(w, na.rm = TRUE) == 0) return(0)
  mu <- weighted.mean(x, w, na.rm = TRUE)
  sqrt(weighted.mean((x - mu)^2, w, na.rm = TRUE))
}

zip_path <- resolve_zip_path(repo_root)

# ============================================================
# LOAD SOURCE TABLES
# ============================================================

profiles <- read_csv_from_zip(
  zip_path,
  "Marketing Data/profiles - profiles.csv",
  col_types = cols(
    profile_id = col_character()
  )
)

payments <- read_csv_from_zip(
  zip_path,
  "Marketing Data/payments - payments.csv",
  col_types = cols(
    profile_id = col_character()
  )
)

activity <- read_csv_from_zip(
  zip_path,
  "Marketing Data/activity - activity.csv",
  col_types = cols(
    profile_id = col_character()
  )
)

# ============================================================
# CANONICAL PROFILE DIMENSION
# Used throughout all questions
# ============================================================

profile_dim <- profiles %>%
  arrange(profile_id, acquisition_source, signup_country_code) %>%
  group_by(profile_id) %>%
  summarize(
    acquisition_source = first(acquisition_source),
    signup_country_code = first(signup_country_code),
    signup_platform = first(signup_platform),
    first_activity_time = first(first_activity_time),
    first_activity_date = as.Date(first(first_activity_time)),
    n_profile_rows = n(),
    n_acquisition_source = n_distinct(acquisition_source),
    n_country = n_distinct(signup_country_code),
    .groups = "drop"
  )

profile_conflicts <- profile_dim %>%
  filter(n_acquisition_source > 1 | n_country > 1)

write_csv(profile_conflicts, file.path(out_dir, "profile_dimension_conflicts.csv"))

# ============================================================
# QUESTION 1
# Exploratory analysis: installs, RPI, trends, anomalies,
# efficient and volatile channels
# ============================================================

# ---------------------------
# Core Q1 tables
# ---------------------------

installs_tbl <- profile_dim %>%
  group_by(acquisition_source, signup_country_code) %>%
  summarize(
    installs = n_distinct(profile_id),
    .groups = "drop"
  )

revenue_tbl <- payments %>%
  left_join(
    profile_dim %>% select(profile_id, acquisition_source, signup_country_code),
    by = "profile_id",
    relationship = "many-to-one"
  ) %>%
  group_by(acquisition_source, signup_country_code) %>%
  summarize(
    total_revenue_usd = sum(gross_bookings_usd, na.rm = TRUE),
    .groups = "drop"
  )

channel_country_tbl <- installs_tbl %>%
  left_join(revenue_tbl, by = c("acquisition_source", "signup_country_code")) %>%
  mutate(
    total_revenue_usd = coalesce(total_revenue_usd, 0),
    revenue_per_install_usd = if_else(installs > 0, total_revenue_usd / installs, 0)
  )

corr_ir <- cor(
  channel_country_tbl$installs,
  channel_country_tbl$total_revenue_usd,
  use = "complete.obs"
)

channel_eff_vol_tbl <- channel_country_tbl %>%
  group_by(acquisition_source) %>%
  summarize(
    installs = sum(installs),
    total_revenue_usd = sum(total_revenue_usd),
    revenue_per_install_usd = if_else(installs > 0, total_revenue_usd / installs, 0),
    rpi_std = weighted_sd(revenue_per_install_usd, installs),
    rpi_min = min(revenue_per_install_usd, na.rm = TRUE),
    rpi_max = max(revenue_per_install_usd, na.rm = TRUE),
    .groups = "drop"
  )

rpi_median <- median(channel_country_tbl$revenue_per_install_usd, na.rm = TRUE)
install_q75 <- quantile(channel_country_tbl$installs, 0.75, na.rm = TRUE)
install_q25 <- quantile(channel_country_tbl$installs, 0.25, na.rm = TRUE)
rpi_q90 <- quantile(channel_country_tbl$revenue_per_install_usd, 0.90, na.rm = TRUE)

anomalies_tbl <- channel_country_tbl %>%
  filter(
    (installs >= install_q75 & revenue_per_install_usd <= rpi_median) |
      (installs <= install_q25 & revenue_per_install_usd >= rpi_q90)
  ) %>%
  arrange(desc(revenue_per_install_usd))

# ---------------------------
# Q1 recommended additions
# ---------------------------

payer_tbl <- payments %>%
  distinct(profile_id) %>%
  mutate(payer = 1L)

payer_rate_channel_country_tbl <- profile_dim %>%
  select(profile_id, acquisition_source, signup_country_code) %>%
  left_join(payer_tbl, by = "profile_id", relationship = "one-to-one") %>%
  mutate(payer = coalesce(payer, 0L)) %>%
  group_by(acquisition_source, signup_country_code) %>%
  summarize(
    payers = sum(payer),
    payer_rate = mean(payer),
    .groups = "drop"
  )

channel_country_enhanced_tbl <- channel_country_tbl %>%
  left_join(
    payer_rate_channel_country_tbl,
    by = c("acquisition_source", "signup_country_code")
  ) %>%
  mutate(
    install_share = installs / sum(installs, na.rm = TRUE)
  )

channel_eff_vol_enhanced_tbl <- channel_eff_vol_tbl %>%
  mutate(
    rpi_cv = if_else(revenue_per_install_usd > 0, rpi_std / revenue_per_install_usd, NA_real_)
  ) %>%
  arrange(desc(revenue_per_install_usd))

corr_installs_rpi <- cor(
  channel_country_tbl$installs,
  channel_country_tbl$revenue_per_install_usd,
  use = "complete.obs"
)

corr_tbl_q1 <- tibble(
  metric = c("corr_installs_revenue", "corr_installs_rpi"),
  value = c(corr_ir, corr_installs_rpi)
)

q1_channel_rank_tbl <- channel_eff_vol_enhanced_tbl %>%
  select(
    acquisition_source,
    installs,
    total_revenue_usd,
    revenue_per_install_usd,
    rpi_std,
    rpi_cv
  ) %>%
  arrange(desc(revenue_per_install_usd))

payer_rate_channel_tbl <- channel_country_enhanced_tbl %>%
  group_by(acquisition_source) %>%
  summarize(
    installs = sum(installs),
    payers = sum(payers, na.rm = TRUE),
    payer_rate = if_else(installs > 0, payers / installs, 0),
    .groups = "drop"
  ) %>%
  arrange(desc(payer_rate))

# ---------------------------
# Q1 exports
# ---------------------------

write_csv(channel_country_tbl, file.path(out_dir, "installs_revenue_rpi_by_channel_country.csv"))
write_csv(channel_eff_vol_tbl, file.path(out_dir, "channel_efficiency_volatility_summary.csv"))
write_csv(anomalies_tbl, file.path(out_dir, "revenue_install_anomalies.csv"))
write_csv(channel_country_enhanced_tbl, file.path(out_dir, "channel_country_enhanced_metrics.csv"))
write_csv(channel_eff_vol_enhanced_tbl, file.path(out_dir, "channel_efficiency_volatility_enhanced.csv"))
write_csv(corr_tbl_q1, file.path(out_dir, "q1_correlation_summary.csv"))
write_csv(q1_channel_rank_tbl, file.path(out_dir, "q1_channel_rank_table.csv"))
write_csv(payer_rate_channel_tbl, file.path(out_dir, "q1_payer_rate_by_channel.csv"))

# ---------------------------
# Q1 charts
# ---------------------------

top_installs_tbl <- channel_country_tbl %>%
  arrange(desc(installs)) %>%
  slice_head(n = 15) %>%
  mutate(segment = paste(acquisition_source, signup_country_code, sep = " | "))

p_q1_top_installs <- ggplot(
  top_installs_tbl,
  aes(x = fct_reorder(segment, installs), y = installs)
) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Top 15 Channel-Country Segments by Installs",
    x = "Channel | Country",
    y = "Installs"
  ) +
  theme_minimal()



ggsave(
  file.path(chart_dir, "top_installs_bar.png"),
  p_q1_top_installs,
  width = 10,
  height = 7,
  dpi = 300
)

p_q1_revenue_vs_installs <- ggplot(
  channel_country_tbl,
  aes(x = installs, y = total_revenue_usd, color = acquisition_source)
) +
  geom_point(alpha = 0.75, size = 3) +
  scale_x_continuous(labels = comma) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Revenue vs Installs by Channel-Country",
    x = "Installs",
    y = "Total Revenue (USD)",
    color = "Channel"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "revenue_vs_installs_scatter.png"),
  p_q1_revenue_vs_installs,
  width = 9,
  height = 6,
  dpi = 300
)

p_q1_channel_rpi <- ggplot(
  channel_eff_vol_tbl,
  aes(x = fct_reorder(acquisition_source, revenue_per_install_usd), y = revenue_per_install_usd)
) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Channel Revenue per Install",
    x = "Channel",
    y = "RPI (USD)"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "channel_rpi_bar.png"),
  p_q1_channel_rpi,
  width = 8,
  height = 5,
  dpi = 300
)

p_q1_channel_volatility <- ggplot(
  channel_eff_vol_tbl,
  aes(x = fct_reorder(acquisition_source, rpi_std), y = rpi_std)
) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Channel Volatility (Weighted SD of Country-Level RPI)",
    x = "Channel",
    y = "RPI Volatility"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "channel_volatility_bar.png"),
  p_q1_channel_volatility,
  width = 8,
  height = 5,
  dpi = 300
)

#anomaly_heatmap_tbl <- anomalies_tbl %>%
 # slice_head(n = min(30, n())) %>%
  #mutate(segment = paste(acquisition_source, signup_country_code, sep = " | "))

#p_q1_anomaly_heatmap <- ggplot(
 # anomaly_heatmap_tbl,
  #aes(x = acquisition_source, y = fct_reorder(signup_country_code, revenue_per_install_usd), fill = revenue_per_install_usd)
#) +
 # geom_tile() +
  #scale_fill_viridis_c(labels = dollar_format()) +
  #labs(
   # title = "Anomalous Segments: Revenue per Install",
    #x = "Channel",
    #y = "Country",
    #fill = "RPI"
  #) +
  #theme_minimal()

#ggsave(
 # file.path(chart_dir, "anomaly_heatmap.png"),
  #p_q1_anomaly_heatmap,
  #width = 8,
  #height = 6,
  #dpi = 300
#)

p_q1_installs_vs_rpi <- ggplot(
  channel_country_enhanced_tbl,
  aes(x = installs, y = revenue_per_install_usd, color = acquisition_source)
) +
  geom_point(alpha = 0.75, size = 3) +
  scale_x_log10(labels = comma) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Revenue per Install vs Installs by Channel-Country",
    x = "Installs (log scale)",
    y = "Revenue per Install (USD)",
    color = "Channel"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "q1_installs_vs_rpi_scatter.png"),
  p_q1_installs_vs_rpi,
  width = 9,
  height = 6,
  dpi = 300
)

p_q1_payer_rate <- ggplot(
  payer_rate_channel_tbl,
  aes(x = fct_reorder(acquisition_source, payer_rate), y = payer_rate)
) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(
    title = "Payer Rate by Channel",
    x = "Channel",
    y = "Payer Rate"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "q1_payer_rate_by_channel_bar.png"),
  p_q1_payer_rate,
  width = 8,
  height = 5,
  dpi = 300
)

# ============================================================
# QUESTION 2
# Estimate 90-day LTV per channel and optional predicted LTV
# ============================================================

# ---------------------------
# Core Q2 tables
# ---------------------------

payments_enriched <- payments %>%
  mutate(reported_date = as.Date(reported_date)) %>%
  left_join(
    profile_dim %>% select(profile_id, acquisition_source, first_activity_date),
    by = "profile_id",
    relationship = "many-to-one"
  ) %>%
  mutate(
    days_since_first_activity = as.integer(reported_date - first_activity_date)
  )

payments_90 <- payments_enriched %>%
  filter(
    !is.na(days_since_first_activity),
    days_since_first_activity >= 0,
    days_since_first_activity <= 90
  )

payers_90 <- payments_90 %>%
  group_by(acquisition_source) %>%
  summarize(
    payers_90 = n_distinct(profile_id),
    revenue_90_usd = sum(gross_bookings_usd, na.rm = TRUE),
    .groups = "drop"
  )

installs_channel <- profile_dim %>%
  group_by(acquisition_source) %>%
  summarize(
    installs = n_distinct(profile_id),
    .groups = "drop"
  )

ltv_90_tbl <- installs_channel %>%
  left_join(payers_90, by = "acquisition_source") %>%
  mutate(
    payers_90 = coalesce(payers_90, 0L),
    revenue_90_usd = coalesce(revenue_90_usd, 0),
    ltv_90_usd = if_else(installs > 0, revenue_90_usd / installs, 0),
    payer_rate_90 = if_else(installs > 0, payers_90 / installs, 0),
    arppu_90_usd = if_else(payers_90 > 0, revenue_90_usd / payers_90, 0)
  )

activity_enriched <- activity %>%
  mutate(activity_date = as.Date(activity_date)) %>%
  left_join(
    profile_dim %>% select(profile_id, acquisition_source, first_activity_date),
    by = "profile_id",
    relationship = "many-to-one"
  ) %>%
  mutate(
    days_since_first_activity = as.integer(activity_date - first_activity_date)
  )

retention_30_tbl <- activity_enriched %>%
  filter(
    !is.na(days_since_first_activity),
    days_since_first_activity >= 30,
    days_since_first_activity <= 37
  ) %>%
  group_by(acquisition_source) %>%
  summarize(
    retained_users_30_37 = n_distinct(profile_id),
    .groups = "drop"
  )

ltv_model_tbl <- ltv_90_tbl %>%
  left_join(retention_30_tbl, by = "acquisition_source") %>%
  mutate(
    retained_users_30_37 = coalesce(retained_users_30_37, 0L),
    retention_30_37 = if_else(installs > 0, retained_users_30_37 / installs, 0),
    predicted_ltv_retention_arppu = retention_30_37 * payer_rate_90 * arppu_90_usd
  )

ltv_lm <- lm(
  ltv_90_usd ~ retention_30_37 + payer_rate_90 + arppu_90_usd,
  data = ltv_model_tbl
)

ltv_model_tbl <- ltv_model_tbl %>%
  mutate(
    predicted_ltv_regression = as.numeric(predict(ltv_lm, newdata = ltv_model_tbl))
  )

# ---------------------------
# Q2 recommended additions
# ---------------------------

q2_ltv_rank_tbl <- ltv_90_tbl %>%
  arrange(desc(ltv_90_usd)) %>%
  mutate(ltv_rank = row_number()) %>%
  select(
    ltv_rank,
    acquisition_source,
    installs,
    payers_90,
    payer_rate_90,
    arppu_90_usd,
    revenue_90_usd,
    ltv_90_usd
  )

payments_week_ltv <- payments %>%
  mutate(reported_date = as.Date(reported_date)) %>%
  left_join(
    profile_dim %>% select(profile_id, acquisition_source, first_activity_date),
    by = "profile_id",
    relationship = "many-to-one"
  ) %>%
  mutate(
    days_since_first_activity = as.integer(reported_date - first_activity_date),
    week_since_install = floor(days_since_first_activity / 7)
  ) %>%
  filter(
    !is.na(week_since_install),
    week_since_install >= 0,
    week_since_install <= 12
  ) %>%
  group_by(acquisition_source, week_since_install) %>%
  summarize(
    weekly_revenue_usd = sum(gross_bookings_usd, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(
    profile_dim %>%
      group_by(acquisition_source) %>%
      summarize(installs = n_distinct(profile_id), .groups = "drop"),
    by = "acquisition_source"
  ) %>%
  arrange(acquisition_source, week_since_install) %>%
  group_by(acquisition_source) %>%
  mutate(
    cumulative_revenue_usd = cumsum(weekly_revenue_usd),
    cumulative_ltv_usd = if_else(installs > 0, cumulative_revenue_usd / installs, 0)
  ) %>%
  ungroup()

q2_driver_tbl <- ltv_90_tbl %>%
  mutate(
    monetization_comment = case_when(
      payer_rate_90 >= median(payer_rate_90, na.rm = TRUE) &
        arppu_90_usd >= median(arppu_90_usd, na.rm = TRUE) ~ "Strong payer rate and ARPPU",
      payer_rate_90 >= median(payer_rate_90, na.rm = TRUE) ~ "Driven more by payer conversion",
      arppu_90_usd >= median(arppu_90_usd, na.rm = TRUE) ~ "Driven more by ARPPU",
      TRUE ~ "Weak on both drivers"
    )
  ) %>%
  arrange(desc(ltv_90_usd))

# ---------------------------
# Q2 exports
# ---------------------------

write_csv(ltv_90_tbl %>% arrange(desc(ltv_90_usd)), file.path(out_dir, "ltv_90_by_channel.csv"))
write_csv(ltv_model_tbl %>% arrange(desc(ltv_90_usd)), file.path(out_dir, "ltv_predictions_by_channel.csv"))
write_csv(q2_ltv_rank_tbl, file.path(out_dir, "q2_ltv_rank_table.csv"))
write_csv(payments_week_ltv, file.path(out_dir, "q2_cumulative_ltv_curve.csv"))
write_csv(q2_driver_tbl, file.path(out_dir, "q2_ltv_driver_decomposition.csv"))

# ---------------------------
# Q2 charts
# ---------------------------

p_q2_ltv90_bar <- ggplot(
  ltv_90_tbl,
  aes(x = fct_reorder(acquisition_source, ltv_90_usd), y = ltv_90_usd)
) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "90-Day LTV by Channel",
    x = "Channel",
    y = "LTV90 (USD)"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "ltv_90_by_channel_bar.png"),
  p_q2_ltv90_bar,
  width = 8,
  height = 5,
  dpi = 300
)

p_q2_payer_arppu <- ggplot(
  ltv_90_tbl,
  aes(x = payer_rate_90, y = arppu_90_usd, size = installs, color = acquisition_source)
) +
  geom_point(alpha = 0.75) +
  scale_x_continuous(labels = percent_format(accuracy = 0.1)) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Payer Rate vs ARPPU by Channel",
    x = "Payer Rate (90-day)",
    y = "ARPPU (90-day USD)",
    size = "Installs",
    color = "Channel"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "payer_rate_vs_arppu_scatter.png"),
  p_q2_payer_arppu,
  width = 8,
  height = 6,
  dpi = 300
)

p_q2_actual_vs_pred <- ggplot(
  ltv_model_tbl,
  aes(x = ltv_90_usd, y = predicted_ltv_regression, color = acquisition_source)
) +
  geom_point(size = 3) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  scale_x_continuous(labels = dollar_format()) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Actual vs Regression-Predicted LTV",
    x = "Actual LTV90 (USD)",
    y = "Predicted LTV90 (USD)",
    color = "Channel"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "ltv_actual_vs_predicted_scatter.png"),
  p_q2_actual_vs_pred,
  width = 8,
  height = 6,
  dpi = 300
)

p_q2_cum_ltv <- ggplot(
  payments_week_ltv,
  aes(x = week_since_install, y = cumulative_ltv_usd, color = acquisition_source)
) +
  geom_line(linewidth = 1) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Cumulative LTV by Channel over Lifecycle Weeks",
    x = "Week Since Install",
    y = "Cumulative LTV (USD)",
    color = "Channel"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "q2_cumulative_ltv_by_channel.png"),
  p_q2_cum_ltv,
  width = 9,
  height = 6,
  dpi = 300
)

p_q2_ltv_vs_installs <- ggplot(
  ltv_90_tbl,
  aes(x = installs, y = ltv_90_usd, color = acquisition_source)
) +
  geom_point(size = 3) +
  scale_x_log10(labels = comma) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "90-Day LTV vs Install Scale by Channel",
    x = "Installs (log scale)",
    y = "90-Day LTV (USD)",
    color = "Channel"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "q2_ltv_vs_installs_scatter.png"),
  p_q2_ltv_vs_installs,
  width = 8,
  height = 5,
  dpi = 300
)

# ============================================================
# QUESTION 3
# Weekly cohort and retention analysis
# ============================================================

# ---------------------------
# Core Q3 tables
# ---------------------------

cohort_base <- profile_dim %>%
  mutate(
    install_date = as.Date(first_activity_date),
    cohort_week = as.Date(cut(install_date, "week"))
  )

cohort_sizes <- cohort_base %>%
  group_by(cohort_week, acquisition_source, signup_platform) %>%
  summarize(
    cohort_users = n_distinct(profile_id),
    .groups = "drop"
  )

activity_enriched_q3 <- activity %>%
  mutate(activity_date = as.Date(activity_date)) %>%
  left_join(
    cohort_base %>% select(profile_id, cohort_week, acquisition_source, signup_platform, install_date),
    by = "profile_id",
    relationship = "many-to-one"
  ) %>%
  mutate(
    days_since_install = as.integer(activity_date - install_date),
    week_since_install = floor(days_since_install / 7)
  ) %>%
  filter(
    !is.na(week_since_install),
    week_since_install >= 0,
    week_since_install <= 12
  )

retention_weekly <- activity_enriched_q3 %>%
  group_by(cohort_week, acquisition_source, signup_platform, week_since_install) %>%
  summarize(
    retained_users = n_distinct(profile_id),
    .groups = "drop"
  ) %>%
  left_join(
    cohort_sizes,
    by = c("cohort_week", "acquisition_source", "signup_platform")
  ) %>%
  mutate(
    retention_rate = if_else(cohort_users > 0, retained_users / cohort_users, 0)
  )

payments_weekly <- payments %>%
  mutate(reported_date = as.Date(reported_date)) %>%
  left_join(
    cohort_base %>% select(profile_id, cohort_week, acquisition_source, signup_platform, install_date),
    by = "profile_id",
    relationship = "many-to-one"
  ) %>%
  mutate(
    days_since_install = as.integer(reported_date - install_date),
    week_since_install = floor(days_since_install / 7)
  ) %>%
  filter(
    !is.na(week_since_install),
    week_since_install >= 0,
    week_since_install <= 12
  ) %>%
  group_by(cohort_week, acquisition_source, signup_platform, week_since_install) %>%
  summarize(
    weekly_revenue_usd = sum(gross_bookings_usd, na.rm = TRUE),
    .groups = "drop"
  )

cohort_retention_revenue <- retention_weekly %>%
  left_join(
    payments_weekly,
    by = c("cohort_week", "acquisition_source", "signup_platform", "week_since_install")
  ) %>%
  mutate(
    weekly_revenue_usd = coalesce(weekly_revenue_usd, 0),
    avg_revenue_per_retained_user = if_else(retained_users > 0, weekly_revenue_usd / retained_users, 0)
  )

retention_channel_summary <- cohort_retention_revenue %>%
  group_by(acquisition_source, signup_platform, week_since_install) %>%
  summarize(
    mean_retention_rate = mean(retention_rate, na.rm = TRUE),
    mean_arpru_weekly = mean(avg_revenue_per_retained_user, na.rm = TRUE),
    .groups = "drop"
  )

retention_ios_android_summary <- retention_channel_summary %>%
  filter(signup_platform %in% c("iOS", "Android"))

# ---------------------------
# Q3 recommended additions
# ---------------------------

retention_platform_weighted_tbl <- retention_weekly %>%
  group_by(signup_platform, week_since_install) %>%
  summarize(
    retained_users_total = sum(retained_users, na.rm = TRUE),
    cohort_users_total = sum(cohort_users, na.rm = TRUE),
    weighted_retention_rate = if_else(cohort_users_total > 0, retained_users_total / cohort_users_total, 0),
    .groups = "drop"
  )

retention_platform_week1plus_tbl <- retention_platform_weighted_tbl %>%
  filter(week_since_install >= 1)

weekly_rpu_tbl <- payments %>%
  mutate(reported_date = as.Date(reported_date)) %>%
  left_join(
    cohort_base %>% select(profile_id, acquisition_source, signup_platform, install_date),
    by = "profile_id",
    relationship = "many-to-one"
  ) %>%
  mutate(
    days_since_install = as.integer(reported_date - install_date),
    week_since_install = floor(days_since_install / 7)
  ) %>%
  filter(
    !is.na(week_since_install),
    week_since_install >= 0,
    week_since_install <= 12
  ) %>%
  group_by(acquisition_source, signup_platform, week_since_install) %>%
  summarize(
    weekly_revenue_usd = sum(gross_bookings_usd, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(
    cohort_base %>%
      group_by(acquisition_source, signup_platform) %>%
      summarize(installs = n_distinct(profile_id), .groups = "drop"),
    by = c("acquisition_source", "signup_platform")
  ) %>%
  mutate(
    revenue_per_installed_user = if_else(installs > 0, weekly_revenue_usd / installs, 0)
  )

weekly_rpu_platform_tbl <- weekly_rpu_tbl %>%
  group_by(signup_platform, week_since_install) %>%
  summarize(
    mean_rpu = mean(revenue_per_installed_user, na.rm = TRUE),
    .groups = "drop"
  )

q3_week1_scorecard_tbl <- retention_channel_summary %>%
  filter(week_since_install == 1) %>%
  arrange(desc(mean_retention_rate))

# ---------------------------
# Q3 exports
# ---------------------------

write_csv(cohort_retention_revenue, file.path(out_dir, "cohort_retention_revenue_weekly.csv"))
write_csv(retention_channel_summary, file.path(out_dir, "retention_channel_platform_summary.csv"))
write_csv(retention_ios_android_summary, file.path(out_dir, "retention_ios_android_summary.csv"))
write_csv(retention_platform_weighted_tbl, file.path(out_dir, "q3_weighted_retention_platform_summary.csv"))
write_csv(retention_platform_week1plus_tbl, file.path(out_dir, "q3_weighted_retention_platform_week1plus.csv"))
write_csv(weekly_rpu_tbl, file.path(out_dir, "q3_weekly_revenue_per_installed_user.csv"))
write_csv(q3_week1_scorecard_tbl, file.path(out_dir, "q3_week1_retention_scorecard.csv"))

# ---------------------------
# Q3 charts
# ---------------------------

retention_curve_platform_tbl <- retention_weekly %>%
  group_by(signup_platform, week_since_install) %>%
  summarize(
    mean_retention_rate = mean(retention_rate, na.rm = TRUE),
    .groups = "drop"
  )

p_q3_retention_curve_platform <- ggplot(
  retention_curve_platform_tbl,
  aes(x = week_since_install, y = mean_retention_rate, color = signup_platform)
) +
  geom_line(linewidth = 1) +
  geom_point() +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Retention Curve by Platform",
    x = "Week Since Install",
    y = "Mean Retention Rate",
    color = "Platform"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "retention_curve_by_platform.png"),
  p_q3_retention_curve_platform,
  width = 8,
  height = 5,
  dpi = 300
)

arpru_platform_tbl <- cohort_retention_revenue %>%
  group_by(signup_platform, week_since_install) %>%
  summarize(
    mean_arpru = mean(avg_revenue_per_retained_user, na.rm = TRUE),
    .groups = "drop"
  )

p_q3_arpru_platform <- ggplot(
  arpru_platform_tbl,
  aes(x = week_since_install, y = mean_arpru, color = signup_platform)
) +
  geom_line(linewidth = 1) +
  geom_point() +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "ARPRU by Platform over Lifecycle Weeks",
    x = "Week Since Install",
    y = "Average Revenue per Retained User (USD)",
    color = "Platform"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "arpru_by_platform_line.png"),
  p_q3_arpru_platform,
  width = 8,
  height = 5,
  dpi = 300
)

week1_channel_platform_tbl <- retention_channel_summary %>%
  filter(week_since_install == 1)

p_q3_week1_channel_platform <- ggplot(
  week1_channel_platform_tbl,
  aes(x = acquisition_source, y = mean_retention_rate, fill = signup_platform)
) +
  geom_col(position = "dodge") +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Week-1 Retention by Channel and Platform",
    x = "Channel",
    y = "Mean Retention Rate",
    fill = "Platform"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "week1_retention_channel_platform_bar.png"),
  p_q3_week1_channel_platform,
  width = 10,
  height = 5,
  dpi = 300
)

retention_heatmap_tbl <- retention_channel_summary %>%
  group_by(acquisition_source, week_since_install) %>%
  summarize(
    mean_retention_rate = mean(mean_retention_rate, na.rm = TRUE),
    .groups = "drop"
  )

p_q3_retention_heatmap <- ggplot(
  retention_heatmap_tbl,
  aes(x = week_since_install, y = acquisition_source, fill = mean_retention_rate)
) +
  geom_tile() +
  scale_fill_viridis_c(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Average Retention by Channel and Lifecycle Week",
    x = "Week Since Install",
    y = "Channel",
    fill = "Retention"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "retention_heatmap_channel_week.png"),
  p_q3_retention_heatmap,
  width = 9,
  height = 5,
  dpi = 300
)

p_q3_retention_weighted <- ggplot(
  retention_platform_week1plus_tbl,
  aes(x = week_since_install, y = weighted_retention_rate, color = signup_platform)
) +
  geom_line(linewidth = 1) +
  geom_point() +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Weighted Weekly Retention by Platform (Week 1+)",
    x = "Week Since Install",
    y = "Weighted Retention Rate",
    color = "Platform"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "q3_weighted_retention_by_platform.png"),
  p_q3_retention_weighted,
  width = 8,
  height = 5,
  dpi = 300
)

p_q3_rpu_platform <- ggplot(
  weekly_rpu_platform_tbl,
  aes(x = week_since_install, y = mean_rpu, color = signup_platform)
) +
  geom_line(linewidth = 1) +
  geom_point() +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Weekly Revenue per Installed User by Platform",
    x = "Week Since Install",
    y = "Revenue per Installed User (USD)",
    color = "Platform"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "q3_weekly_rpu_by_platform.png"),
  p_q3_rpu_platform,
  width = 8,
  height = 5,
  dpi = 300
)

# ============================================================
# QUESTION 4
# Incrementality / experimentation / MMM design answer
# ============================================================

q4_experiment_checklist_tbl <- tibble(
  item = c(
    "Primary estimand defined",
    "Treatment/control geos selected",
    "Pre-trends validated",
    "Spillover risk reviewed",
    "Other channel budgets locked or monitored",
    "Ramp-in and ramp-out planned",
    "Primary KPI defined",
    "Secondary KPIs defined",
    "Power/MDE reviewed",
    "Analysis plan pre-registered"
  ),
  purpose = c(
    "Clarify causal question",
    "Ensure valid randomization unit",
    "Support comparability",
    "Reduce interference risks",
    "Avoid budget substitution bias",
    "Reduce auction-learning noise",
    "Anchor decision metric",
    "Track mechanism and guardrails",
    "Avoid underpowered null results",
    "Prevent post-hoc metric shopping"
  )
)

q4_kpi_framework_tbl <- tibble(
  metric_type = c(
    "Primary",
    "Primary",
    "Secondary",
    "Secondary",
    "Guardrail",
    "Guardrail"
  ),
  metric = c(
    "Incremental 90-day LTV",
    "Incremental iROAS",
    "Incremental installs",
    "Payer rate / retention",
    "Spend shift in other channels",
    "Platform mix shift"
  ),
  reason = c(
    "Best business-value outcome",
    "Direct budget efficiency metric",
    "Volume diagnostic",
    "Mechanism diagnostic",
    "Detect substitution bias",
    "Detect audience composition drift"
  )
)

q4_experiment_results_template <- tibble(
  geo_cell = character(),
  group = character(),
  week = integer(),
  spend_meta = double(),
  installs = double(),
  revenue_90_proxy = double(),
  payer_rate = double(),
  retention_w1 = double()
)

write_csv(q4_experiment_checklist_tbl, file.path(out_dir, "q4_experiment_design_checklist.csv"))
write_csv(q4_kpi_framework_tbl, file.path(out_dir, "q4_kpi_framework.csv"))
write_csv(q4_experiment_results_template, file.path(out_dir, "q4_experiment_results_template.csv"))

experiment_data_path <- file.path(out_dir, "q4_experiment_results_filled.csv")

if (file.exists(experiment_data_path)) {
  experiment_df <- read_csv(experiment_data_path, show_col_types = FALSE)
  
  p_q4_trend <- ggplot(
    experiment_df,
    aes(x = week, y = installs, color = group)
  ) +
    geom_line(linewidth = 1) +
    labs(
      title = "Treatment vs Control Installs over Time",
      x = "Week",
      y = "Installs",
      color = "Group"
    ) +
    theme_minimal()
  
  ggsave(
    file.path(chart_dir, "q4_treatment_control_installs.png"),
    p_q4_trend,
    width = 8,
    height = 5,
    dpi = 300
  )
}

# ============================================================
# QUESTION 5
# Marketing summary and next-month recommendation
# ============================================================

q5_recommendation_tbl <- ltv_90_tbl %>%
  left_join(
    channel_eff_vol_tbl %>%
      select(acquisition_source, revenue_per_install_usd, rpi_std),
    by = "acquisition_source"
  ) %>%
  mutate(
    recommendation = case_when(
      acquisition_source %in% c("acquisition_source_6", "acquisition_source_1") ~
        "Scale gradually (+10% to +20%) with guardrails",
      acquisition_source %in% c("acquisition_source_5", "acquisition_source_7") ~
        "Hold or trim pending diagnosis",
      TRUE ~
        "Maintain as stable base and test creative/offers"
    ),
    caution = case_when(
      rpi_std >= median(rpi_std, na.rm = TRUE) ~ "High volatility; watch marginal returns",
      TRUE ~ "More stable"
    )
  ) %>%
  arrange(desc(ltv_90_usd))

q5_weekly_scorecard_template <- tibble(
  week_start = as.Date(character()),
  acquisition_source = character(),
  signup_platform = character(),
  installs = double(),
  ltv90_proxy = double(),
  payer_rate = double(),
  arppu = double(),
  week1_retention = double(),
  week4_retention = double(),
  rpi = double(),
  note = character()
)

q5_priority_actions_tbl <- tibble(
  priority = c(1, 2, 3, 4, 5),
  action = c(
    "Scale top-value channels gradually",
    "Reduce exposure to weak channels",
    "Investigate anomaly segments",
    "Improve iOS onboarding and paywall",
    "Run incrementality test on suspect paid channel"
  ),
  owner = c(
    "Growth marketing",
    "Growth marketing",
    "Analytics + CRM/Product",
    "Product + Lifecycle marketing",
    "Marketing analytics"
  ),
  success_metric = c(
    "Higher weighted LTV90",
    "Lower spend share in weak channels",
    "Lower anomaly count",
    "Higher week-1 iOS retention",
    "Measured iROAS with confidence interval"
  )
)

write_csv(q5_recommendation_tbl, file.path(out_dir, "q5_channel_recommendation_table.csv"))
write_csv(q5_weekly_scorecard_template, file.path(out_dir, "q5_weekly_marketing_scorecard_template.csv"))
write_csv(q5_priority_actions_tbl, file.path(out_dir, "q5_priority_actions_table.csv"))

p_q5_ltv_scale <- ggplot(
  q5_recommendation_tbl,
  aes(x = installs, y = ltv_90_usd, color = acquisition_source)
) +
  geom_point(size = 4) +
  scale_x_log10(labels = comma) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Channel Value vs Scale",
    x = "Installs (log scale)",
    y = "90-Day LTV (USD)",
    color = "Channel"
  ) +
  theme_minimal()

ggsave(
  file.path(chart_dir, "q5_channel_value_vs_scale.png"),
  p_q5_ltv_scale,
  width = 8,
  height = 5,
  dpi = 300
)

# ============================================================
# OPTIONAL SUMMARY TABLES
# ============================================================

q1_summary_tbl <- q1_channel_rank_tbl %>%
  select(acquisition_source, installs, revenue_per_install_usd, rpi_std, rpi_cv)

q2_summary_tbl <- q2_ltv_rank_tbl %>%
  select(acquisition_source, installs, payer_rate_90, arppu_90_usd, ltv_90_usd)

q3_summary_tbl <- q3_week1_scorecard_tbl %>%
  select(acquisition_source, signup_platform, mean_retention_rate, mean_arpru_weekly)

q5_summary_tbl <- q5_recommendation_tbl %>%
  select(acquisition_source, installs, ltv_90_usd, revenue_per_install_usd, recommendation, caution)

write_csv(q1_summary_tbl, file.path(out_dir, "summary_q1.csv"))
write_csv(q2_summary_tbl, file.path(out_dir, "summary_q2.csv"))
write_csv(q3_summary_tbl, file.path(out_dir, "summary_q3.csv"))
write_csv(q5_summary_tbl, file.path(out_dir, "summary_q5.csv"))

# ============================================================
# CONSOLE MESSAGE
# ============================================================

message("Analysis complete.")
message(glue("Output directory: {out_dir}"))
message(glue("Charts directory: {chart_dir}"))