# FM445 Structured Project: Factor Strategy Performance Under Secular Stagnation
# Constructing Value, Momentum, and Quality Portfolios from CRSP/Compustat
# Isaac Stella — 250044074

library(tidyverse)
library(lubridate)
library(RPostgres)    
# 1. CONNECT TO WRDS

# prompt for your WRDS username and password
wrds = dbConnect(Postgres(),
                 host     = "wrds-pgdata.wharton.upenn.edu",
                 port     = 9737,
                 dbname   = "wrds",
                 sslmode  = "require",
                 gssencmode = "disable", #Wasn't working without this. Bypassing an encryption negotiation.
                 user     = rstudioapi::askForPassword("WRDS username"),
                 password = rstudioapi::askForPassword("WRDS password"))

# =============================================================================
# 2. PULL CRSP MONTHLY STOCK DATA
# =============================================================================

# Common stocks (shrcd 10, 11) on NYSE/AMEX/NASDAQ (exchcd 1, 2, 3)
# Monthly returns, prices, shares outstanding
crsp = dbGetQuery(wrds, "
  SELECT a.permno, a.date, a.ret, a.retx, a.shrout, a.prc, a.cfacpr,
         b.shrcd, b.exchcd, b.siccd
  FROM crsp.msf AS a
  LEFT JOIN crsp.msenames AS b
    ON a.permno = b.permno
    AND a.date >= b.namedt
    AND a.date <= b.nameendt
  WHERE a.date BETWEEN '1989-01-01' AND '2022-12-31'
    AND b.shrcd IN (10, 11)
    AND b.exchcd IN (1, 2, 3)
")

crsp = crsp %>%
  mutate(date = ymd(date),
         ret  = as.numeric(ret),
         prc  = abs(as.numeric(prc)),
         me   = (prc * shrout) / 1000) %>%
  filter(!is.na(ret), !is.na(me), me > 0,
         prc >= 5)    # drop penny stocks/illegitimate stocks below $5

winsorize = function(x, probs = c(0.01, 0.99)) {
  bounds = quantile(x, probs, na.rm = TRUE)
  pmax(pmin(x, bounds[2]), bounds[1])
}

crsp = crsp %>% mutate(ret = winsorize(ret))
summary(crsp$ret)

cat("CRSP rows:", nrow(crsp), "\n")
cat("Unique stocks:", n_distinct(crsp$permno), "\n")
cat("Date range:", as.character(range(crsp$date)), "\n")

# =============================================================================
# 3. PULL COMPUSTAT ANNUAL DATA
# =============================================================================

# Book equity and profitability measures
comp = dbGetQuery(wrds, "
  SELECT gvkey, datadate, fyear,
         at, ceq, txditc, pstkl, pstkrv, pstk,
         seq, lt, mib,
         revt, cogs, xsga, xint, txt, ib, oiadp, gp,
         sale, dp
  FROM comp.funda
  WHERE datadate BETWEEN '1988-01-01' AND '2022-12-31'
    AND indfmt  = 'INDL'
    AND datafmt = 'STD'
    AND popsrc  = 'D'
    AND consol  = 'C'
")

comp = comp %>%
  mutate(datadate = ymd(datadate)) %>%
  mutate(across(c(at, ceq, txditc, pstkl, pstkrv, pstk, seq,
                  lt, mib, revt, cogs, xsga, xint, txt, ib,
                  oiadp, gp, sale, dp),
                as.numeric))

# --- Book Equity (following Fama-French convention) ---
# BE = stockholders' equity + deferred taxes - preferred stock
comp = comp %>%
  mutate(
    # Stockholders' equity: use seq, or if missing: ceq, or if missing: at - lt - mib
    she = coalesce(seq, ceq, at - lt - replace_na(mib, 0)),
    # Preferred stock: use pstkl (liquidating), pstkrv (redemption), pstk (par)
    ps  = coalesce(pstkl, pstkrv, pstk, 0),
    # Deferred taxes
    dt  = replace_na(txditc, 0),
    # Book equity
    be  = she + dt - ps
  ) %>%
  filter(!is.na(be), be > 0)

# --- Quality / Profitability Measures ---
# Gross profitability (Novy-Marx 2013): GP/AT
# Return on equity: IB/BE
# Operating profitability: (REVT - COGS - XSGA - XINT) / BE  (Fama-French RMW)
comp = comp %>%
  mutate(
    gp_at   = ifelse(!is.na(gp) & !is.na(at) & at > 0, gp / at, NA),
    roe     = ifelse(!is.na(ib) & be > 0, ib / be, NA),
    op_be   = ifelse(be > 0,
                     (replace_na(revt,0) - replace_na(cogs,0) -
                        replace_na(xsga,0) - replace_na(xint,0)) / be,
                     NA)
  )

#Winsorization
comp = comp %>%
  mutate(gp_at = ifelse(!is.na(gp_at), winsorize(gp_at), NA)) 

#Sanity checks
cat("Compustat rows:", nrow(comp), "\n")
summary(comp$be)
summary(comp$gp_at)
cat("Non-NA gp_at:", sum(!is.na(comp$gp_at)), "\n")

# =============================================================================
# 4. CRSP-COMPUSTAT LINK
# =============================================================================

ccm_link = dbGetQuery(wrds, "
  SELECT gvkey, lpermno AS permno, linkdt, linkenddt, linktype, linkprim
  FROM crsp.ccmxpf_lnkhist
  WHERE linktype IN ('LU', 'LC')
    AND linkprim IN ('P', 'C')
")

ccm_link = ccm_link %>%
  mutate(linkdt    = ymd(linkdt),
         linkenddt = ymd(ifelse(is.na(linkenddt), "2099-12-31", as.character(linkenddt))))

# Merge Compustat with CRSP identifiers
comp_crsp = comp %>%
  inner_join(ccm_link, by = "gvkey") %>%
  filter(datadate >= linkdt, datadate <= linkenddt) %>%
  select(-linkdt, -linkenddt, -linktype, -linkprim)

cat("Linked Compustat-CRSP rows:", nrow(comp_crsp), "\n")

# =============================================================================
# 5. CONSTRUCT ANNUAL CHARACTERISTICS (JUNE SORTING)
# =============================================================================

# Fama-French convention: use fiscal year-end data from year t-1, 
# match to returns from July of year t through June of year t+1.
# Book equity from fiscal year ending in calendar year t-1 is matched 
# with market equity from December of year t-1.

# December market equity for B/M ratio
dec_me = crsp %>%
  filter(month(date) == 12) %>%
  mutate(year = year(date)) %>%
  select(permno, year, me_dec = me)

# June market equity for portfolio weighting
jun_me = crsp %>%
  filter(month(date) == 6) %>%
  mutate(year = year(date)) %>%
  select(permno, year, me_jun = me, exchcd)

# Match: fiscal year t-1 accounting data -> portfolios formed June year t
# We use the most recent fiscal year ending in calendar year t-1
annual_chars = comp_crsp %>%
  mutate(year = year(datadate)) %>%
  group_by(permno, year) %>%
  slice_max(datadate, n = 1) %>%     # most recent if multiple fiscal year-ends
  ungroup() %>%
  mutate(sort_year = year + 1) %>%   # data from year t-1 used in June of year t
  # Merge December ME from year t-1 for B/M
  left_join(dec_me, by = c("permno", "year")) %>%
  # Merge June ME from sort_year for weighting
  left_join(jun_me, by = c("permno", "sort_year" = "year")) %>%
  filter(!is.na(me_dec), me_dec > 0, !is.na(me_jun), me_jun > 0) %>%
  mutate(
    bm = be / me_dec    # book-to-market ratio
  )

cat("Annual characteristics rows:", nrow(annual_chars), "\n")

# =============================================================================
# 6. PORTFOLIO ASSIGNMENT
# =============================================================================

# NYSE breakpoints for all sorts (standard Fama-French approach)
# Quintile portfolios (5 groups) — you could also do deciles

assign_quintiles = function(x, breakpoints) {
  findInterval(x, breakpoints, rightmost.closed = TRUE) + 1
}

portfolios = annual_chars %>%
  group_by(sort_year) %>%
  mutate(
    # NYSE stocks only for breakpoints
    nyse = (exchcd == 1),
    # Value sort: B/M quintiles using NYSE breakpoints
    bm_breaks = list(quantile(bm[nyse], probs = c(0.2, 0.4, 0.6, 0.8), na.rm = TRUE)),
    bm_q      = assign_quintiles(bm, bm_breaks[[1]]),
    # Quality sort: GP/AT quintiles using NYSE breakpoints
    gp_breaks = list(quantile(gp_at[nyse], probs = c(0.2, 0.4, 0.6, 0.8), na.rm = TRUE)),
    gp_q      = assign_quintiles(gp_at, gp_breaks[[1]])
  ) %>%
  ungroup() %>%
  select(permno, sort_year, bm_q, gp_q, me_jun)

# =============================================================================
# 7. MOMENTUM SIGNAL (MONTHLY)
# =============================================================================

# 12-2 month momentum: cumulative return from t-12 to t-2
# This is computed monthly, not annually

crsp_mom = crsp %>%
  arrange(permno, date) %>%
  group_by(permno) %>%
  mutate(
    # Cumulative return from t-12 to t-2 (skip most recent month)
    ret_12_2 = slider::slide_dbl(
      ret,
      ~ prod(1 + .x) - 1,
      .before = 11,
      .after  = -1,    # exclude t-1 (i.e. skip month t-1)
      .complete = TRUE
    )
  ) %>%
  ungroup()

# NOTE on the .after = -1 approach: slider may not support negative .after
# Alternative approach without slider:
crsp_mom = crsp %>%
  arrange(permno, date) %>%
  group_by(permno) %>%
  mutate(
    cum_ret   = cumprod(1 + ret),
    cum_lag2  = lag(cum_ret, 1),    # cumulative return up to t-2
    cum_lag12 = lag(cum_ret, 11),   # cumulative return up to t-12
    ret_12_2  = ifelse(!is.na(cum_lag2) & !is.na(cum_lag12) & cum_lag12 != 0,
                       cum_lag2 / cum_lag12 - 1,
                       NA)
  ) %>%
  ungroup() %>%
  select(permno, date, ret, me, exchcd, ret_12_2)

# Monthly momentum quintile assignment (NYSE breakpoints)
crsp_mom = crsp_mom %>%
  filter(!is.na(ret_12_2)) %>%
  group_by(date) %>%
  mutate(
    nyse = (exchcd == 1),
    mom_breaks = list(quantile(ret_12_2[nyse], probs = c(0.2, 0.4, 0.6, 0.8), na.rm = TRUE)),
    mom_q      = assign_quintiles(ret_12_2, mom_breaks[[1]])
  ) %>%
  ungroup()

# =============================================================================
# 8. COMPUTE PORTFOLIO RETURNS
# =============================================================================

# Assign sort_year to monthly returns: July year t -> June year t+1
crsp_dated = crsp %>%
  mutate(sort_year = ifelse(month(date) >= 7, year(date), year(date) - 1))

# Merge annual portfolio assignments (value, quality)
crsp_merged = crsp_dated %>%
  left_join(portfolios, by = c("permno", "sort_year")) %>%
  # Merge momentum assignment
  left_join(crsp_mom %>% select(permno, date, ret_12_2, mom_q),
            by = c("permno", "date"))

# --- Value-weighted portfolio return function ---
vw_return = function(data, group_var) {
  data %>%
    filter(!is.na(!!sym(group_var)), !is.na(ret), !is.na(me)) %>%
    group_by(date, !!sym(group_var)) %>%
    summarise(
      ret_vw = weighted.mean(ret, me, na.rm = TRUE),
      n_stocks = n(),
      .groups = "drop"
    )
}

# --- Value Portfolios (quintiles 1 = growth, 5 = value) ---
val_port = vw_return(crsp_merged, "bm_q") %>%
  pivot_wider(id_cols = date, names_from = bm_q, values_from = ret_vw,
              names_prefix = "BM_Q") %>%
  mutate(HML = BM_Q5 - BM_Q1)   # long value, short growth

# --- Momentum Portfolios (quintiles 1 = losers, 5 = winners) ---
mom_port = vw_return(crsp_merged, "mom_q") %>%
  pivot_wider(id_cols = date, names_from = mom_q, values_from = ret_vw,
              names_prefix = "MOM_Q") %>%
  mutate(UMD = MOM_Q5 - MOM_Q1)  # long winners, short losers

# --- Quality Portfolios (quintiles 1 = junk, 5 = quality) ---
qual_port = vw_return(crsp_merged, "gp_q") %>%
  pivot_wider(id_cols = date, names_from = gp_q, values_from = ret_vw,
              names_prefix = "GP_Q") %>%
  mutate(QMJ = GP_Q5 - GP_Q1)   # long quality, short junk

# =============================================================================
# 9. FAMA-FRENCH FACTORS FOR RHS OF REGRESSIONS
# =============================================================================

# Download FF5 factors for use as controls (not as dependent variables)
# These are your benchmark models for computing alphas
ff5 = dbGetQuery(wrds, "
  SELECT date, mktrf, smb, hml, rmw, cma, rf
  FROM ff.fivefactors_monthly
  WHERE date BETWEEN '1990-01-01' AND '2022-12-31'
")

ff5 = ff5 %>%
  mutate(date = ymd(date)) %>%
  # FF factors from WRDS are in decimal; convert to match CRSP returns
  mutate(across(c(mktrf, smb, hml, rmw, cma, rf), ~ . ))

# Check units: if CRSP ret is in decimals (0.05 = 5%), FF should match
# If FF is in percentages (5.0 = 5%), divide by 100
cat("\nFF5 mean mktrf:", mean(ff5$mktrf, na.rm = TRUE), "\n")
cat("CRSP mean ret:",   mean(crsp$ret, na.rm = TRUE), "\n")
# Both should be similar magnitude — if not, rescale one

# =============================================================================
# 10. MASTER DATASET
# =============================================================================

master = val_port %>%
  select(date, HML) %>%
  left_join(mom_port %>% select(date, UMD), by = "date") %>%
  left_join(qual_port %>% select(date, QMJ), by = "date") %>%
  left_join(ff5, by = "date") %>%
  filter(date >= ymd("1990-01-01"), date <= ymd("2022-12-31"))

# Convert your portfolio returns to excess returns (subtract RF)
master = master %>%
  mutate(
    HML_ex = HML - rf,
    UMD_ex = UMD - rf,
    QMJ_ex = QMJ - rf
  )

# =============================================================================
# 11. DEFINE REGIMES
# =============================================================================

master = master %>%
  mutate(Regime = case_when(
    date >= ymd("1990-01-01") & date <= ymd("2007-12-01") ~ "Normal",
    date >= ymd("2009-07-01") & date <= ymd("2021-12-01") ~ "Secular_Stagnation",
    TRUE ~ "Excluded"
  ))

analysis = master %>% filter(Regime != "Excluded")

cat("\nObservations by regime:\n")
analysis %>% count(Regime) %>% print()

# =============================================================================
# 12. DESCRIPTIVE STATISTICS
# =============================================================================

strat_cols = c("HML", "UMD", "QMJ")

# Annualised mean returns
cat("\n--- Annualised Mean Returns by Regime ---\n")
analysis %>%
  group_by(Regime) %>%
  summarise(across(all_of(strat_cols), ~ mean(., na.rm = TRUE) * 12,
                   .names = "{.col}_ann_mean")) %>%
  print()

# Annualised Sharpe ratios
cat("\n--- Annualised Sharpe Ratios by Regime ---\n")
analysis %>%
  group_by(Regime) %>%
  summarise(across(all_of(strat_cols),
                   ~ (mean(., na.rm = TRUE) / sd(., na.rm = TRUE)) * sqrt(12),
                   .names = "{.col}_sharpe")) %>%
  print()

# Annualised volatility
cat("\n--- Annualised Volatility by Regime ---\n")
analysis %>%
  group_by(Regime) %>%
  summarise(across(all_of(strat_cols), ~ sd(., na.rm = TRUE) * sqrt(12),
                   .names = "{.col}_vol")) %>%
  print()

# Number of stocks in long and short legs (spot check)
cat("\n--- Average Stocks per Portfolio (Value) ---\n")
crsp_merged %>%
  filter(!is.na(bm_q)) %>%
  group_by(date, bm_q) %>%
  summarise(n = n(), .groups = "drop") %>%
  group_by(bm_q) %>%
  summarise(avg_n = mean(n)) %>%
  print()

# =============================================================================
# 13. CUMULATIVE RETURN PLOTS
# =============================================================================

cum_plot_data = analysis %>%
  arrange(date) %>%
  mutate(across(all_of(strat_cols), ~ cumprod(1 + .), .names = "cum_{.col}"))

ggplot(cum_plot_data, aes(x = date)) +
  geom_line(aes(y = cum_HML, color = "Value (HML)")) +
  geom_line(aes(y = cum_UMD, color = "Momentum (UMD)")) +
  geom_line(aes(y = cum_QMJ, color = "Quality (QMJ)")) +
  geom_vline(xintercept = as.numeric(ymd("2007-12-01")),
             linetype = "dashed", alpha = 0.5) +
  geom_vline(xintercept = as.numeric(ymd("2009-07-01")),
             linetype = "dashed", alpha = 0.5) +
  labs(title = "Cumulative Returns: Factor Strategies",
       x = "", y = "Growth of $1", color = "Strategy") +
  theme_minimal()

# =============================================================================
# 14. FACTOR REGRESSIONS BY REGIME
# =============================================================================

run_regressions = function(data, dep_var, regime_label) {
  cat("\n==========================================================\n")
  cat("Strategy:", dep_var, " | Regime:", regime_label, "\n")
  cat("==========================================================\n")
  
  sub = data %>% filter(Regime == regime_label)
  
  # CAPM alpha
  capm = lm(reformulate("mktrf", dep_var), data = sub)
  cat("\n--- CAPM ---\n")
  print(summary(capm))
  
  # FF3 alpha
  ff3 = lm(reformulate(c("mktrf", "smb", "hml"), dep_var), data = sub)
  cat("\n--- FF3 ---\n")
  print(summary(ff3))
  
  # FF5 alpha
  ff5_reg = lm(reformulate(c("mktrf", "smb", "hml", "rmw", "cma"), dep_var),
               data = sub)
  cat("\n--- FF5 ---\n")
  print(summary(ff5_reg))
  
  return(list(capm = capm, ff3 = ff3, ff5 = ff5_reg))
}

# Value
hml_norm = run_regressions(analysis, "HML", "Normal")
hml_stag = run_regressions(analysis, "HML", "Secular_Stagnation")

# Momentum
umd_norm = run_regressions(analysis, "UMD", "Normal")
umd_stag = run_regressions(analysis, "UMD", "Secular_Stagnation")

# Quality
qmj_norm = run_regressions(analysis, "QMJ", "Normal")
qmj_stag = run_regressions(analysis, "QMJ", "Secular_Stagnation")

# =============================================================================
# 15. FORMAL TEST: REGIME INTERACTION
# =============================================================================

test_alpha_diff = function(data, dep_var) {
  cat("\n=== Alpha Difference Test:", dep_var, "===\n")
  
  sub = data %>% mutate(D = ifelse(Regime == "Secular_Stagnation", 1, 0))
  
  # CAPM with interaction
  f_capm = as.formula(paste0(dep_var, " ~ mktrf * D"))
  cat("\n--- CAPM Interaction ---\n")
  print(summary(lm(f_capm, data = sub)))
  
  # FF5 with interaction
  f_ff5 = as.formula(paste0(dep_var,
                            " ~ mktrf + smb + hml + rmw + cma + D + mktrf:D + smb:D + hml:D + rmw:D + cma:D"))
  cat("\n--- FF5 Interaction ---\n")
  print(summary(lm(f_ff5, data = sub)))
}

test_alpha_diff(analysis, "HML")
test_alpha_diff(analysis, "UMD")
test_alpha_diff(analysis, "QMJ")

# =============================================================================
# 16. ROLLING ALPHA (ROBUSTNESS)
# =============================================================================

rolling_window = 36

rolling_results = analysis %>% arrange(date)
rolling_results$roll_alpha_HML = NA_real_
rolling_results$roll_alpha_UMD = NA_real_
rolling_results$roll_alpha_QMJ = NA_real_

for (i in rolling_window:nrow(rolling_results)) {
  w = rolling_results[(i - rolling_window + 1):i, ]
  rolling_results$roll_alpha_HML[i] = coef(lm(HML ~ mktrf, data = w))[1]
  rolling_results$roll_alpha_UMD[i] = coef(lm(UMD ~ mktrf, data = w))[1]
  rolling_results$roll_alpha_QMJ[i] = coef(lm(QMJ ~ mktrf, data = w))[1]
}

ggplot(rolling_results %>% filter(!is.na(roll_alpha_HML)),
       aes(x = date)) +
  geom_line(aes(y = roll_alpha_HML, color = "Value (HML)")) +
  geom_line(aes(y = roll_alpha_UMD, color = "Momentum (UMD)")) +
  geom_line(aes(y = roll_alpha_QMJ, color = "Quality (QMJ)")) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_vline(xintercept = as.numeric(ymd("2008-09-01")),
             linetype = "dotted", color = "red") +
  labs(title = "36-Month Rolling CAPM Alpha",
       x = "", y = "Monthly Alpha", color = "Strategy") +
  theme_minimal()

# =============================================================================
# 17. MAXIMUM DRAWDOWN & SORTINO RATIOS
# =============================================================================

max_drawdown = function(r) {
  cum = cumprod(1 + r)
  peak = cummax(cum)
  min((cum - peak) / peak)
}

sortino = function(r) {
  down = r[r < 0]
  dd = sqrt(mean(down^2, na.rm = TRUE))
  (mean(r, na.rm = TRUE) / dd) * sqrt(12)
}

cat("\n--- Maximum Drawdown by Regime ---\n")
analysis %>%
  group_by(Regime) %>%
  summarise(across(all_of(strat_cols), max_drawdown, .names = "MDD_{.col}")) %>%
  print()

cat("\n--- Annualised Sortino Ratios by Regime ---\n")
analysis %>%
  group_by(Regime) %>%
  summarise(across(all_of(strat_cols), sortino, .names = "Sortino_{.col}")) %>%
  print()

# =============================================================================
# 18. VALIDATION: COMPARE YOUR FACTORS TO FRENCH LIBRARY
# =============================================================================

# Important sanity check: your HML should correlate highly with FF HML
cat("\n--- Correlation of Your Factors vs. FF Library ---\n")
cat("Your HML vs FF HML:", cor(analysis$HML, analysis$hml, use = "complete.obs"), "\n")

# If correlation is > 0.9, your construction is solid
# If it's lower, check breakpoints, weighting, or rebalancing timing

# =============================================================================
# DISCONNECT FROM WRDS
# =============================================================================

dbDisconnect(wrds)

# =============================================================================
# EXPORT: Run from console
# sink("FM445_project_output.txt")
# source("FM445_project.R", echo = TRUE, max.deparse.length = Inf)
# sink()
# =============================================================================