library(duckdb)
library(data.table)
library(ggplot2)
library(ggrepel)
library(scales)

if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- "./Data"
}

pay_file <- file.path(data_root, "Final_Master_Tables", "Master_Payments_2015_2018.csv")
net_file <- file.path(data_root, "Final_Master_Tables", "Master_Network_2015_2018.csv")
mapping_file <- file.path(data_root, "NPIHRRMapping", "Master_NPI_HRR_Mapping_2015_2018.csv")

npi_hrr_panel <- fread(mapping_file, colClasses = "character")
npi_hrr_panel <- unique(npi_hrr_panel[, .(year, npi, hrrnum)]) 

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
duckdb_register(con, "npi_hrr_panel", npi_hrr_panel)

sql_pay <- sprintf("
  SELECT 
    CAST(p.Year AS INTEGER) AS Year, 
    m.hrrnum, 
    SUM(CAST(p.Amount AS DOUBLE)) AS total_payment
  FROM read_csv_auto('%s', all_varchar=true) p
  JOIN npi_hrr_panel m ON p.Year = m.year AND p.NPI = m.npi
  WHERE p.NPI IS NOT NULL AND CAST(p.Amount AS DOUBLE) > 20
  GROUP BY p.Year, m.hrrnum
", pay_file)

hrr_payments <- setDT(dbGetQuery(con, sql_pay))


sql_edge <- sprintf("
  SELECT DISTINCT CAST(Year AS INTEGER) AS Year, source AS A_npi, target AS B_npi
  FROM read_csv_auto('%s', all_varchar=true)
  WHERE source IS NOT NULL AND target IS NOT NULL
", net_file)
edges_final <- setDT(dbGetQuery(con, sql_edge))

dbDisconnect(con, shutdown = TRUE)

deg_A <- edges_final[, .(deg = .N), by = .(Year, npi = A_npi)]
deg_B <- edges_final[, .(deg = .N), by = .(Year, npi = B_npi)]
physician_degree <- rbind(deg_A, deg_B)[, .(degree = sum(deg)), by = .(Year, npi)]

npi_hrr_panel[, year := as.integer(year)]

phys_mapped <- merge(physician_degree, npi_hrr_panel, 
                     by.x = c("Year", "npi"), by.y = c("year", "npi"), all.x = TRUE)

hrr_net_metrics <- phys_mapped[!is.na(hrrnum), .(
  avg_degree = mean(degree, na.rm = TRUE),
  physician_count = .N
), by = .(Year, hrrnum)]

hrr_yearly <- merge(hrr_net_metrics, hrr_payments, by = c("Year", "hrrnum"), all.x = TRUE)
hrr_yearly[is.na(total_payment), total_payment := 0]

hrr_final <- hrr_yearly[, .(
  mean_avg_degree = mean(avg_degree, na.rm = TRUE),
  mean_payment_per_capita = mean(total_payment / physician_count, na.rm = TRUE)
), by = hrrnum]

hrr_final <- hrr_final[!is.na(mean_payment_per_capita) & !is.na(mean_avg_degree)]


cor_res <- cor.test(hrr_final$mean_avg_degree, hrr_final$mean_payment_per_capita, method = "pearson")
lm_model <- lm(mean_payment_per_capita ~ mean_avg_degree, data = hrr_final)

cat(sprintf("✅ Pearson  coefficient(R): %.4f\n", cor_res$estimate))
cat(sprintf("✅ P-value: %s\n", format.pval(cor_res$p.value, eps = 0.001)))
cat(sprintf("✅ R-squared: %.4f\n", summary(lm_model)$r.squared))
cat("==========================================\n")

p_hrr <- ggplot(hrr_final, aes(x = mean_avg_degree, y = mean_payment_per_capita)) +
  geom_point(alpha = 0.7, color = "steelblue", size = 3) +
  geom_smooth(method = "lm", color = "darkred", fill = "red", alpha = 0.1, se = TRUE) +
  # 自动给排名前 5 的土豪 HRR 打上标签，看看是哪些城市
  geom_text_repel(
    data = head(hrr_final[order(-mean_payment_per_capita)], 5),
    aes(label = paste("HRR:", hrrnum)),
    size = 4, color = "black", box.padding = 0.5
  ) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "HRR Level: Physician Network Density vs. Payment Received",
    subtitle = sprintf("N = %d HRRs | Pearson R = %.3f | P-value = %s", 
                       nrow(hrr_final), cor_res$estimate, format.pval(cor_res$p.value, eps = 0.001)),
    x = "Regional Network Density\n(Average Shared Patient Connections per Physician)",
    y = "Regional Marketing Intensity\n(Average Payment per Physician in $, 2015-2018)"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12),
        panel.grid.minor = element_blank())

print(p_hrr)

#Excluding leverage points
hrr_filtered <- hrr_final[!as.character(hrrnum) %in% c("379", "412")]

cor_res_filtered <- cor.test(hrr_filtered$mean_avg_degree, hrr_filtered$mean_payment_per_capita, method = "pearson")
lm_model_filtered <- lm(mean_payment_per_capita ~ mean_avg_degree, data = hrr_filtered)

cat(sprintf("✅ Pearson  Coefficient(R): %.4f\n", cor_res_filtered$estimate))
cat(sprintf("✅ P-value: %s\n", format.pval(cor_res_filtered$p.value, eps = 0.001)))
cat(sprintf("✅ R-squared: %.4f\n", summary(lm_model_filtered)$r.squared))

p_hrr_filtered <- ggplot(hrr_filtered, aes(x = mean_avg_degree, y = mean_payment_per_capita)) +
  geom_point(alpha = 0.7, color = "steelblue", size = 3) +
  geom_smooth(method = "lm", color = "darkred", fill = "red", alpha = 0.1, se = TRUE) +
  geom_text_repel(
    data = head(hrr_filtered[order(-mean_payment_per_capita)], 5),
    aes(label = paste("HRR:", hrrnum)),
    size = 4, color = "black", box.padding = 0.5
  ) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "HRR Level: Physician Network Density vs. Payment Received (Outliers Removed)",
    subtitle = sprintf("N = %d HRRs | Pearson R = %.3f | P-value = %s", 
                       nrow(hrr_filtered), cor_res_filtered$estimate, format.pval(cor_res_filtered$p.value, eps = 0.001)),
    x = "Regional Network Density\n(Average Shared Patient Connections per Physician)",
    y = "Average Payment per Physician in $ (2015-2018)"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12),
        panel.grid.minor = element_blank())

print(p_hrr_filtered)


# Lagged model - Payment of year T and Network of year T+1

hrr_yearly[, payment_per_capita := total_payment / physician_count]

pay_t <- hrr_yearly[, .(hrrnum, Year, payment_per_capita)]
pay_t[, match_year := Year + 1] 

net_t1 <- hrr_yearly[, .(hrrnum, Year, avg_degree)]
setnames(net_t1, "Year", "match_year")

lagged_panel <- merge(
  net_t1, 
  pay_t, 
  by = c("hrrnum", "match_year"), 
  all = FALSE 
)

setnames(lagged_panel, 
         old = c("match_year", "avg_degree", "Year", "payment_per_capita"), 
         new = c("Network_Year", "Network_Density_t1", "Payment_Year", "Payment_t"))

lagged_panel <- lagged_panel[!as.character(hrrnum) %in% c("379", "412")]

cor_lagged <- cor.test(lagged_panel$Payment_t, lagged_panel$Network_Density_t1, method = "pearson")
lm_lagged <- lm(Network_Density_t1 ~ Payment_t, data = lagged_panel)

cat(sprintf("✅ Pearson  Coefficient(R): %.4f\n", cor_lagged$estimate))
cat(sprintf("✅ P-value: %s\n", format.pval(cor_lagged$p.value, eps = 0.001)))
cat(sprintf("✅ R-squared: %.4f\n", summary(lm_lagged)$r.squared))

p_lagged <- ggplot(lagged_panel, aes(x = Payment_t, y = Network_Density_t1)) +
  geom_point(alpha = 0.5, color = "darkorange", size = 2) +
  geom_smooth(method = "lm", color = "darkblue", fill = "blue", alpha = 0.1, se = TRUE) +
  scale_x_continuous(labels = comma) +
  labs(
    title = "Lagged Model: Does Payment(t) Predict Network Density(t+1)?",
    subtitle = sprintf("Pooled Panel (2015-16, 2016-17, 2017-18) | N = %d | P-value = %s", 
                       nrow(lagged_panel), format.pval(cor_lagged$p.value, eps = 0.001)),
    x = "Independent Var: Payment per Physician in Year t ($)",
    y = "Dependent Var: Network Density in Year t+1"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_lagged)

# Deal with heteroscedasticity
threshold_98 <- quantile(lagged_panel$Payment_t, 0.98)
cat(sprintf("    Threshold of 98%: $%.2f\n", threshold_98))

lagged_panel_trimmed <- lagged_panel[Payment_t <= threshold_98]

cor_trimmed <- cor.test(lagged_panel_trimmed$Payment_t, lagged_panel_trimmed$Network_Density_t1, method = "pearson")
lm_trimmed <- lm(Network_Density_t1 ~ Payment_t, data = lagged_panel_trimmed)

cat(sprintf("✅ Pearson  %.4f\n", cor_trimmed$estimate))
cat(sprintf("✅ P-value: %s\n", format.pval(cor_trimmed$p.value, eps = 0.001)))
cat(sprintf("✅ R-squared: %.4f\n", summary(lm_trimmed)$r.squared))

p_lagged_trimmed <- ggplot(lagged_panel_trimmed, aes(x = Payment_t, y = Network_Density_t1)) +
  geom_point(alpha = 0.5, color = "darkorange", size = 2) +
  geom_smooth(method = "lm", color = "darkblue", fill = "blue", alpha = 0.2, se = TRUE) +
  scale_x_continuous(labels = comma) +
  labs(
    title = "Lagged Model: Payment(t) vs Network Density(t+1) [Top 2% Outliers Removed]",
    subtitle = sprintf("Pooled Panel | N = %d | P-value = %s | R-squared = %.4f", 
                       nrow(lagged_panel_trimmed), format.pval(cor_trimmed$p.value, eps = 0.001), summary(lm_trimmed)$r.squared),
    x = "Independent Var: Payment per Physician in Year t ($)",
    y = "Dependent Var: Network Density in Year t+1"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_lagged_trimmed)

