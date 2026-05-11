library(duckdb)
library(data.table)
library(ggplot2)
library(ggrepel)
library(scales)
library(igraph)

if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- "./Data"
}

pay_file <- file.path(data_root, "Final_Master_Tables", "Master_Payments_2015_2018.csv")
net_file <- file.path("E:/HEC/NetworkPaymentsPrescription/Data/Merged_Pairs_Undirected/Pair_Network_Payment_Undirected.csv.gz")
mapping_file <- file.path(data_root, "NPIHRRMapping", "Master_NPI_HRR_Mapping_2015_2018.csv")

npi_hrr_panel <- fread(mapping_file, colClasses = "character")
npi_hrr_panel <- unique(npi_hrr_panel[, .(year, npi, hrrnum)]) 

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

dbExecute(con, "PRAGMA temp_directory='E:/duckdb_temp'") 
dbExecute(con, "PRAGMA preserve_insertion_order=FALSE")
dbExecute(con, "PRAGMA threads=4")

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
  SELECT DISTINCT 
    CAST(Year AS INTEGER) AS Year, 
    A_npi, 
    B_npi
  FROM read_csv_auto('%s', all_varchar=true)
  WHERE A_npi IS NOT NULL AND B_npi IS NOT NULL
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


# =========================================================
# Lagged model - Payment of year T and Network of year T+1
# =========================================================

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

hrr_lagged_mean <- lagged_panel[, .(
  Mean_Payment_t = mean(Payment_t, na.rm = TRUE),
  Mean_Network_Density_t1 = mean(Network_Density_t1, na.rm = TRUE)
), by = hrrnum]

cor_lagged <- cor.test(hrr_lagged_mean$Mean_Payment_t, hrr_lagged_mean$Mean_Network_Density_t1, method = "pearson")
lm_lagged <- lm(Mean_Network_Density_t1 ~ Mean_Payment_t, data = hrr_lagged_mean)

cat("\n==========================================\n")
cat("🎯 Forward Lagged Model (HRR-level Average)\n")
cat(sprintf("✅ Pearson Coefficient(R): %.4f\n", cor_lagged$estimate))
cat(sprintf("✅ P-value: %s\n", format.pval(cor_lagged$p.value, eps = 0.001)))
cat(sprintf("✅ R-squared: %.4f\n", summary(lm_lagged)$r.squared))
cat("==========================================\n")

p_lagged <- ggplot(hrr_lagged_mean, aes(x = Mean_Payment_t, y = Mean_Network_Density_t1)) +
  geom_point(alpha = 0.7, color = "darkorange", size = 2.5) +
  geom_smooth(method = "lm", color = "darkblue", fill = "blue", alpha = 0.1, se = TRUE) +
  scale_x_continuous(labels = comma) +
  labs(
    title = "Lagged Model: Mean Payment(t) vs Mean Network Degree(t+1)",
    subtitle = sprintf("HRR Level Average | N = %d | P-value = %s | Pearson R = %.4f", 
                       nrow(hrr_lagged_mean), format.pval(cor_lagged$p.value, eps = 0.001), cor_lagged$estimate),
    x = "Average Payment per Physician in Year t ($)",
    y = "Average Network Density in Year t+1"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_lagged)

# --- Deal with heteroscedasticity (Trimmed) ---
threshold_98 <- quantile(hrr_lagged_mean$Mean_Payment_t, 0.98)
cat(sprintf("    Threshold of 98%%: $%.2f\n", threshold_98))

hrr_lagged_mean_trimmed <- hrr_lagged_mean[Mean_Payment_t <= threshold_98]

cor_trimmed <- cor.test(hrr_lagged_mean_trimmed$Mean_Payment_t, hrr_lagged_mean_trimmed$Mean_Network_Density_t1, method = "pearson")
lm_trimmed <- lm(Mean_Network_Density_t1 ~ Mean_Payment_t, data = hrr_lagged_mean_trimmed)

p_lagged_trimmed <- ggplot(hrr_lagged_mean_trimmed, aes(x = Mean_Payment_t, y = Mean_Network_Density_t1)) +
  geom_point(alpha = 0.7, color = "darkorange", size = 2.5) +
  geom_smooth(method = "lm", color = "darkblue", fill = "blue", alpha = 0.2, se = TRUE) +
  scale_x_continuous(labels = comma) +
  labs(
    title = "Lagged Model [Trimmed]: Mean Payment(t) vs Mean Network Density(t+1)",
    subtitle = sprintf("HRR Level Average | N = %d | P-value = %s | R-squared = %.4f", 
                       nrow(hrr_lagged_mean_trimmed), format.pval(cor_trimmed$p.value, eps = 0.001), summary(lm_trimmed)$r.squared),
    x = "Average Payment per Physician in Year t ($)",
    y = "Average Network Density in Year t+1"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_lagged_trimmed)


net_t <- hrr_yearly[, .(hrrnum, Year, avg_degree)]
net_t[, match_year := Year + 1] 

pay_t1 <- hrr_yearly[, .(hrrnum, Year, payment_per_capita)]
setnames(pay_t1, "Year", "match_year")

lagged_panel_rev <- merge(
  pay_t1, 
  net_t, 
  by = c("hrrnum", "match_year"), 
  all = FALSE 
)

setnames(lagged_panel_rev, 
         old = c("match_year", "payment_per_capita", "Year", "avg_degree"), 
         new = c("Payment_Year", "Payment_t1", "Network_Year", "Network_Density_t"))

lagged_panel_rev <- lagged_panel_rev[!as.character(hrrnum) %in% c("379", "412")]

hrr_lagged_rev_mean <- lagged_panel_rev[, .(
  Mean_Network_Density_t = mean(Network_Density_t, na.rm = TRUE),
  Mean_Payment_t1 = mean(Payment_t1, na.rm = TRUE)
), by = hrrnum]

threshold_98_rev <- quantile(hrr_lagged_rev_mean$Mean_Payment_t1, 0.98)
hrr_lagged_rev_mean_trimmed <- hrr_lagged_rev_mean[Mean_Payment_t1 <= threshold_98_rev]

cor_lagged_rev <- cor.test(hrr_lagged_rev_mean_trimmed$Mean_Network_Density_t, hrr_lagged_rev_mean_trimmed$Mean_Payment_t1, method = "pearson")
lm_lagged_rev <- lm(Mean_Payment_t1 ~ Mean_Network_Density_t, data = hrr_lagged_rev_mean_trimmed)

cat("\n==========================================\n")
cat(sprintf("✅ Pearson Coefficient (R): %.4f\n", cor_lagged_rev$estimate))
cat(sprintf("✅ P-value: %s\n", format.pval(cor_lagged_rev$p.value, eps = 0.001)))
cat(sprintf("✅ R-squared: %.4f\n", summary(lm_lagged_rev)$r.squared))
cat("==========================================\n")

p_lagged_rev <- ggplot(hrr_lagged_rev_mean_trimmed, aes(x = Mean_Network_Density_t, y = Mean_Payment_t1)) +
  geom_point(alpha = 0.7, color = "purple", size = 2.5) +
  geom_smooth(method = "lm", color = "darkred", fill = "red", alpha = 0.2, se = TRUE) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Reverse Lagged [Trimmed]: Mean Network Density(t) vs Mean Payment(t+1)",
    subtitle = sprintf("HRR Level Average | N = %d | P-value = %s | R-squared = %.4f", 
                       nrow(hrr_lagged_rev_mean_trimmed), format.pval(cor_lagged_rev$p.value, eps = 0.001), summary(lm_lagged_rev)$r.squared),
    x = "Average Network Density in Year t",
    y = "Average Payment per Physician in Year t+1 ($)"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_lagged_rev)

# Compare the degree of those who did/did not receive payment
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
sql_pay <- sprintf("
  SELECT 
    CAST(Year AS INTEGER) AS Year, 
    NPI AS npi, 
    SUM(CAST(Amount AS DOUBLE)) AS total_payment
  FROM read_csv_auto('%s', all_varchar=true)
  WHERE NPI IS NOT NULL AND CAST(Amount AS DOUBLE) > 20
  GROUP BY Year, NPI
", pay_file)

phys_pay <- setDT(dbGetQuery(con, sql_pay))
dbDisconnect(con, shutdown = TRUE)

analysis_dt <- merge(physician_degree, phys_pay, by = c("Year", "npi"), all.x = TRUE)

analysis_dt[is.na(total_payment), total_payment := 0]

analysis_dt[, payment_status := ifelse(total_payment > 0, "Received Payment", "No Payment")]

compare_stats <- analysis_dt[, .(
  Physician_Count = .N,
  Mean_Degree = mean(degree, na.rm = TRUE),
  Median_Degree = as.numeric(median(degree, na.rm = TRUE)),
  SD_Degree = sd(degree, na.rm = TRUE)
), by = payment_status]

compare_stats <- compare_stats[order(payment_status)]
print(compare_stats)

test_result <- wilcox.test(degree ~ payment_status, data = analysis_dt, exact = FALSE)

cat(sprintf("✅ P-value: %s\n", format.pval(test_result$p.value, eps = 0.001)))


p_box <- ggplot(analysis_dt, aes(x = payment_status, y = degree, fill = payment_status)) +
  geom_boxplot(alpha = 0.7, outlier.alpha = 0.05, outlier.size = 0.5) +
  scale_y_log10(labels = comma) + 
  scale_fill_manual(values = c("No Payment" = "grey75", "Received Payment" = "steelblue")) +
  labs(
    title = "Physician Network Centrality: Paid vs. Unpaid Physicians",
    subtitle = sprintf("Wilcoxon P-value: %s", format.pval(test_result$p.value, eps = 0.001)),
    x = "",
    y = "Degree (Number of Connections, Log 10 Scale)"
  ) +
  theme_minimal() +
  theme(
    legend.position = "none",
    text = element_text(size = 14),
    axis.text.x = element_text(face = "bold")
  )

print(p_box)

# Comparison with lagged model
pay_t <- phys_pay[, .(npi, Year, total_payment)]
pay_t[, match_year := Year + 1] 

deg_t1 <- physician_degree[, .(npi, Year, degree)]
setnames(deg_t1, old = "Year", new = "match_year")

lagged_micro <- merge(deg_t1, pay_t, by = c("npi", "match_year"), all.x = TRUE)

lagged_micro[is.na(total_payment), total_payment := 0]

lagged_micro[, payment_status_t := ifelse(total_payment > 0, "Received Payment (Year t)", "No Payment (Year t)")]

lagged_stats <- lagged_micro[, .(
  Physician_Count = .N,
  Mean_Degree_t1 = mean(degree, na.rm = TRUE),
  Median_Degree_t1 = as.numeric(median(degree, na.rm = TRUE))
), by = payment_status_t]

print(lagged_stats[order(payment_status_t)])

wilcox_lagged <- wilcox.test(degree ~ payment_status_t, data = lagged_micro, exact = FALSE)

cat("\n==========================================\n")
cat(sprintf("✅ Wilcoxon P-value: %s\n", format.pval(wilcox_lagged$p.value, eps = 0.001)))
cat("==========================================\n")


#Distribution * Network Density


output_dir_metrics <- file.path(data_root, "Network_Metrics_Export")
if(!dir.exists(output_dir_metrics)) dir.create(output_dir_metrics, recursive = TRUE)

calc_gini <- function(x) {
  x <- sort(x)
  n <- length(x)
  if (n <= 1 || sum(x, na.rm = TRUE) == 0) return(0)
  return((2 * sum((1:n) * x)) / (n * sum(x)) - (n + 1) / n)
}

calc_hhi <- function(x) {
  total <- sum(x, na.rm = TRUE)
  if (total == 0) return(0)
  shares <- (x / total) * 100
  return(sum(shares^2))
}

years <- sort(unique(edges_final$Year))
clustering_list <- list()

for (y in years) {
  cat(sprintf("    Processing Year: %d ...\n", y))
  
  e_y <- edges_final[Year == y, .(A_npi, B_npi)]
  
  g <- graph_from_data_frame(e_y, directed = FALSE)
  
  cc <- transitivity(g, type = "local", isolates = "zero")
  
  clustering_list[[as.character(y)]] <- data.table(
    Year = y, 
    npi = V(g)$name, 
    clustering_coef = cc
  )
}

node_clustering <- rbindlist(clustering_list)

export_file <- file.path(output_dir_metrics, "Physician_Local_Clustering.csv")
fwrite(node_clustering, export_file)

node_clustering_mapped <- merge(
  node_clustering, 
  npi_hrr_panel, 
  by.x = c("Year", "npi"), 
  by.y = c("year", "npi"), 
  all.x = TRUE
)

hrr_clustering <- node_clustering_mapped[!is.na(hrrnum), .(
  mean_clustering = mean(clustering_coef, na.rm = TRUE)
), by = .(Year, hrrnum)]


distribution_dt <- merge(
  phys_mapped[, .(Year, npi, hrrnum)], 
  phys_pay[, .(Year, npi, total_payment)], 
  by = c("Year", "npi"), 
  all.x = TRUE
)
distribution_dt[is.na(total_payment), total_payment := 0]

hrr_distribution <- distribution_dt[, .(
  gini_index = calc_gini(total_payment),
  hhi_index = calc_hhi(total_payment),
  physician_count = .N
), by = .(Year, hrrnum)]



cross_panel <- merge(hrr_clustering, hrr_distribution, by = c("Year", "hrrnum"), all = FALSE)
cross_panel <- cross_panel[!as.character(hrrnum) %in% c("379", "412")]

hrr_cross_mean <- cross_panel[, .(
  Mean_Gini = mean(gini_index, na.rm = TRUE),
  Mean_HHI = mean(hhi_index, na.rm = TRUE),
  Mean_Clustering = mean(mean_clustering, na.rm = TRUE)
), by = hrrnum]

cor_cross <- cor.test(hrr_cross_mean$Mean_Gini, hrr_cross_mean$Mean_Clustering, method = "pearson")
lm_cross <- lm(Mean_Clustering ~ Mean_Gini, data = hrr_cross_mean)

cat(sprintf("✅ 同年模型 (Gini vs Clustering) | Pearson Coef: %.4f | P-value: %s\n", 
            cor_cross$estimate, format.pval(cor_cross$p.value, eps = 0.001)))

p_cross_gini <- ggplot(hrr_cross_mean, aes(x = Mean_Gini, y = Mean_Clustering)) +
  geom_point(alpha = 0.7, color = "seagreen", size = 2.5) +
  geom_smooth(method = "lm", color = "darkgreen", fill = "green", alpha = 0.1, se = TRUE) +
  labs(
    title = "Cross-Sectional: Payment Inequality vs Network Clustering",
    subtitle = sprintf("HRR Level Average | N = %d | Pearson Coef = %.4f | R-squared = %.4f | P-value = %s", 
                       nrow(hrr_cross_mean), cor_cross$estimate, summary(lm_cross)$r.squared, format.pval(cor_cross$p.value, eps = 0.001)),
    x = "Payment Concentration (Gini Index, 0=Equal, 1=Monopoly)",
    y = "Network Cohesiveness (Mean Clustering Coefficient)"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_cross_gini)

cor_cross_hhi <- cor.test(hrr_cross_mean$Mean_HHI, hrr_cross_mean$Mean_Clustering, method = "pearson")
lm_cross_hhi <- lm(Mean_Clustering ~ Mean_HHI, data = hrr_cross_mean)

cat(sprintf("✅ 同年模型 (HHI vs Clustering)  | Pearson Coef: %.4f | P-value: %s\n", 
            cor_cross_hhi$estimate, format.pval(cor_cross_hhi$p.value, eps = 0.001)))

p_cross_hhi <- ggplot(hrr_cross_mean, aes(x = Mean_HHI, y = Mean_Clustering)) +
  geom_point(alpha = 0.7, color = "royalblue", size = 2.5) +
  geom_smooth(method = "lm", color = "darkblue", fill = "blue", alpha = 0.1, se = TRUE) +
  labs(
    title = "Cross-Sectional: Payment Concentration (HHI) vs Network Clustering",
    subtitle = sprintf("HRR Level Average | N = %d | Pearson Coef = %.4f | R-squared = %.4f | P-value = %s", 
                       nrow(hrr_cross_mean), cor_cross_hhi$estimate, summary(lm_cross_hhi)$r.squared, format.pval(cor_cross_hhi$p.value, eps = 0.001)),
    x = "Payment Concentration (Mean HHI Index)",
    y = "Network Cohesiveness (Mean Clustering Coefficient)"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_cross_hhi)


dist_t <- hrr_distribution[, .(hrrnum, Year, gini_index, hhi_index)]
dist_t[, match_year := Year + 1]

clust_t1 <- hrr_clustering[, .(hrrnum, Year, mean_clustering)]
setnames(clust_t1, "Year", "match_year")

lagged_panel <- merge(clust_t1, dist_t, by = c("hrrnum", "match_year"), all = FALSE)
setnames(lagged_panel, 
         old = c("match_year", "mean_clustering", "Year", "gini_index", "hhi_index"), 
         new = c("Network_Year", "Clustering_t1", "Payment_Year", "Gini_t", "HHI_t"))

lagged_panel <- lagged_panel[!as.character(hrrnum) %in% c("379", "412")]

hrr_lag_mean <- lagged_panel[, .(
  Mean_Gini_t = mean(Gini_t, na.rm = TRUE),
  Mean_HHI_t = mean(HHI_t, na.rm = TRUE),
  Mean_Clustering_t1 = mean(Clustering_t1, na.rm = TRUE)
), by = hrrnum]

cor_lag <- cor.test(hrr_lag_mean$Mean_Gini_t, hrr_lag_mean$Mean_Clustering_t1, method = "pearson")
lm_lag <- lm(Mean_Clustering_t1 ~ Mean_Gini_t, data = hrr_lag_mean)

cat(sprintf("✅ 滞后模型 (Gini(t) vs Clustering(t+1)) | Pearson Coef: %.4f | P-value: %s\n", 
            cor_lag$estimate, format.pval(cor_lag$p.value, eps = 0.001)))

p_lag_gini <- ggplot(hrr_lag_mean, aes(x = Mean_Gini_t, y = Mean_Clustering_t1)) +
  geom_point(alpha = 0.7, color = "darkorange", size = 2.5) +
  geom_smooth(method = "lm", color = "darkred", fill = "red", alpha = 0.1, se = TRUE) +
  labs(
    title = "Lagged Model: Payment Concentration(T) Predicts Network Clustering(T+1)",
    subtitle = sprintf("HRR Level Average | N = %d | Pearson Coef = %.4f | R-squared = %.4f | P-value = %s", 
                       nrow(hrr_lag_mean), cor_lag$estimate, summary(lm_lag)$r.squared, format.pval(cor_lag$p.value, eps = 0.001)),
    x = "Independent Var: Payment Concentration in Year T (Mean Gini Index)",
    y = "Dependent Var: Network Cohesiveness in Year T+1 (Mean Clustering)"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_lag_gini)

cor_lag_hhi <- cor.test(hrr_lag_mean$Mean_HHI_t, hrr_lag_mean$Mean_Clustering_t1, method = "pearson")
lm_lag_hhi <- lm(Mean_Clustering_t1 ~ Mean_HHI_t, data = hrr_lag_mean)

cat(sprintf("✅ 滞后模型 (HHI(t) vs Clustering(t+1)) | Pearson Coef: %.4f | P-value: %s\n", 
            cor_lag_hhi$estimate, format.pval(cor_lag_hhi$p.value, eps = 0.001)))

p_lag_hhi <- ggplot(hrr_lag_mean, aes(x = Mean_HHI_t, y = Mean_Clustering_t1)) +
  geom_point(alpha = 0.7, color = "purple", size = 2.5) +
  geom_smooth(method = "lm", color = "darkred", fill = "red", alpha = 0.1, se = TRUE) +
  labs(
    title = "Lagged Model: Payment HHI(T) Predicts Network Clustering(T+1)",
    subtitle = sprintf("HRR Level Average | N = %d | Pearson Coef = %.4f | R-squared = %.4f | P-value = %s", 
                       nrow(hrr_lag_mean), cor_lag_hhi$estimate, summary(lm_lag_hhi)$r.squared, format.pval(cor_lag_hhi$p.value, eps = 0.001)),
    x = "Independent Var: Payment Concentration in Year T (Mean HHI)",
    y = "Dependent Var: Network Cohesiveness in Year T+1 (Mean Clustering)"
  ) +
  theme_minimal() +
  theme(text = element_text(size = 12))

print(p_lag_hhi)

# Individual level

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
duckdb_register(con, "npi_hrr_panel", npi_hrr_panel)

sql_indiv_pay <- sprintf("
  SELECT 
    CAST(Year AS INTEGER) AS Year, 
    NPI AS npi,
    SUM(CASE WHEN Nature LIKE '%%Food%%' THEN CAST(Amount AS DOUBLE) ELSE 0 END) AS pay_food,
    SUM(CASE WHEN Nature LIKE '%%Consulting%%' THEN CAST(Amount AS DOUBLE) ELSE 0 END) AS pay_consulting,
    SUM(CASE WHEN (Nature LIKE '%%Travel%%' OR Nature LIKE '%%Lodging%%') THEN CAST(Amount AS DOUBLE) ELSE 0 END) AS pay_travel,
    SUM(CAST(Amount AS DOUBLE)) AS pay_total
  FROM read_csv_auto('%s', all_varchar=true)
  WHERE NPI IS NOT NULL AND CAST(Amount AS DOUBLE) > 20
  GROUP BY Year, NPI
", pay_file)

phys_pay_types <- setDT(dbGetQuery(con, sql_indiv_pay))
dbDisconnect(con, shutdown = TRUE)


bet_list <- list()

for (y in years) {
  cat(sprintf("    Calculating Betweenness for Year: %d ...\n", y))
  e_y <- edges_final[Year == y, .(A_npi, B_npi)]
  g <- graph_from_data_frame(e_y, directed = FALSE)
  
  bet <- betweenness(g, directed = FALSE, normalized = TRUE)
  
  bet_list[[as.character(y)]] <- data.table(
    Year = y,
    npi = V(g)$name,
    betweenness = bet
  )
}
phys_betweenness <- rbindlist(bet_list)

output_dir_indiv <- file.path(data_root, "Physician_Level_Metrics")
if(!dir.exists(output_dir_indiv)) dir.create(output_dir_indiv, recursive = TRUE)

fwrite(phys_betweenness, file.path(output_dir_indiv, "Physician_Betweenness_Centrality.csv"))
cat(sprintf("✅ 医生个体 Betweenness 结果已保存至: %s\n", output_dir_indiv))

indiv_analysis_dt <- merge(physician_degree, phys_betweenness, by = c("Year", "npi"), all.x = TRUE)
indiv_analysis_dt <- merge(indiv_analysis_dt, phys_pay_types, by = c("Year", "npi"), all.x = TRUE)

cols_to_fix <- c("pay_food", "pay_consulting", "pay_travel", "pay_total", "betweenness")
for (col in cols_to_fix) indiv_analysis_dt[is.na(get(col)), (col) := 0]

indiv_analysis_dt[, `:=`(
  has_food = as.integer(pay_food > 0),
  has_consulting = as.integer(pay_consulting > 0),
  has_travel = as.integer(pay_travel > 0)
)]

mag_cols <- c("degree", "betweenness", "pay_food", "pay_consulting", "pay_travel")
cor_matrix <- cor(indiv_analysis_dt[, ..mag_cols], method = "spearman")

cat("\n--- 个体中心度与支付金额的相关性 (Spearman) ---\n")
print(round(cor_matrix, 4))

compare_types <- indiv_analysis_dt[, .(
  N = .N,
  Mean_Degree = mean(degree),
  Mean_Betweenness = mean(betweenness)
), by = .(has_food, has_consulting, has_travel)][order(-Mean_Degree)]

cat("\n--- 不同 Payment 组合下的平均网络地位 ---\n")
print(compare_types)

model_deg <- lm(log1p(degree) ~ pay_food + pay_consulting + pay_travel, data = indiv_analysis_dt)
model_bet <- lm(betweenness ~ pay_food + pay_consulting + pay_travel, data = indiv_analysis_dt)

print_model_stats <- function(model, label) {
  f <- summary(model)$fstatistic
  p_val <- pf(f[1], f[2], f[3], lower.tail = FALSE)
  cat(sprintf("\n--- %s ---\n", label))
  cat(sprintf("Adjusted R-squared: %.4f | Model P-value: %s\n", 
              summary(model)$adj.r.squared, format.pval(p_val, eps = 0.001)))
  print(summary(model)$coefficients)
}

print_model_stats(model_deg, "回归分析：哪种钱最能预测 Degree (log)?")
print_model_stats(model_bet, "回归分析：哪种钱最能预测 Betweenness?")

melt_dt <- melt(indiv_analysis_dt, 
                id.vars = "degree", 
                measure.vars = c("has_food", "has_consulting", "has_travel"),
                variable.name = "Payment_Type", value.name = "Received")

p_indiv <- ggplot(melt_dt[Received == 1], aes(x = Payment_Type, y = degree, fill = Payment_Type)) +
  geom_boxplot(outlier.alpha = 0.1, alpha = 0.7) +
  scale_y_log10(labels = comma) +
  labs(title = "Network Degree by Type of Payment Received",
       subtitle = "Physicians receiving Consulting Fees tend to occupy more central positions",
       x = "Payment Category", y = "Degree (Log Scale)") +
  theme_minimal() + theme(legend.position = "none")

print(p_indiv)

melt_bet_dt <- melt(indiv_analysis_dt, 
                    id.vars = "betweenness", 
                    measure.vars = c("has_food", "has_consulting", "has_travel"),
                    variable.name = "Payment_Type", value.name = "Received")

# 可视化 Betweenness
# 注意：由于 Betweenness 数值极小（10^-8 级别），我们使用科学计数法显示 Y 轴
p_indiv_bet <- ggplot(melt_bet_dt[Received == 1], aes(x = Payment_Type, y = betweenness, fill = Payment_Type)) +
  geom_boxplot(outlier.alpha = 0.05, alpha = 0.7) +
  scale_y_continuous(labels = scientific_format()) + 
  labs(title = "Network Betweenness by Type of Payment Received",
       subtitle = "Betweenness measures a physician's role as a bridge between groups",
       x = "Payment Category", y = "Normalized Betweenness Centrality") +
  theme_minimal() + 
  theme(legend.position = "none", text = element_text(size = 12))

print(p_indiv_bet)


# 保存当前所有的个体层面分析结果 (包含度、中心度、三类钱)
saveRDS(indiv_analysis_dt, file.path(data_root, "Physician_Level_Metrics", "Individual_Analysis_Master.rds"))

# 如果你想保存之前算好的 HRR 层面结果
saveRDS(hrr_lag_mean, file.path(data_root, "Network_Metrics_Export", "HRR_Lagged_Analysis.rds"))

cat(">>> 关键分析数据已保存为 .rds 格式。下次只需用 readRDS() 加载。")


library(hexbin)

# =========================================================
# Part 4: Individual-Level Cross-Sectional & Lagged Models
# =========================================================
cat("\n>>> 正在运行个体层级大规模回归分析 (NPI Level)...\n")

# --- 1. 同年横截面模型 (Cross-Sectional: Pay T vs. Cent T) ---
# 使用 log1p 处理极度偏态的金额和度
model_cross_indiv <- lm(log1p(degree) ~ log1p(pay_total), data = indiv_analysis_dt)

cat("\n--- 个体同年模型: log(Pay) -> log(Degree) ---\n")
print_model_stats(model_cross_indiv, "Individual Cross-Sectional Model")

# --- 2. 滞后预测模型 (Lagged: Pay T -> Cent T+1) ---
# 准备滞后数据：T 年的钱匹配 T+1 年的中心度
pay_npi_t <- phys_pay_types[, .(npi, Year, pay_total)]
pay_npi_t[, match_year := Year + 1]

cent_npi_t1 <- indiv_analysis_dt[, .(npi, Year, degree, betweenness)]
setnames(cent_npi_t1, "Year", "match_year")

lagged_indiv_dt <- merge(cent_npi_t1, pay_npi_t, by = c("npi", "match_year"), all = FALSE)

# 滞后回归
model_lag_indiv_deg <- lm(log1p(degree) ~ log1p(pay_total), data = lagged_indiv_dt)
model_lag_indiv_bet <- lm(betweenness ~ log1p(pay_total), data = lagged_indiv_dt)

cat("\n--- 个体滞后模型: Pay(T) -> Degree(T+1) ---\n")
print_model_stats(model_lag_indiv_deg, "Individual Lagged Model (Degree)")

cat("\n--- 个体滞后模型: Pay(T) -> Betweenness(T+1) ---\n")
print_model_stats(model_lag_indiv_bet, "Individual Lagged Model (Betweenness)")

# --- 3. 可视化：大规模数据下的趋势 ---
# 使用 geom_hex 代替 geom_point，颜色深浅代表该区域点的密度
p_indiv_mass <- ggplot(indiv_analysis_dt[pay_total > 0], aes(x = log1p(pay_total), y = log1p(degree))) +
  geom_hex(bins = 50) +
  geom_smooth(method = "lm", color = "red", size = 1.2) +
  scale_fill_gradient(low = "lightblue", high = "darkblue", name = "Physician Count") +
  labs(
    title = "Individual Level: Total Payment vs. Network Degree",
    subtitle = sprintf("N = %d physicians | Pearson Coef = %.4f", 
                       nrow(indiv_analysis_dt[pay_total > 0]), 
                       cor(log1p(indiv_analysis_dt$pay_total), log1p(indiv_analysis_dt$degree), method = "spearman")),
    x = "Total Payment Amount Received (log1p scale)",
    y = "Network Degree (log1p scale)"
  ) +
  theme_minimal()

print(p_indiv_mass)

# --- 4. 结果保存 ---
saveRDS(lagged_indiv_dt, file.path(data_root, "Physician_Level_Metrics", "Individual_Lagged_Master.rds"))
cat("\n✅ 个体层级滞后分析底表已保存。")



# =========================================================
# Part 5: Specialty Analysis (PCP vs. Specialist) & HRR Density
# =========================================================
cat("\n>>> 开始专科层级分析：PCP vs. Specialist 支付分布与网络凝聚力...\n")

# 1. 恢复之前运行过的指标 (减少重复计算)
# ---------------------------------------------------------
# 加载个体分析主表
indiv_analysis_dt <- readRDS(file.path(data_root, "Physician_Level_Metrics", "Individual_Analysis_Master.rds"))

# 加载之前算好的 Clustering Coefficient (Density)
phys_clustering <- fread(file.path(data_root, "Network_Metrics_Export", "Physician_Local_Clustering.csv"))

# 2. 处理 NPI PFile 提取专科分类
# ---------------------------------------------------------
# 【已修改路径】直接引用你的 NPPES 文件
npi_pfile_path <- file.path(data_root, "NPPES", "npidata_pfile_20050523-20260208.csv")

cat(">>> 正在读取 NPI PFile (仅提取 NPI 和 Taxonomy)... \n")
# 仅读取必要的两列：NPI 和第一个 Taxonomy Code
npi_spec_raw <- fread(npi_pfile_path, 
                      select = c("NPI", "Healthcare Provider Taxonomy Code_1"), 
                      colClasses = c("NPI" = "character"))

# 定义 PCP 的 Taxonomy 代码前缀 (一般内科、家庭医学、全科、儿科)
pcp_prefixes <- c("207R", "208D", "2080", "2084")

# 创建分类标签：PCP = 0, Specialist = 1
npi_spec_raw[, is_specialist := 1]
npi_spec_raw[substr(`Healthcare Provider Taxonomy Code_1`, 1, 4) %in% pcp_prefixes, is_specialist := 0]

# 提取唯一的 NPI-Specialization 映射
npi_specialization <- unique(npi_spec_raw[, .(npi = NPI, is_specialist)])
rm(npi_spec_raw); gc()

# 3. 数据合并与 HRR 层级聚合
# ---------------------------------------------------------
# 合并专科信息
indiv_analysis_dt <- merge(indiv_analysis_dt, npi_specialization, by = "npi", all.x = TRUE)

npi_hrr_panel[, year := as.integer(year)]

# 合并 HRR 映射 (确保有 hrrnum)
indiv_analysis_dt <- merge(indiv_analysis_dt, npi_hrr_panel, 
                           by.x = c("Year", "npi"), by.y = c("year", "npi"), all.x = TRUE)

# 计算各 HRR 的支付人数占比
hrr_spec_stats[, `:=`(
  # 原有的渗透率 (解释：这群人里有多少拿了钱)
  frac_spec_paid = count_spec_paid / count_spec,
  frac_pcp_paid = count_pcp_paid / count_pcp,
  
  # 新增的构成比 (解释：在所有拿钱的人里，这一群人占多少。这两者之和永远等于 1)
  share_spec_in_paid = count_spec_paid / (count_spec_paid + count_pcp_paid),
  share_pcp_in_paid = count_pcp_paid / (count_spec_paid + count_pcp_paid)
)]

npi_hrr_panel[, npi := as.integer(npi)]

# 合并网络 Density (Clustering)
# 我们需要先为 phys_clustering 匹配 HRR
phys_clustering_mapped <- merge(phys_clustering, npi_hrr_panel, 
                                by.x = c("Year", "npi"), by.y = c("year", "npi"), all.x = TRUE)

hrr_clustering_mean <- phys_clustering_mapped[!is.na(hrrnum), .(
  mean_density = mean(clustering_coef, na.rm = TRUE)
), by = .(Year, hrrnum)]

final_spec_panel <- merge(hrr_spec_stats, hrr_clustering_mean, by = c("Year", "hrrnum"))

# 时间平均：307 个 HRR 数据点
hrr_final_spec <- final_spec_panel[, .(
  Mean_Density = mean(mean_density, na.rm = TRUE),
  Frac_Spec_Paid = mean(frac_spec_paid, na.rm = TRUE),
  Frac_PCP_Paid = mean(frac_pcp_paid, na.rm = TRUE),
  Share_Spec_Paid = mean(share_spec_in_paid, na.rm = TRUE),
  Share_PCP_Paid = mean(share_pcp_in_paid, na.rm = TRUE)
), by = hrrnum]
# 4. 回归分析
# ---------------------------------------------------------
hrr_final_spec <- hrr_final_spec[!as.character(hrrnum) %in% c("379", "412")]

# 模型 1: Specialist 支付比例 vs. 密度
cor_spec <- cor.test(hrr_final_spec$Frac_Spec_Paid, hrr_final_spec$Mean_Density)
lm_spec <- lm(Frac_Spec_Paid ~ Mean_Density, data = hrr_final_spec)

cat("\n--- HRR Level: Network Density vs. Fraction of Specialists Paid ---\n")
cat(sprintf("Pearson Coef: %.4f | R-squared: %.4f | P-value: %s\n", 
            cor_spec$estimate, summary(lm_spec)$r.squared, format.pval(cor_spec$p.value, eps = 0.001)))

# 模型 2: PCP 支付比例 vs. 密度
cor_pcp <- cor.test(hrr_final_spec$Frac_PCP_Paid, hrr_final_spec$Mean_Density)
lm_pcp <- lm(Frac_PCP_Paid ~ Mean_Density, data = hrr_final_spec)

cat("\n--- HRR Level: Network Density vs. Fraction of PCPs Paid ---\n")
cat(sprintf("Pearson Coef: %.4f | R-squared: %.4f | P-value: %s\n", 
            cor_pcp$estimate, summary(lm_pcp)$r.squared, format.pval(cor_pcp$p.value, eps = 0.001)))

# 5. 可视化对比：在拿钱的医生池子里，大蛋糕是怎么分的？
p_share_pcp <- ggplot(hrr_final_spec) +
  geom_smooth(aes(x = Mean_Density, y = Share_Spec_Paid, color = "Specialists"), method = "lm", se = TRUE) +
  geom_smooth(aes(x = Mean_Density, y = Share_PCP_Paid, color = "PCPs"), method = "lm", se = TRUE) +
  labs(title = "Composition of Paid Physicians by Network Density",
       subtitle = "Among all physicians receiving payments, how does the proportion of Specialists vs. PCPs shift?",
       x = "Network Cohesiveness (Clustering Coefficient)",
       y = "Share among ALL Paid Physicians (Sum = 1)",
       color = "Physician Type") +
  theme_minimal()

print(p_share_pcp)

print(p_spec_pcp)
