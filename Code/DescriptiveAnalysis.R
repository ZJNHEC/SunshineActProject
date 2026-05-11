library(data.table)
library(ggplot2)
library(scales)

if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- "./Data"
}

dt_pay <- fread(file.path(data_root, "Final_Master_Tables", "Master_Payments_2015_2018.csv"), 
                select = c("Year", "NPI", "Amount", "Nature"), 
                colClasses = c(NPI="character", Amount="numeric"))
dt_pay <- dt_pay[Amount > 20 & !is.na(NPI)]

dt_pay[, Category := "Other"]
dt_pay[grepl("Food|Beverage", Nature, ignore.case=TRUE), Category := "Food"]
dt_pay[grepl("Travel|Lodging", Nature, ignore.case=TRUE), Category := "Travel"]
dt_pay[grepl("Consulting Fee", Nature, ignore.case=TRUE), Category := "Consulting"]
dt_pay[grepl("faculty or as a speaker", Nature, ignore.case=TRUE), Category := "Speaker"]

dt_target <- dt_pay[Category %in% c("Food", "Travel", "Consulting", "Speaker")]
rm(dt_pay); gc()

npi_pfile_path <- file.path(data_root, "NPPES", "npidata_pfile_20050523-20260208.csv")
npi_spec_raw <- fread(npi_pfile_path, 
                      select = c("NPI", "Healthcare Provider Taxonomy Code_1"), 
                      colClasses = c("NPI" = "character"))

pcp_prefixes <- c("207R", "208D", "2080", "2084")
npi_spec_raw[, is_specialist := 1]
npi_spec_raw[substr(`Healthcare Provider Taxonomy Code_1`, 1, 4) %in% pcp_prefixes, is_specialist := 0]

npi_specialization <- unique(npi_spec_raw[, .(npi = NPI, is_specialist)])
rm(npi_spec_raw); gc()

dt_target <- merge(dt_target, npi_specialization, by.x = "NPI", by.y = "npi", all.x = FALSE)
dt_target[, phys_type := ifelse(is_specialist == 1, "Specialist", "PCP")]

phys_cat_agg <- dt_target[, .(
  phys_annual_amount = sum(Amount, na.rm = TRUE),
  phys_annual_freq = .N
), by = .(phys_type, Category, NPI, Year)] 

calc_gini <- function(x) {
  x <- sort(x)
  n <- length(x)
  if (n <= 1 || sum(x, na.rm = TRUE) == 0) return(0)
  return((2 * sum((1:n) * x)) / (n * sum(x)) - (n + 1) / n)
}

panoramic_view <- phys_cat_agg[, .(
  Recipient_Years_Count = .N,                                  
  Total_Transactions = sum(phys_annual_freq),                
  Total_Amount = sum(phys_annual_amount),                    
  Avg_Annual_Freq_Per_Doc = mean(phys_annual_freq),            
  Median_Annual_Freq_Per_Doc = as.numeric(median(phys_annual_freq)),
  Avg_Annual_Amount_Per_Doc = mean(phys_annual_amount),      
  Median_Annual_Amount_Per_Doc = as.numeric(median(phys_annual_amount)),
  Annual_Gini_Among_Recipients = calc_gini(phys_annual_amount) 
), by = .(phys_type, Category)]

target_order <- c("Food", "Travel", "Consulting", "Speaker")
panoramic_view[, Category := factor(Category, levels = target_order)]

setorder(panoramic_view, phys_type, Category)
print(panoramic_view, digits = 2)

#Quartiles analysis
npi_hrr_panel <- fread(file.path(data_root, "NPIHRRMapping", "Master_NPI_HRR_Mapping_2015_2018.csv"), colClasses = "character")
npi_hrr_panel[, year := as.integer(year)]
npi_hrr_panel[, npi := as.character(npi)]

phys_clustering <- fread(file.path(data_root, "Network_Metrics_Export", "Physician_Local_Clustering.csv"))
phys_clustering[, npi := as.character(npi)]

phys_clustering_mapped <- merge(phys_clustering, npi_hrr_panel, by.x = c("Year", "npi"), by.y = c("year", "npi"), all.x = TRUE)
hrr_clustering_mean <- phys_clustering_mapped[!is.na(hrrnum), .(
  mean_density = mean(clustering_coef, na.rm = TRUE)
), by = .(Year, hrrnum)]

hrr_static_density <- hrr_clustering_mean[, .(Mean_Density = mean(mean_density, na.rm = TRUE)), by = hrrnum]
hrr_static_density <- hrr_static_density[!as.character(hrrnum) %in% c("379", "412")]

hrr_static_density[, Density_Quartile := as.integer(cut(Mean_Density, 
                                                        breaks = quantile(Mean_Density, probs = seq(0, 1, 0.25), na.rm = TRUE), 
                                                        include.lowest = TRUE))]

dt_target_q <- dt_target[Category %in% c("Food", "Consulting", "Speaker")]
dt_target_q[, NPI := as.character(NPI)]

dt_target_q <- merge(dt_target_q, npi_hrr_panel, by.x = c("Year", "NPI"), by.y = c("year", "npi"), all.x = TRUE)
dt_target_q <- merge(dt_target_q, hrr_static_density[, .(hrrnum, Density_Quartile)], by = "hrrnum", all.x = TRUE)

dt_target_q <- dt_target_q[!is.na(Density_Quartile)]

phys_yr_q_agg <- dt_target_q[, .(
  phys_annual_amount = sum(Amount, na.rm = TRUE),
  phys_annual_freq = .N
), by = .(Density_Quartile, phys_type, Category, NPI, Year)]

quartile_view <- phys_yr_q_agg[, .(
  Recipient_Years_Count = .N,
  Avg_Annual_Freq = mean(phys_annual_freq),
  Avg_Annual_Amount = mean(phys_annual_amount),
  Annual_Gini = calc_gini(phys_annual_amount)
), by = .(phys_type, Category, Density_Quartile)]

target_order_q <- c("Food", "Consulting", "Speaker")
quartile_view[, Category := factor(Category, levels = target_order_q)]
setorder(quartile_view, phys_type, Category, Density_Quartile)

quartile_view[, Density_Level := paste0("Q", Density_Quartile)]
quartile_view[, Density_Quartile := NULL]
setcolorder(quartile_view, c("phys_type", "Category", "Density_Level"))

print(quartile_view, digits = 2)

master_micro <- readRDS(file.path(data_root, "Physician_Level_Metrics", "Individual_Analysis_Master.rds"))

master_micro[, npi := as.character(npi)]

dt_indiv_analysis <- merge(phys_cat_agg[Category %in% c("Food", "Consulting", "Speaker")], 
                           master_micro[, .(Year, npi, degree)], 
                           by.x = c("Year", "NPI"), by.y = c("Year", "npi"), 
                           all.x = TRUE)

dt_indiv_analysis <- merge(phys_cat_agg[Category %in% c("Food", "Consulting", "Speaker")], 
                           master_micro[, .(Year, npi, degree)], 
                           by.x = c("Year", "NPI"), by.y = c("Year", "npi"), 
                           all.x = TRUE)

dt_indiv_analysis <- dt_indiv_analysis[!is.na(degree)]

dt_indiv_analysis[, Degree_Quartile := {
  if(.N < 4) as.integer(NA) 
  else as.integer(cut(degree, 
                      breaks = quantile(degree, probs = seq(0, 1, 0.25), na.rm = TRUE), 
                      include.lowest = TRUE))
}, by = .(phys_type, Category)]

indiv_quartile_view <- dt_indiv_analysis[!is.na(Degree_Quartile), .(
  Recipient_Years_Count = .N,
  Avg_Annual_Freq = mean(phys_annual_freq),
  Avg_Annual_Amount = mean(phys_annual_amount),
  Annual_Gini = calc_gini(phys_annual_amount)
), by = .(phys_type, Category, Degree_Quartile)]

target_order_indiv <- c("Food", "Consulting", "Speaker")
indiv_quartile_view[, Category := factor(Category, levels = target_order_indiv)]

indiv_quartile_view[, Degree_Level := paste0("Q", Degree_Quartile)]
setorder(indiv_quartile_view, phys_type, Category, Degree_Quartile)

setcolorder(indiv_quartile_view, c("phys_type", "Category", "Degree_Level"))
indiv_quartile_view[, Degree_Quartile := NULL]

print(indiv_quartile_view, digits = 2)
