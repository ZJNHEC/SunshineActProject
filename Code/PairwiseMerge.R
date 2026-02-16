if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, here, glue)

if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- here("Data")
}

input_dir <- file.path(data_root, "Final_Master_Tables")
output_dir <- file.path(data_root, "Merged_Pairs_Undirected") 
if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

message(">>> Loading Master Tables...")

dt_net <- fread(file.path(input_dir, "Master_Network_2015_2018.csv"))
dt_pay <- fread(file.path(input_dir, "Master_Payments_2015_2018.csv"))
dt_rx  <- fread(file.path(input_dir, "Master_Prescriptions_2015_2018.csv"))


if("source" %in% names(dt_net)) setnames(dt_net, "source", "from_npi")
if("target" %in% names(dt_net)) setnames(dt_net, "target", "to_npi")

cols_int_net <- c("Year", "from_npi", "to_npi")
dt_net[, (cols_int_net) := lapply(.SD, as.integer), .SDcols = cols_int_net]

cols_int_pay <- c("Year", "NPI")
dt_pay[, (cols_int_pay) := lapply(.SD, as.integer), .SDcols = cols_int_pay]

dt_rx[, Year := as.integer(Year)]
dt_rx[, NPI := as.integer(NPI)]

initial_rows <- nrow(dt_pay)
dt_pay <- dt_pay[Amount > 20]
message(glue(">>> Filtered Payments: Reduced from {initial_rows} to {nrow(dt_pay)} rows (kept > $20 only)."))

gc()


# Network+Payment
message(">>> Merging Network + Payments (Sequential Processing)...")

output_file_net_pay <- file.path(output_dir, "Pair_Network_Payment_Undirected.csv")
if(file.exists(output_file_net_pay)) file.remove(output_file_net_pay)

cols_net_keep <- c("Year", "from_npi", "to_npi")
years <- sort(unique(dt_net$Year))

for (y in years) {
  message(glue("    Processing Year: {y} ..."))
  net_y <- dt_net[Year == y, ..cols_net_keep]
  pay_y <- dt_pay[Year == y]
  
  if(nrow(net_y) == 0 || nrow(pay_y) == 0) {
    message("      No data for this year, skipping.")
    next
  }
  
  # A. Source
  chunk_source <- merge(
    net_y, pay_y,
    by.x = c("Year", "from_npi"),
    by.y = c("Year", "NPI"),
    all.x = TRUE, 
    allow.cartesian = TRUE 
  )
  
  if(nrow(chunk_source) > 0) {
    chunk_source[, `:=`(Role = "Source", Target_NPI = from_npi)]
    fwrite(chunk_source, output_file_net_pay, append = TRUE)
  }
  
  rm(chunk_source); gc()
  
  # B. Target
  chunk_target <- merge(
    net_y, pay_y,
    by.x = c("Year", "to_npi"),
    by.y = c("Year", "NPI"),
    all.x = TRUE, 
    allow.cartesian = TRUE
  )
  
  if(nrow(chunk_target) > 0) {
    chunk_target[, `:=`(Role = "Target", Target_NPI = to_npi)]
    fwrite(chunk_target, output_file_net_pay, append = TRUE)
  }
  rm(chunk_target, net_y, pay_y); gc()
}

message("Network + Payment merge complete.")

# Network + Prescription (too big, I didn't do this)
message(">>> Merging Network + Prescriptions (Loop per year)...")
out_file_rx <- file.path(output_dir, "Pair_Network_Prescription_Undirected.csv")
if(file.exists(out_file_rx)) file.remove(out_file_rx)

for (y in years) {
  message(glue("    Processing Year: {y}"))
  net_y <- dt_net[Year == y, ..cols_net_keep]
  rx_y  <- dt_rx[Year == y]
  
  if(nrow(net_y) == 0 || nrow(rx_y) == 0) next
  
  # A. Source
  chunk_s <- merge(net_y, rx_y, by.x = c("Year", "from_npi"), by.y = c("Year", "NPI"), 
                   all.x = TRUE, allow.cartesian = TRUE)
  if(nrow(chunk_s) > 0) {
    chunk_s[, `:=`(Role = "Source", Target_NPI = from_npi)]
    fwrite(chunk_s, out_file_rx, append = TRUE)
  }
  rm(chunk_s); gc()
  
  # B. Target
  chunk_t <- merge(net_y, rx_y, by.x = c("Year", "to_npi"), by.y = c("Year", "NPI"), 
                   all.x = TRUE, allow.cartesian = TRUE)
  if(nrow(chunk_t) > 0) {
    chunk_t[, `:=`(Role = "Target", Target_NPI = to_npi)]
    fwrite(chunk_t, out_file_rx, append = TRUE)
  }
  rm(chunk_t, net_y, rx_y); gc()
}


# Payment+Prescription
message(">>> Merging Payment + Prescription (Chunked by Year)...")

output_file_pay_rx <- file.path(output_dir, "Pair_Payment_Prescription.csv")
if(file.exists(output_file_pay_rx)) file.remove(output_file_pay_rx)

years_pay <- sort(unique(dt_pay$Year))
years_rx  <- sort(unique(dt_rx$Year))
common_years <- intersect(years_pay, years_rx)

if(exists("dt_net")) { rm(dt_net); gc() }

for (target_year in common_years) {
  message(glue("    Processing Payment+Rx Year: {target_year} ..."))
  
  pay_y <- dt_pay[Year == target_year]
  rx_y  <- dt_rx[Year == target_year]
  
  if(nrow(pay_y) == 0 || nrow(rx_y) == 0) next

  chunk_pay_rx <- merge(
    pay_y, rx_y,
    by = c("Year", "NPI"), 
    all = TRUE, 
    allow.cartesian = TRUE
  )
  
  if(nrow(chunk_pay_rx) > 0) {
    fwrite(chunk_pay_rx, output_file_pay_rx, append = TRUE)
  }
  
  rm(chunk_pay_rx, pay_y, rx_y)
  gc()
}

message(glue("All done. Output saved to: {output_dir}"))