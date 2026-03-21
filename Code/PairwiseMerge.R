library(data.table)
library(glue)

input_dir <- file.path(data_root, "Final_Master_Tables")
output_dir <- file.path(data_root, "Merged_Pairs_Undirected") 
if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

temp_file_d <- "D:/temp_Pair_Network_Payment_Undirected.csv.gz"
output_file_e <- file.path(output_dir, "Pair_Network_Payment_Undirected.csv.gz")

if(file.exists(temp_file_d)) file.remove(temp_file_d)

message(">>> Loading Master Tables...")

dt_net <- fread(file.path(input_dir, "Master_Network_2015_2018.csv"), colClasses = "character")
dt_pay <- fread(file.path(input_dir, "Master_Payments_2015_2018.csv"), colClasses = "character")
npi_hrr_panel <- fread(file.path(data_root, "NPIHRRMapping", "Master_NPI_HRR_Mapping_2015_2018.csv"), colClasses = "character")

npi_hrr_panel <- npi_hrr_panel[, .(year, npi, hrrnum)]

if("source" %in% names(dt_net)) setnames(dt_net, "source", "A_npi")
if("target" %in% names(dt_net)) setnames(dt_net, "target", "B_npi")
if("transaction_count" %in% names(dt_net)) setnames(dt_net, "transaction_count", "shared_patient")

dt_pay[, Amount := as.numeric(Amount)]
dt_pay <- dt_pay[Amount > 20]
setnames(dt_pay, old = c("NPI", "Payer", "Amount", "Nature"), new = c("receiver_npi", "payer", "amount", "nature"), skip_absent = TRUE)

message(">>> Mapping HRR...")

dt_net <- merge(dt_net, npi_hrr_panel, by.x = c("Year", "A_npi"), by.y = c("year", "npi"), all.x = TRUE)
setnames(dt_net, "hrrnum", "A_hrr")

dt_net <- merge(dt_net, npi_hrr_panel, by.x = c("Year", "B_npi"), by.y = c("year", "npi"), all.x = TRUE)
setnames(dt_net, "hrrnum", "B_hrr")

dt_pay <- merge(dt_pay, npi_hrr_panel, by.x = c("Year", "receiver_npi"), by.y = c("year", "npi"), all.x = TRUE)
setnames(dt_pay, "hrrnum", "receiver_hrr")

final_cols <- c("Year", "A_npi", "A_hrr", "B_npi", "B_hrr", "shared_patient", "payer", "amount", "nature", "receiver_npi", "receiver_hrr")

#Temporary saving in D drive
years <- sort(unique(dt_net$Year))
message(">>> Merging and writing to D Drive (GZIP compression enabled)...")

for (y in years) {
  message(sprintf("    Processing Year: %s ...", y))
  
  net_y <- dt_net[Year == y]
  pay_y <- dt_pay[Year == y]
  
  if(nrow(net_y) == 0 || nrow(pay_y) == 0) next

  chunk_A <- merge(
    net_y, pay_y,
    by.x = c("Year", "A_npi"),
    by.y = c("Year", "receiver_npi"),
    all = FALSE, 
    allow.cartesian = TRUE 
  )
  
  if(nrow(chunk_A) > 0) {
    chunk_A[, receiver_npi := A_npi]
    chunk_A <- chunk_A[, ..final_cols]
    fwrite(chunk_A, temp_file_d, append = TRUE, compress = "gzip")
  }
  rm(chunk_A); gc()
  
  chunk_B <- merge(
    net_y, pay_y,
    by.x = c("Year", "B_npi"),
    by.y = c("Year", "receiver_npi"),
    all = FALSE,
    allow.cartesian = TRUE
  )
  
  if(nrow(chunk_B) > 0) {
    chunk_B[, receiver_npi := B_npi]
    chunk_B <- chunk_B[, ..final_cols]
    fwrite(chunk_B, temp_file_d, append = TRUE, compress = "gzip")
  }
  rm(chunk_B, net_y, pay_y); gc()
}

message(">>> Moving compressed file back to E Drive...")

if(file.exists(output_file_e)) file.remove(output_file_e)
file.copy(from = temp_file_d, to = output_file_e, overwrite = TRUE)
file.remove(temp_file_d)

message(">>> Process complete.")
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