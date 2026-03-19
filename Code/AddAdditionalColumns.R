#Useless file 
library(data.table)
library(glue)
library(here) 

message(">>> Preparing Environment and Loading Data...")

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

mapping_file <- file.path(data_root, "NPIHRRMapping", "Master_NPI_HRR_Mapping_2015_2018.csv")
npi_hrr_panel <- fread(mapping_file)
npi_hrr_panel[, `:=`(year = as.integer(year), npi = as.character(npi))]
npi_hrr_panel <- unique(npi_hrr_panel, by = c("year", "npi")) 

message(">>> Loading Master Tables...")
dt_pay <- fread(file.path(input_dir, "Master_Payments_2015_2018.csv"))
dt_pay[, Year := as.integer(Year)]
dt_pay[, NPI := as.character(NPI)]

dt_rx  <- fread(file.path(input_dir, "Master_Prescriptions_2015_2018.csv"))
dt_rx[, Year := as.integer(Year)]
dt_rx[, NPI := as.character(NPI)]

message(">>> Merging Payment + Prescription (Sequential Processing)...")

output_file_pay_rx <- file.path(output_dir, "Pair_Payment_Prescription.csv")
if(file.exists(output_file_pay_rx)) file.remove(output_file_pay_rx)

years_pay <- sort(unique(dt_pay$Year))
years_rx  <- sort(unique(dt_rx$Year))
common_years <- intersect(years_pay, years_rx)

if(exists("dt_net")) { rm(dt_net); gc() }

num_chunks <- 50 

for (target_year in common_years) {
  message(glue("    Processing Payment+Rx Year: {target_year} ..."))
  
  pay_y <- dt_pay[Year == target_year]
  rx_y  <- dt_rx[Year == target_year]
  
  if(nrow(pay_y) == 0 && nrow(rx_y) == 0) next
  
  all_npis <- unique(c(pay_y$NPI, rx_y$NPI))
  npi_chunks <- split(all_npis, cut(seq_along(all_npis), breaks = num_chunks, labels = FALSE))
  
  for (i in seq_along(npi_chunks)) {
    message(glue("      -> Processing chunk {i} / {num_chunks} ..."))
    
    current_npis <- npi_chunks[[i]]
    chunk_pay <- pay_y[NPI %in% current_npis]
    chunk_rx  <- rx_y[NPI %in% current_npis]
    
    if(nrow(chunk_pay) == 0 && nrow(chunk_rx) == 0) {
      rm(current_npis, chunk_pay, chunk_rx); gc()
      next
    }
    
    chunk_pay_rx <- merge(
      chunk_pay, chunk_rx,
      by = c("Year", "NPI"), 
      all = TRUE, 
      allow.cartesian = TRUE
    )
    
    if(nrow(chunk_pay_rx) > 0) {
      fwrite(chunk_pay_rx, output_file_pay_rx, append = TRUE)
    }
    
    rm(current_npis, chunk_pay, chunk_rx, chunk_pay_rx)
    gc()
  }
  rm(pay_y, rx_y, all_npis, npi_chunks); gc()
}

message(glue("All done. Output saved to: {output_file_pay_rx}"))