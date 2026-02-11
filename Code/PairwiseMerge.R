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


dt_net <- fread(file.path(input_dir, "Master_Network_2014_2018.csv"))
dt_pay <- fread(file.path(input_dir, "Master_Payments_2014_2018.csv"))
dt_rx  <- fread(file.path(input_dir, "Master_Prescriptions_2014_2018.csv"))

dt_net[, Year := as.integer(Year)]
dt_pay[, Year := as.integer(Year)]
dt_rx[,  Year := as.integer(Year)]

dt_net[, `:=`(from_npi = as.integer(from_npi), to_npi = as.integer(to_npi))]
dt_pay[, NPI := as.integer(NPI)]
dt_rx[,  NPI := as.integer(NPI)]

# Network + Payment
net_pay_source <- merge(
  dt_net, dt_pay,
  by.x = c("Year", "from_npi"),
  by.y = c("Year", "NPI"),
  all.x = TRUE, 
  allow.cartesian = TRUE
)
net_pay_source[, `:=`(Role = "Source", Target_NPI = from_npi)] 

net_pay_target <- merge(
  dt_net, dt_pay,
  by.x = c("Year", "to_npi"),
  by.y = c("Year", "NPI"),
  all.x = TRUE, 
  allow.cartesian = TRUE
)
net_pay_target[, `:=`(Role = "Target", Target_NPI = to_npi)]

merged_net_pay <- rbind(net_pay_source, net_pay_target)

fwrite(merged_net_pay, file.path(output_dir, "Pair_Network_Payment_Undirected.csv"))

rm(net_pay_source, net_pay_target, merged_net_pay); gc()


#Network + Prescription (caution: very big! I didn't use it)
out_file_rx <- file.path(output_dir, "Pair_Network_Prescription_Undirected.csv")

is_first_write <- TRUE
for (y in 2014:2018) {
  message(glue("    Processing year {y}..."))
  
  net_y <- dt_net[Year == y]
  rx_y  <- dt_rx[Year == y]
  gc()

  chunk_source <- merge(
    net_y, rx_y,
    by.x = c("Year", "from_npi"),
    by.y = c("Year", "NPI"),
    all.x = TRUE, 
    allow.cartesian = TRUE
  )
  
  if (nrow(chunk_source) > 0) {
    chunk_source[, `:=`(Role = "Source", Target_NPI = from_npi)]
    
    fwrite(chunk_source, out_file_rx, append = !is_first_write, col.names = is_first_write)
    is_first_write <- FALSE 
  }

  rm(chunk_source); gc()
  chunk_target <- merge(
    net_y, rx_y,
    by.x = c("Year", "to_npi"),
    by.y = c("Year", "NPI"),
    all.x = TRUE, 
    allow.cartesian = TRUE
  )
  
  if (nrow(chunk_target) > 0) {
    chunk_target[, `:=`(Role = "Target", Target_NPI = to_npi)]

    fwrite(chunk_target, out_file_rx, append = TRUE)
  }
  
  rm(chunk_target, net_y, rx_y)
  gc() 
}


# Payment + Prescription
merged_pay_rx <- merge(
  dt_pay, dt_rx,
  by = c("Year", "NPI"),
  all = TRUE, 
  allow.cartesian = TRUE
)

fwrite(merged_pay_rx, file.path(output_dir, "Pair_Payment_Prescription.csv"))

rm(merged_pay_rx); gc()

message("All done. File in Data/Merged_Pairs_Undirected")
