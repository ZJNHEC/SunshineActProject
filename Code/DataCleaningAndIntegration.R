if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, here, glue, stringr)

data_root <- here("Data")
if (!dir.exists(data_root)) {
  if (dir.exists("Data")) {
    data_root <- "Data"
  } else if (dir.exists("../Data")) {
    data_root <- "../Data"
  }
}
message(glue(">>> Data root detected at: {data_root}"))

processed_dir <- file.path(data_root, "Processed_Aligned")
if(!dir.exists(processed_dir)) dir.create(processed_dir)
output_dir <- file.path(data_root, "Final_Master_Tables")
if(!dir.exists(output_dir)) dir.create(output_dir)


cols_docgraph <- c("from_npi", "to_npi", "transaction_count") 
cols_op <- c("Covered_Recipient_NPI", "Total_Amount_of_Payment_USDollars", "Nature_of_Payment_or_Transfer_of_Value", "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name")
cols_med <- c("Prscrbr_NPI", "Gnrc_Name", "Tot_Clms", "Tot_Drug_Cst")

find_col <- function(header, keywords, exclude = NULL) {
  matches <- grep(keywords, header, ignore.case = TRUE, value = TRUE)
  if (!is.null(exclude)) matches <- matches[!grepl(exclude, matches, ignore.case = TRUE)]
  if (length(matches) > 0) return(matches[1]) else return(NA)
}

# Generate 12 files
process_year_simple <- function(target_year) {
  message(glue(">>> Processing Year: {target_year}"))
  
  # A. DocGraph
  dg_path <- file.path(data_root, "DocGraph", glue("Network{target_year}.csv"))
  if(!file.exists(dg_path)) { warning("DocGraph missing"); return(NULL) }
  
  dt_network <- fread(dg_path, select = cols_docgraph)
  setnames(dt_network, old = cols_docgraph, new = c("source", "target", "transaction_count"))
  dt_network[, `:=`(source = as.integer(source), target = as.integer(target))]
  
  valid_npis <- unique(c(dt_network$source, dt_network$target))
  message(glue("      Network: {length(valid_npis)} unique NPIs loaded."))
  
  # B. Open Payments
  op_dir <- file.path(data_root, "OpenPayments", glue("Payment{target_year}"))
  op_file_path <- list.files(op_dir, pattern = "(?i)GNRL.*\\.csv$", full.names = TRUE, recursive = TRUE)[1]
  
  dt_op <- NULL
  if(!is.na(op_file_path)) {
    message(glue("      Reading OpenPayments file: {basename(op_file_path)}"))
    raw_header <- names(fread(op_file_path, nrows = 0))
    col_npi <- intersect(c("Covered_Recipient_NPI", "Physician_NPI"), raw_header)[1]
    
    if (!is.na(col_npi)) {
      col_amt <- grep("Total_Amount_of_Payment_USDollars", raw_header, value = TRUE)[1]
      col_nature <- grep("Nature_of_Payment", raw_header, value = TRUE)[1]
    
      col_payer <- grep("Applicable_Manufacturer.*Name", raw_header, value = TRUE)
      col_payer <- col_payer[!grepl("^Submitting", col_payer)][1]
      
      target_cols <- c(col_npi, col_payer, col_amt, col_nature)
      
      if(!any(is.na(target_cols))) {
        dt_op <- fread(op_file_path, select = target_cols, colClasses = list(character = col_npi))
        setnames(dt_op, old = target_cols, new = c("npi", "payer_name", "amount", "payment_nature"))
        
        dt_op[, npi := suppressWarnings(as.integer(str_trim(npi)))]
        dt_op <- dt_op[!is.na(npi) & npi %in% valid_npis]
        
        message(glue("      OpenPayments: Kept {nrow(dt_op)} rows"))
      }
    }
  }
  
  # C. Medicare
  med_path <- file.path(data_root, "MedicarePartD", glue("Prescription{target_year}.csv"))
  dt_med <- NULL
  if(file.exists(med_path)) {
    med_header <- names(fread(med_path, nrows = 0))
    actual_cols <- med_header[toupper(med_header) %in% toupper(cols_med)]
    if(length(actual_cols) > 0) {
      dt_med <- fread(med_path, select = actual_cols)
      npi_col <- grep("npi", names(dt_med), ignore.case = T, value = T)
      setnames(dt_med, old = c(npi_col, "Gnrc_Name", "Tot_Clms", "Tot_Drug_Cst"), 
               new = c("npi", "drug_name", "claim_count", "drug_cost"), skip_absent = TRUE)
      dt_med[, npi := suppressWarnings(as.integer(npi))]
      dt_med <- dt_med[npi %in% valid_npis]
      message(glue("      Medicare: Kept {nrow(dt_med)} rows"))
    }
  } 
  
  save_path <- file.path(processed_dir, glue("Aligned_Data_{target_year}.rds"))
  aligned_data <- list(network = dt_network, payments = dt_op, prescriptions = dt_med)
  saveRDS(aligned_data, save_path)
  
  rm(dt_network, dt_op, dt_med, aligned_data, valid_npis); gc()
}


for (y in 2015:2018) {
  process_year_simple(y)
}

# Generate 3 master tables
message(">>> Generating Master CSVs from corrected RDS files...")

list_network <- list()
list_payment <- list()
list_medicare <- list()

for (target_year in 2015:2018) {
  rds_path <- file.path(processed_dir, glue("Aligned_Data_{target_year}.rds"))
  
  if(file.exists(rds_path)) {
    message(glue("   Reading RDS {target_year}..."))
    data_content <- readRDS(rds_path)
    
    # Network
    if(!is.null(data_content$network)) {
      dt <- data_content$network
      dt[, Year := target_year] 
      list_network[[as.character(target_year)]] <- dt
    }
    
    # Payment
    if(!is.null(data_content$payments) && nrow(data_content$payments) > 0) {
      dt <- data_content$payments
      dt[, Year := target_year]
      setnames(dt, old = c("npi", "payer_name", "amount", "payment_nature"), 
               new = c("NPI", "Payer", "Amount", "Nature"), skip_absent = TRUE)
      list_payment[[as.character(target_year)]] <- dt
    }
    
    # Medicare
    if(!is.null(data_content$prescriptions) && nrow(data_content$prescriptions) > 0) {
      dt <- data_content$prescriptions
      dt[, Year := target_year]
      setnames(dt, old = c("npi", "drug_name", "claim_count", "drug_cost"), 
               new = c("NPI", "Drug_Name", "Claim_Count", "Drug_Cost"), skip_absent = TRUE)
      list_medicare[[as.character(target_year)]] <- dt
    }
    rm(data_content); gc()
  } else {
    warning(glue("RDS file for {target_year} not found at {rds_path}"))
  }
}

message("Writing Master Files...")

if(length(list_network) > 0) {
  fwrite(rbindlist(list_network, fill = TRUE), file.path(output_dir, "Master_Network_2015_2018.csv"))
}

if(length(list_payment) > 0) {
  fwrite(rbindlist(list_payment, fill = TRUE), file.path(output_dir, "Master_Payments_2015_2018.csv"))
  message("Master_Payments saved. Check size > 1KB.")
}

if(length(list_medicare) > 0) {
  fwrite(rbindlist(list_medicare, fill = TRUE), file.path(output_dir, "Master_Prescriptions_2015_2018.csv"))
}

message("All done.")