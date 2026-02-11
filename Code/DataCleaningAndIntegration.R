if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, here, glue, stringr)

data_root <- here("Data")
processed_dir <- here("Data", "Processed_Aligned")
if(!dir.exists(processed_dir)) dir.create(processed_dir)
output_dir <- here("Data", "Final_Master_Tables")
if(!dir.exists(output_dir)) dir.create(output_dir)


cols_docgraph <- c("from_npi", "to_npi", "transaction_count") 

cols_op <- c("Covered_Recipient_NPI", 
             "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
             "Total_Amount_of_Payment_USDollars",
             "Nature_of_Payment_or_Transfer_of_Value")

cols_med <- c("Prscrbr_NPI", 
              "Gnrc_Name", 
              "Tot_Clms", 
              "Tot_Drug_Cst")


process_year_simple <- function(year) {
  message(glue(">>> Start processing year: {year}"))
  dg_path <- file.path(data_root, "DocGraph", glue("Network{year}.csv"))
  dt_network <- fread(dg_path, select = cols_docgraph)
  setnames(dt_network, old = cols_docgraph, new = c("source", "target", "transaction_count"))
  
  valid_npis <- unique(c(dt_network$source, dt_network$target))
  message(glue("      There are {length(valid_npis)} valid and unique NPIs"))
  
  op_dir <- file.path(data_root, "OpenPayments", glue("Payment{year}"))
  op_file_path <- list.files(op_dir, pattern = "(?i)GNRL.*\\.csv$", full.names = TRUE)[1]
  
  if(!is.na(op_file_path)) {
    raw_header <- names(fread(op_file_path, nrows = 0, header = TRUE))
    
    idx_npi <- grep("Recipient.*NPI", raw_header, ignore.case = TRUE)
    if(length(idx_npi) == 0) idx_npi <- grep("Profile_ID", raw_header, ignore.case = TRUE)
    
    idx_payer <- grep("Manufacturer|GPO", raw_header, ignore.case = TRUE)
    idx_payer <- idx_payer[!grepl("ID", raw_header[idx_payer], ignore.case = TRUE)]
    
    idx_amount <- grep("Amount", raw_header, ignore.case = TRUE)
    idx_nature <- grep("Nature", raw_header, ignore.case = TRUE)
    
    target_cols <- c(raw_header[idx_npi[1]], raw_header[idx_payer[1]], raw_header[idx_amount[1]], raw_header[idx_nature[1]])
    
    if (any(is.na(target_cols))) {
      warning(glue("      Year {year} Matching failed"))
      dt_op <- NULL
    } else {
      dt_op <- fread(op_file_path, select = target_cols)
      setnames(dt_op, old = target_cols, new = c("npi", "payer_name", "amount", "payment_nature"))
      dt_op <- dt_op[npi %in% valid_npis]
      message(glue("     Success with {nrow(dt_op)} records"))
    }
  } else {
    dt_op <- NULL
    warning(glue("      Didn't find year {year} OpenPayments"))
  }
  
  med_path <- file.path(data_root, "MedicarePartD", glue("Prescription{year}.csv"))
  
  if(file.exists(med_path)) {
    header_med <- names(fread(med_path, nrows = 0))
    actual_cols <- header_med[toupper(header_med) %in% toupper(cols_med)]
    
    dt_med <- fread(med_path, select = actual_cols)
    npi_col_name <- grep("npi", names(dt_med), ignore.case = T, value = T)
    setnames(dt_med, old = c(npi_col_name, "Gnrc_Name", "Tot_Clms", "Tot_Drug_Cst"), 
             new = c("npi", "drug_name", "claim_count", "drug_cost"), skip_absent = TRUE)
    
    dt_med <- dt_med[npi %in% valid_npis]
  } 
  
  
 #Save rds files
  save_path <- file.path(processed_dir, glue("Aligned_Data_{year}.rds"))
  aligned_data <- list(network = dt_network, payments = dt_op, prescriptions = dt_med)
  saveRDS(aligned_data, save_path)
  
  rm(dt_network, dt_op, dt_med, aligned_data, valid_npis); gc()
}

for (y in 2014:2018) {
  process_year_simple(y)
}


# Initialize
list_network <- list()
list_payment <- list()
list_medicare <- list()

find_col <- function(header, keywords, exclude = NULL) {
  matches <- grep(keywords, header, ignore.case = TRUE, value = TRUE)
  if (!is.null(exclude)) matches <- matches[!grepl(exclude, matches, ignore.case = TRUE)]
  if (length(matches) > 0) return(matches[1]) else return(NA)
}

for (year in 2014:2018) {
  message(glue("Processing year {year} "))
  
  dg_path <- file.path(data_root, "DocGraph", glue("Network{year}.csv"))
  
  if(file.exists(dg_path)) {
    dg_header <- names(fread(dg_path, nrows = 0))

    npi_cols <- grep("npi", dg_header, ignore.case = TRUE, value = TRUE)
    col_trans <- grep("transaction_count", dg_header, ignore.case = TRUE, value = TRUE)[1]
    if(is.na(col_trans)) col_trans <- grep("transaction", dg_header, ignore.case = TRUE, value = TRUE)[1]

    if(length(npi_cols) >= 2 && !is.na(col_trans)) {
      cols_read <- c(npi_cols[1], npi_cols[2], col_trans)
      dt <- fread(dg_path, select = cols_read)
      setnames(dt, old = cols_read, new = c("from_npi", "to_npi", "transaction_count"))
      
      dt[, Year := year]
      setcolorder(dt, "Year")
      list_network[[as.character(year)]] <- dt
      valid_npis <- unique(c(dt$from_npi, dt$to_npi))
    } 
  } 
  
  op_dir <- file.path(data_root, "OpenPayments", glue("Payment{year}"))
  op_file <- list.files(op_dir, pattern = "(?i)GNRL.*\\.csv$", full.names = TRUE)[1]
  
  if(!is.na(op_file) && length(valid_npis) > 0) {
    op_header <- names(fread(op_file, nrows = 0))
    col_npi <- find_col(op_header, "Recipient.*NPI|Physician.*NPI|Profile_ID")
    col_payer <- find_col(op_header, "Manufacturer|GPO", exclude = "ID")
    col_amt <- find_col(op_header, "Amount")
    col_nature <- find_col(op_header, "Nature")
    target_cols <- c(col_npi, col_payer, col_amt, col_nature)
    
    if(!any(is.na(target_cols))) {
      dt <- fread(op_file, select = target_cols)
      setnames(dt, old = target_cols, new = c("NPI", "Payer", "Amount", "Nature"))
      dt <- dt[NPI %in% valid_npis]
      dt[, Year := year]
      setcolorder(dt, c("Year", "NPI"))
      list_payment[[as.character(year)]] <- dt
      rm(dt)
    }
  }
  
  med_path <- file.path(data_root, "MedicarePartD", glue("Prescription{year}.csv"))
  
  if(file.exists(med_path) && length(valid_npis) > 0) {
    med_header <- names(fread(med_path, nrows = 0))
    col_npi   <- grep("Prscrbr_NPI", med_header, ignore.case = TRUE, value = TRUE)[1]
    col_drug  <- grep("Gnrc_Name", med_header, ignore.case = TRUE, value = TRUE)[1]
    col_claim <- grep("Tot_Clms", med_header, ignore.case = TRUE, value = TRUE)[1]
    col_cost  <- grep("Tot_Drug_Cst", med_header, ignore.case = TRUE, value = TRUE)[1]
    target_cols <- c(col_npi, col_drug, col_claim, col_cost)
    
    if(!any(is.na(target_cols))) {
      dt <- fread(med_path, select = target_cols)
      setnames(dt, old = target_cols, new = c("NPI", "Drug_Name", "Claim_Count", "Drug_Cost"))
      dt <- dt[NPI %in% valid_npis]
      dt[, Year := year]
      setcolorder(dt, c("Year", "NPI"))
      list_medicare[[as.character(year)]] <- dt
      rm(dt)
    } 
  }
  gc()
}

# Merge files

if(length(list_network) > 0) {
  message("   Writing Master Network...")
  fwrite(rbindlist(list_network), file.path(output_dir, "Master_Network_2014_2018.csv"))
}

if(length(list_payment) > 0) {
  message("   Writing Master Payments...")
  fwrite(rbindlist(list_payment), file.path(output_dir, "Master_Payments_2014_2018.csv"))
}

if(length(list_medicare) > 0) {
  message("   Writing Master Prescriptions...")
  fwrite(rbindlist(list_medicare), file.path(output_dir, "Master_Prescriptions_2014_2018.csv"))
}

message("All done")
