library(readr)

if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- "./Data"
}

input_file <- file.path(data_root, "Merged_Pairs_Undirected", "Pair_Network_Payment_Undirected.csv")

temp_file_d <- "D:/temp_Pair_Network_Payment.csv"


process_chunk <- function(df, pos) {
  append_mode <- (pos != 1)
  
  write_csv(df, temp_file_d, append = append_mode, col_names = !append_mode)
}

read_csv_chunked(
  file = input_file,
  callback = DataFrameCallback$new(process_chunk),
  chunk_size = 1000000, 
  col_types = "cccccccc---", 
  show_col_types = FALSE
)


if (file.exists(input_file)) {
  file.remove(input_file)
}

file.copy(from = temp_file_d, to = input_file, overwrite = TRUE)
file.remove(temp_file_d)



library(readr)
library(data.table)

if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- "./Data"
}

file_pair <- file.path(data_root, "Merged_Pairs_Undirected", "Pair_Network_Payment_Undirected.csv")
file_master <- file.path(data_root, "Final_Master_Tables", "Master_Network_2015_2018.csv")

temp_file_d <- "D:/temp_Pair_Network_Merged.csv"

master_dt <- fread(file_master, 
                   select = c("Year", "source", "target", "transaction_count"),
                   colClasses = "character")

setnames(master_dt, old = c("source", "target"), new = c("from_npi", "to_npi"))

setkey(master_dt, Year, from_npi, to_npi)


process_chunk <- function(df, pos) {
  chunk_dt <- as.data.table(df)
  
  merged_chunk <- master_dt[chunk_dt, on = .(Year, from_npi, to_npi)]

  setcolorder(merged_chunk, c(names(chunk_dt), "transaction_count"))
  
  append_mode <- (pos != 1)
  write_csv(merged_chunk, temp_file_d, append = append_mode, col_names = !append_mode)
  
}

read_csv_chunked(
  file = file_pair,
  callback = DataFrameCallback$new(process_chunk),
  chunk_size = 1000000, 
  col_types = "cccccccc", 
  show_col_types = FALSE
)

rm(master_dt)
gc()

if (file.exists(file_pair)) {
  file.remove(file_pair)
}

file.copy(from = temp_file_d, to = file_pair, overwrite = TRUE)
file.remove(temp_file_d)

