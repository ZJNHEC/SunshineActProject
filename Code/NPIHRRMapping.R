library(tidyverse)
library(data.table) 
library(here)


if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- here("Data")
}

cw_path <- file.path(data_root, "ZIPHRRPair", "ZipHsaHrr18.csv")
zip_hrr_cw <- fread(cw_path) %>%
  mutate(zip5 = str_pad(as.character(zipcode18), width = 5, side = "left", pad = "0")) %>%
  select(zip5, hrrnum) %>%
  distinct(zip5, .keep_all = TRUE) 

process_npi_year <- function(target_year) {
  cat("Processing year:", target_year, "...\n")
  
  file_name <- paste0("core", target_year, "12.csv")
  file_path <- file.path(data_root, "NPIZIPPair", file_name) 
  
  df <- fread(file_path, select = c("npi", "entity", "ploczip")) %>%
    filter(entity == 1) %>%
    mutate(
      zip5 = substr(as.character(ploczip), 1, 5),
      year = target_year
    ) %>%
    left_join(zip_hrr_cw, by = "zip5") %>%
    select(year, npi, hrrnum)
  
  return(df)
}

years <- 2015:2018
panel_list <- lapply(years, process_npi_year)

npi_hrr_panel <- bind_rows(panel_list)

cat("Panel Data created. The number of rows:", nrow(npi_hrr_panel), "\n")


mapping_dir <- file.path(data_root, "NPIHRRMapping")

if(!dir.exists(mapping_dir)) {
  dir.create(mapping_dir, recursive = TRUE)
  message("Created directory: ", mapping_dir)
}

message("Writing NPI-HRR Mapping Panel...")

output_file <- file.path(mapping_dir, "Master_NPI_HRR_Mapping_2015_2018.csv")
fwrite(npi_hrr_panel, output_file)

message("Save complete! Panel data is ready at: ", output_file)

# Possible Robustness check for those who have changed their HRR
# movers_file <- file.path(mapping_dir, "Movers_to_npi015_2018.csv")
# fwrite(movers_analysis, movers_file)

movers_analysis <- npi_hrr_panel %>%
  filter(!is.na(hrrnum)) %>%
  group_by(npi) %>%
  summarise(
    unique_hrrs = n_distinct(hrrnum),
    years_active = n()               
  ) %>%
  ungroup()

total_doctors <- nrow(movers_analysis)
movers_count <- movers_analysis %>% filter(unique_hrrs > 1) %>% nrow()
movers_ratio <- (movers_count / total_doctors) * 100

cat(sprintf("Total physicians: %d\n", total_doctors))
cat(sprintf("Total physicians who have changed their HRR: %d\n", movers_count))
cat(sprintf("Change rate: %.2f%%\n", movers_ratio))

movers_npi_list <- movers_analysis %>% filter(unique_hrrs > 1) %>% pull(npi)
