#This document is for filtering out representative individual payments for hypothesis development. Not for population-level analysis
library(data.table)
library(dplyr)
library(stringr)

file_path <- "E:/HEC/NetworkPaymentsPrescription/Data/OpenPayments/Payment2018/OP_DTL_GNRL_PGYR2018_P01232026_01102026.csv"

df <- fread(file_path)

strict_specialty_blacklist <- paste0(
  "(?i)Cardio|Endo|Gastro|Hema|Onco|Nephro|Neuro|Rheum|Infect|Pulmon|",
  "Psych|Uro|Ophthal|Derm|Surg|Radio|Ortho|Emergency|Anesthes|Pathology|",
  "Allergy|Immunology|Geriatric|Sports|Pain|Critical Care"
)

product_category_blacklist <- "(?i)CNS|Central Nervous System|Oncology|Hematology|Psych|Rare Disease|Biologic"

result <- df %>%
  filter(Covered_Recipient_Type == "Covered Recipient Physician") %>%
  filter(Total_Amount_of_Payment_USDollars >= 5000) %>%
  filter(str_detect(Nature_of_Payment_or_Transfer_of_Value, "(?i)Consulting|speaker|faculty") & 
         !str_detect(Nature_of_Payment_or_Transfer_of_Value, "(?i)other than consulting")) %>%
  filter(Related_Product_Indicator == "Yes") %>%
  filter(is.na(Product_Category_or_Therapeutic_Area_1) | !str_detect(Product_Category_or_Therapeutic_Area_1, product_category_blacklist)) %>%
  
  filter(str_detect(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, "(?i)Intuitive Surgical") | 
         str_detect(Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name, "(?i)Intuitive Surgical")) %>%
  filter(if_any(starts_with("Name_of_Drug_or_Biological_or_Device"), ~ str_detect(.x, "(?i)da\\s*vinci"))) %>%
  
  filter(!is.na(Contextual_Information) & nchar(str_trim(as.character(Contextual_Information))) > 10) %>%
  
  mutate(
    context_len = nchar(as.character(Contextual_Information)),
    context_len = ifelse(is.na(context_len), 0, context_len),
    info_score = context_len + 
      (ifelse(!is.na(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1), 500, 0)) + 
      (ifelse(Third_Party_Payment_Recipient_Indicator != "No Third Party Payment" & !is.na(Third_Party_Payment_Recipient_Indicator), 500, 0))
  ) %>%
  arrange(desc(info_score)) %>%
  head(1)


if(nrow(result) == 0) {
  cat("Didn't find appropriate records")
} else {
  print(as.data.frame(t(result)))
}
