# 加载必要的包
library(tidyverse)
library(data.table) # 用于极速读取大文件
library(here)

# 1. 沿用你的路径逻辑
if (dir.exists("Data")) {
  data_root <- "Data"
} else if (dir.exists("../Data")) {
  data_root <- "../Data"
} else {
  data_root <- here("Data")
}

# 2. 读取并清洗 ZIP-HRR Crosswalk
# 假设 ZipHsaHrr18.csv 包含 zipcode18 和 hrrnum 列 (Dartmouth 常见命名)
cw_path <- file.path(data_root, "ZIPHRRPair", "ZipHsaHrr18.csv")
zip_hrr_cw <- fread(cw_path) %>%
  # 确保 zip 是 5 位字符，如果原始数据是数值型，不足 5 位会丢失前导 0 (例如波士顿 02115 变成 2115)
  mutate(zip5 = str_pad(as.character(zipcode18), width = 5, side = "left", pad = "0")) %>%
  select(zip5, hrrnum) %>%
  distinct(zip5, .keep_all = TRUE) # 确保 ZIP 到 HRR 是 1:1 映射

# 3. 定义一个函数来循环处理每年的 Core 文件
process_npi_year <- function(target_year) {
  cat("Processing year:", target_year, "...\n")
  
  file_name <- paste0("core", target_year, "12.csv")
  # 根据你给的样例路径，假定在 NPIZIPPair 文件夹下
  file_path <- file.path(data_root, "NPIZIPPair", file_name) 
  
  # 使用 select 参数只读取必要的 3 列，极大节省内存
  df <- fread(file_path, select = c("npi", "entity", "ploczip")) %>%
    # entity == 1 代表 Individual (医生个人)，排除机构
    filter(entity == 1) %>%
    mutate(
      # 清洗 ploczip：截取前 5 位 (处理 Zip+4 格式，如 123456789 -> 12345)
      zip5 = substr(as.character(ploczip), 1, 5),
      year = target_year
    ) %>%
    # 与 crosswalk 合并
    left_join(zip_hrr_cw, by = "zip5") %>%
    select(year, npi, hrrnum)
  
  return(df)
}

# 4. 批量处理 2015-2018 并合并为一张长表 (Long Panel)
years <- 2015:2018
panel_list <- lapply(years, process_npi_year)

# 最终的 Panel 数据集
npi_hrr_panel <- bind_rows(panel_list)

cat("Panel Data 建立完成！总行数:", nrow(npi_hrr_panel), "\n")

# ---------------------------------------------------------
# 5. 存储 NPI-HRR Mapping Panel 备用
# ---------------------------------------------------------

# 定义存储路径，保持与你项目结构一致
mapping_dir <- file.path(data_root, "NPIHRRMapping")

# 检查文件夹是否存在，不存在则创建
if(!dir.exists(mapping_dir)) {
  dir.create(mapping_dir, recursive = TRUE)
  message("Created directory: ", mapping_dir)
}

message("Writing NPI-HRR Mapping Panel...")

# 使用 fwrite 高效存入 CSV
output_file <- file.path(mapping_dir, "Master_NPI_HRR_Mapping_2015_2018.csv")
fwrite(npi_hrr_panel, output_file)

message("Save complete! Panel data is ready at: ", output_file)

# (可选) 如果你觉得有必要，也可以把那些跨越 HRR 的医生名单存下来做鲁棒性检验 (Robustness Check)
# movers_file <- file.path(mapping_dir, "Movers_NPI_2015_2018.csv")
# fwrite(movers_analysis, movers_file)

# 计算地址变更人数
movers_analysis <- npi_hrr_panel %>%
  # 剔除未能成功匹配 HRR 的记录 (NA)
  filter(!is.na(hrrnum)) %>%
  group_by(npi) %>%
  summarise(
    unique_hrrs = n_distinct(hrrnum), # 计算每个 NPI 在 4 年内出现过的不同 HRR 数量
    years_active = n()                # 计算该医生在这 4 年中活跃的年数
  ) %>%
  ungroup()

# 统计变更人数
total_doctors <- nrow(movers_analysis)
movers_count <- movers_analysis %>% filter(unique_hrrs > 1) %>% nrow()
movers_ratio <- (movers_count / total_doctors) * 100

cat(sprintf("4年间总计医生数: %d\n", total_doctors))
cat(sprintf("跨 HRR 变更地址的医生数: %d\n", movers_count))
cat(sprintf("变更率: %.2f%%\n", movers_ratio))

# (可选) 提取出这些 Movers 的 NPI 列表，以备后续分析时作为 Control/Treatment 使用
movers_npi_list <- movers_analysis %>% filter(unique_hrrs > 1) %>% pull(npi)
