# ==============================================================================
# 简化版数据读取与对齐脚本 (2014-2018)
# 目标：以 DocGraph 的 NPI 为核心，提取对应的 OpenPayments 和 Medicare 原始记录
# ==============================================================================

if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, here, glue, stringr)

# 1. 设置文件路径与列名 ========================================================

# 根目录自动定位到 E:/HEC/NetworkPaymentsPrescription
data_root <- here("Data")
processed_dir <- here("Data", "Processed_Aligned") # 处理后的文件放这里
if(!dir.exists(processed_dir)) dir.create(processed_dir)

# --- 定义你要读取的列 (大小写敏感，后续会统一重命名) ---

# A. DocGraph (网络关系)
# 保留 source 和 target，用于构建网络
cols_docgraph <- c("from_npi", "to_npi") 

# B. Open Payments (收钱记录 - General Payment)
# 只要: 谁收钱(NPI), 谁付钱(Manufacturer), 多少钱(Amount), 什么名目(Nature)
cols_op <- c("Covered_Recipient_NPI", 
             "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
             "Total_Amount_of_Payment_USDollars",
             "Nature_of_Payment_or_Transfer_of_Value")

# C. Medicare Part D (开药记录)
# 只要: 谁开药(NPI), 什么药(Generic Name), 开了多少(Claims), 成本多少(Cost)
cols_med <- c("Prscrbr_NPI", 
              "Gnrc_Name", 
              "Tot_Clms", 
              "Tot_Drug_Cst")

# 2. 逐年处理函数 ==============================================================

process_year_simple <- function(year) {
  message(glue(">>> 开始处理年份: {year}"))
  
  # --- Step 1: 读取 DocGraph (构建 NPI 宇宙) ---
  dg_path <- file.path(data_root, "DocGraph", glue("Network{year}.csv"))
  
  if(!file.exists(dg_path)) {
    warning(glue("找不到文件: {dg_path}"))
    return(NULL)
  }
  
  message("   1. 读取 DocGraph...")
  # 只读取 from_npi 和 to_npi
  dt_network <- fread(dg_path, select = cols_docgraph)
  
  # 统一列名: source, target
  setnames(dt_network, old = cols_docgraph, new = c("source", "target"))
  
  # 提取所有在该网络中出现过的 NPI (去重)
  # 这是我们的"白名单"，只有在这个名单里的医生，我们要保留他的付款和开药记录
  valid_npis <- unique(c(dt_network$source, dt_network$target))
  message(glue("      网络中共有 {length(valid_npis)} 个唯一的 NPI"))
  
  
  # --- Step 2: 读取并筛选 Open Payments (修复版) ---
  
  # 1. 寻找文件
  op_dir <- file.path(data_root, "OpenPayments", glue("Payment{year}"))
  # 忽略大小写，找包含 GNRL 的 csv
  op_file_path <- list.files(op_dir, pattern = "(?i)GNRL.*\\.csv$", full.names = TRUE)[1]
  
  if(!is.na(op_file_path)) {
    message(glue("   2. [读取中] OpenPayments: {basename(op_file_path)}"))
    
    # 2. 先只读表头，解决 BOM 和列名不一致问题
    # fileEncoding = "UTF-8-BOM" 是专门用来剔除那个讨厌的乱码的
    raw_header <- names(fread(op_file_path, nrows = 0, header = TRUE))
    
    # 3. 智能定位列索引 (不依赖精确的字符串匹配)
    # 逻辑：在所有列名中找关键字，返回它在第几列
    
    # [目标1] NPI 列：找同时包含 "Recipient" 和 "NPI" 的列
    idx_npi <- grep("Recipient.*NPI", raw_header, ignore.case = TRUE)
    # 如果没找到，尝试只找 "Physician_Profile_ID" (早期年份可能叫这个)
    if(length(idx_npi) == 0) idx_npi <- grep("Profile_ID", raw_header, ignore.case = TRUE)
    
    # [目标2] 付款方列：找包含 "Manufacturer" 或 "GPO" 的列
    idx_payer <- grep("Manufacturer|GPO", raw_header, ignore.case = TRUE)
    # 通常会有 ID 和 Name 两列，我们要 Name (排除 ID)
    idx_payer <- idx_payer[!grepl("ID", raw_header[idx_payer], ignore.case = TRUE)]
    
    # [目标3] 金额列：找 "Amount" 且包含 "US" 或 "Total"
    idx_amount <- grep("Amount", raw_header, ignore.case = TRUE)
    
    # [目标4] 性质列：找 "Nature"
    idx_nature <- grep("Nature", raw_header, ignore.case = TRUE)
    
    # 4. 提取最终要读取的列名 (只取每种类型的第一个匹配项)
    target_cols <- c(
      raw_header[idx_npi[1]], 
      raw_header[idx_payer[1]], 
      raw_header[idx_amount[1]], 
      raw_header[idx_nature[1]]
    )
    
    # 5. 安全检查：如果任何一列没找到 (是 NA)，就报错并跳过，防止程序崩溃
    if (any(is.na(target_cols))) {
      warning(glue("      [错误] {year} 年列名匹配失败。无法找到关键列。跳过此文件。"))
      dt_op <- NULL
    } else {
      # 6. 正式读取 (使用 select 仅仅读取我们定位到的这 4 列)
      dt_op <- fread(op_file_path, select = target_cols)
      
      # 7. 统一重命名 (按我们查找的顺序: NPI, Payer, Amount, Nature)
      setnames(dt_op, old = target_cols, 
               new = c("npi", "payer_name", "amount", "payment_nature"))
      
      # 8. 过滤
      dt_op <- dt_op[npi %in% valid_npis]
      message(glue("      [成功] 筛选后保留 {nrow(dt_op)} 条记录"))
    }
    
  } else {
    dt_op <- NULL
    warning(glue("      [跳过] 未找到 {year} 年的 OpenPayments GNRL 文件"))
  }
  
  # --- Step 3: 读取并筛选 Medicare Part D ---
  med_path <- file.path(data_root, "MedicarePartD", glue("Prescription{year}.csv"))
  
  if(file.exists(med_path)) {
    message("   3. 读取 Medicare Part D (只保留网络内的医生)...")
    
    # 为了防止列名大小写不一致，先读取 header 检查一下
    header_med <- names(fread(med_path, nrows = 0))
    # 找到实际存在的列名 (忽略大小写匹配)
    actual_cols <- header_med[toupper(header_med) %in% toupper(cols_med)]
    
    dt_med <- fread(med_path, select = actual_cols)
    
    # 统一重命名 (假设顺序一致，如果不一致需增加判断，通常 Medicare 格式较固定)
    # 强制将 NPI 列重命名为 "npi"
    npi_col_name <- grep("npi", names(dt_med), ignore.case = T, value = T)
    setnames(dt_med, old = c(npi_col_name, "Gnrc_Name", "Tot_Clms", "Tot_Drug_Cst"), 
             new = c("npi", "drug_name", "claim_count", "drug_cost"), skip_absent = TRUE)
    
    # 关键步骤：过滤！
    dt_med <- dt_med[npi %in% valid_npis]
    message(glue("      筛选后保留 {nrow(dt_med)} 条开药记录"))
    
  } else {
    dt_med <- NULL
    warning("      未找到 Medicare 文件")
  }
  
  # --- Step 4: 保存对齐后的数据 ---
  message("   4. 保存处理后的文件...")
  
  # 我们将这三张表保存为 .rds 格式 (R 的原生格式，读取速度比 CSV 快 10 倍且保留数据类型)
  # 也可以保存为 CSV，但文件会很大
  
  save_path <- file.path(processed_dir, glue("Aligned_Data_{year}.rds"))
  
  # 将三个表格打包成一个 List 保存
  aligned_data <- list(
    network = dt_network,  # 关系表 (Edge List)
    payments = dt_op,      # 属性表1 (收钱流水)
    prescriptions = dt_med # 属性表2 (开药流水)
  )
  
  saveRDS(aligned_data, save_path)
  message(glue(">>> {year} 年数据已整合保存至: {save_path}"))
  
  # 清理内存
  rm(dt_network, dt_op, dt_med, aligned_data, valid_npis)
  gc()
}

# 3. 执行循环 ==================================================================

# 依次运行 2014 到 2018
for (y in 2014:2018) {
  process_year_simple(y)
}


# ==============================================================================
# 02_Master_Table_Generator.R (最终修正版)
# 目标：生成 3 张总表 (Network, Payments, Prescriptions)
# 修复：Medicare 部分采用精准缩写匹配，OpenPayments 保持智能模糊匹配
# ==============================================================================

if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, here, glue, stringr)

# 1. 路径设置
data_root <- here("Data")
output_dir <- here("Data", "Final_Master_Tables")
if(!dir.exists(output_dir)) dir.create(output_dir)

# 2. 初始化容器
list_network <- list()
list_payment <- list()
list_medicare <- list()

# 3. 辅助函数：查找列名 (保留用于 OpenPayments)
find_col <- function(header, keywords, exclude = NULL) {
  matches <- grep(keywords, header, ignore.case = TRUE, value = TRUE)
  if (!is.null(exclude)) matches <- matches[!grepl(exclude, matches, ignore.case = TRUE)]
  if (length(matches) > 0) return(matches[1]) else return(NA)
}

# 4. 循环处理 2014-2018
for (year in 2014:2018) {
  message(glue(">>> 正在处理: {year} 年 ==============================="))
  
  # ============================================================================
  # A. DocGraph (网络)
  # ============================================================================
  dg_path <- file.path(data_root, "DocGraph", glue("Network{year}.csv"))
  
  if(file.exists(dg_path)) {
    message("   读取 DocGraph...")
    dg_header <- names(fread(dg_path, nrows = 0))
    # 找前两列包含 npi 的
    npi_cols <- grep("npi", dg_header, ignore.case = TRUE, value = TRUE)
    
    if(length(npi_cols) >= 2) {
      # 默认取前两个匹配到的 NPI 列作为 Source 和 Target
      dt <- fread(dg_path, select = c(npi_cols[1], npi_cols[2]))
      setnames(dt, old = names(dt), new = c("from_npi", "to_npi"))
      
      dt[, Year := year]
      setcolorder(dt, "Year")
      
      list_network[[as.character(year)]] <- dt
      valid_npis <- unique(c(dt$from_npi, dt$to_npi)) # 更新白名单
    } else {
      warning(glue("      [跳过] {year} DocGraph 列名异常"))
      valid_npis <- c()
    }
  } else {
    warning(glue("      [跳过] 文件不存在: {dg_path}"))
    valid_npis <- c()
  }
  
  # ============================================================================
  # B. Open Payments (保持刚才成功的模糊匹配逻辑)
  # ============================================================================
  op_dir <- file.path(data_root, "OpenPayments", glue("Payment{year}"))
  op_file <- list.files(op_dir, pattern = "(?i)GNRL.*\\.csv$", full.names = TRUE)[1]
  
  if(!is.na(op_file) && length(valid_npis) > 0) {
    message("   读取 Open Payments...")
    op_header <- names(fread(op_file, nrows = 0))
    
    # 智能定位 (解决 BOM 和列名变化)
    col_npi <- find_col(op_header, "Recipient.*NPI|Physician.*NPI|Profile_ID")
    col_payer <- find_col(op_header, "Manufacturer|GPO", exclude = "ID")
    col_amt <- find_col(op_header, "Amount")
    col_nature <- find_col(op_header, "Nature")
    
    target_cols <- c(col_npi, col_payer, col_amt, col_nature)
    
    if(!any(is.na(target_cols))) {
      dt <- fread(op_file, select = target_cols)
      setnames(dt, old = target_cols, new = c("NPI", "Payer", "Amount", "Nature"))
      
      # 过滤 + 年份
      dt <- dt[NPI %in% valid_npis]
      dt[, Year := year]
      setcolorder(dt, c("Year", "NPI"))
      
      list_payment[[as.character(year)]] <- dt
      rm(dt)
    }
  }
  
  # ============================================================================
  # C. Medicare Part D (修正：回归使用这一套标准缩写)
  #    Prscrbr_NPI, Gnrc_Name, Tot_Clms, Tot_Drug_Cst
  # ============================================================================
  med_path <- file.path(data_root, "MedicarePartD", glue("Prescription{year}.csv"))
  
  if(file.exists(med_path) && length(valid_npis) > 0) {
    message("   读取 Medicare Part D...")
    
    med_header <- names(fread(med_path, nrows = 0))
    
    # 【修正点】直接指定你要的那 4 个标准列名
    # 使用 grep 逐个寻找，以防大小写或 BOM 问题，但关键词非常具体
    col_npi   <- grep("Prscrbr_NPI", med_header, ignore.case = TRUE, value = TRUE)[1]
    col_drug  <- grep("Gnrc_Name", med_header, ignore.case = TRUE, value = TRUE)[1]
    col_claim <- grep("Tot_Clms", med_header, ignore.case = TRUE, value = TRUE)[1]
    col_cost  <- grep("Tot_Drug_Cst", med_header, ignore.case = TRUE, value = TRUE)[1]
    
    target_cols <- c(col_npi, col_drug, col_claim, col_cost)
    
    # 只有当4列都找到时才读取
    if(!any(is.na(target_cols))) {
      dt <- fread(med_path, select = target_cols)
      
      # 统一重命名
      setnames(dt, old = target_cols, new = c("NPI", "Drug_Name", "Claim_Count", "Drug_Cost"))
      
      # 过滤 + 年份
      dt <- dt[NPI %in% valid_npis]
      dt[, Year := year]
      setcolorder(dt, c("Year", "NPI"))
      
      list_medicare[[as.character(year)]] <- dt
      rm(dt)
      message("      [成功] Medicare 数据读取完毕")
    } else {
      # 打印出没找到的列，方便调试
      missing <- c("Prscrbr_NPI", "Gnrc_Name", "Tot_Clms", "Tot_Drug_Cst")[is.na(target_cols)]
      warning(glue("      [错误] {year} Medicare 缺少列: {paste(missing, collapse=', ')}"))
    }
  }
  
  gc() # 内存回收
}

# 5. 合并与保存 ================================================================
message(">>> 正在合并所有年份数据并保存...")

if(length(list_network) > 0) {
  message("   正在保存 Master Network (可能需要几分钟)...")
  fwrite(rbindlist(list_network), file.path(output_dir, "Master_Network_2014_2018.csv"))
}

if(length(list_payment) > 0) {
  message("   正在保存 Master Payments...")
  fwrite(rbindlist(list_payment), file.path(output_dir, "Master_Payments_2014_2018.csv"))
}

if(length(list_medicare) > 0) {
  message("   正在保存 Master Prescriptions...")
  fwrite(rbindlist(list_medicare), file.path(output_dir, "Master_Prescriptions_2014_2018.csv"))
}

message(">>> 全部完成！")
