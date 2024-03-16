library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Establishing the connection to db
my_db <- RSQLite::dbConnect(RSQLite::SQLite(), "ecommerce.db")

file_paths <- list(
  "ADS" = list.files(path = "Data", pattern = "ADS.*\\.csv$", full.names = TRUE),
  "CATEGORY" = list.files(path = "Data", pattern = "CATEGORY.*\\.csv$", full.names = TRUE),
  "SUPPLIER" = list.files(path = "Data", pattern = "SUPPLIER.*\\.csv$", full.names = TRUE),
  "CUSTOMERS" = list.files(path = "Data", pattern = "CUSTOMERS.*\\.csv$", full.names = TRUE),
  "SKU" = list.files(path = "Data", pattern = "SKU.*\\.csv$", full.names = TRUE),
  "PROMOTION" = list.files(path = "Data", pattern = "PROMOTION.*\\.csv$", full.names = TRUE),
  "TRANSACTIONS" = list.files(path = "Data", pattern = "TRANSACTIONS.*\\.csv$", full.names = TRUE),
  "ORDER_SHIPMENT" = list.files(path = "Data", pattern = "ORDER_SHIPMENT.*\\.csv$", full.names = TRUE),
  "ORDERS" = list.files(path = "Data", pattern = "ORDERS.*\\.csv$", full.names = TRUE)
)

tables <- list(
  "ADS" = "AD_ID",
  "CATEGORY" = "CATEGORY_ID",
  "SUPPLIER" = "SUPPLIER_ID",
  "CUSTOMERS" = "CUSTOMER_ID",
  "SKU" = "PRODUCT_ID",
  "PROMOTION" = "PROMOTION_ID",
  "TRANSACTIONS" = "TRANSACTION_ID",
  "ORDER_SHIPMENT" = "SHIPPING_ID",
  "ORDERS" = c("ORDER_ID", "PRODUCT_ID", "CUSTOMER_ID")
)

# Define write_errors function with folder path argument
write_errors <- function(errors, folder_path, file_name) {
  # Ensure the folder exists, if not, create it
  if (!dir.exists(folder_path)) {
    dir.create(folder_path, recursive = TRUE)
  }
  
  file_path <- file.path(folder_path, file_name)
  
  if (length(errors) > 0) {
    cat("Errors:\n", file = file_path)
    for (error in errors) {
      cat(error, "\n", file = file_path, append = TRUE)
    }
    cat("\n", file = file_path, append = TRUE)
  }
}

# List to store errors
error_list <- c()

# Function to check if data entries exist and load new entries
for (table_name in names(tables)) {
  for (file_path in file_paths[[table_name]]) {
    table_data <- read_csv(file_path)  
    
    ## Apply specific rules for attributes based on the table
    if (table_name == "ADS") {
      ### Convert AD_START_DATE and AD_END_DATE to character
      table_data$AD_START_DATE <- as.character(table_data$AD_START_DATE)
      table_data$AD_END_DATE <- as.character(table_data$AD_END_DATE)
    }
    
    ## Apply specific rules for attributes based on the table
    if (table_name == "ADS") {
      ### Convert AD_START_DATE and AD_END_DATE to character
      table_data$AD_START_DATE <- as.character(table_data$AD_START_DATE)
      table_data$AD_END_DATE <- as.character(table_data$AD_END_DATE)
    }
    if (table_name == "CUSTOMERS") {
      ### Convert DATE_OF_BIRTH to character
      table_data$DATE_OF_BIRTH <- as.character(table_data$DATE_OF_BIRTH)
    }
    if (table_name == "ORDERS") {
      ### Convert ORDER_DATE, DELIVERY_DATE, RETURN_DATE to character
      table_data$ORDER_DATE <- as.character(table_data$ORDER_DATE)
      table_data$DELIVERY_DATE <- as.character(table_data$DELIVERY_DATE)
      table_data$RETURN_DATE <- as.character(table_data$RETURN_DATE)
    }  
    
    ## Check for primary key duplication
    for (i in seq_along(table_data)) {
      new_record <- table_data[i, ]
      pk_columns <- tables[[table_name]]
      pk_values <- new_record[pk_columns]
      conditions <- paste(pk_columns, "=", paste0("'", pk_values, "'"), collapse = " AND ")
      
      key_exists <- dbGetQuery(my_db, paste("SELECT COUNT(*) FROM", table_name, "WHERE", conditions))
      
      if (key_exists == 0) {
        tryCatch({
          RSQLite::dbAppendTable(my_db, table_name, new_record, overwrite = FALSE, append = TRUE)
        }, error = function(e) {
          error_list <- c(error_list, paste("Error inserting record with primary key", paste(pk_values, collapse = ", "), "into table", table_name))
          print(paste("Error inserting record with primary key", paste(pk_values, collapse = ", "), "into table", table_name))
          print(e)
        })
      } else {
        print(paste("Record with primary key", paste(pk_values, collapse = ", "), "already exists in table", table_name))
      }
    }
  }
}

# Save errors to a folder named "Error logs" within the current directory
write_errors(error_list, "Error logs", "error_log.txt")