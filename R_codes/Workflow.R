library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Establishing the connection to db
my_db <- RSQLite::dbConnect(RSQLite::SQLite(),"ecommerce.db")

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
      print(paste("Error inserting record with primary key", paste(pk_values, collapse = ", "), "into table", table_name))
      print(e)
    })
  }
}
