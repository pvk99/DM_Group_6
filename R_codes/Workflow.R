library(readr)
library(RSQLite)
library(dplyr)

# Establishing the connection to db
my_db <- RSQLite::dbConnect(RSQLite::SQLite(),"ecommerce.db")

# Reading the Generated Data
transactions <- readr::read_csv('Data/TRANSACTIONS.csv')
ads <- readr::read_csv('Data/ADS.csv')
ads$AD_START_DATE <- as.character(ads$AD_START_DATE)
ads$AD_END_DATE <- as.character(ads$AD_START_DATE)

category <- readr::read_csv('Data/CATEGORY.csv')

customers <- readr::read_csv('Data/CUSTOMERS.csv')
customers$DATE_OF_BIRTH <- as.character(customers$DATE_OF_BIRTH)

order_shipment <- readr::read_csv('Data/ORDER_SHIPMENT.csv')

orders <- readr::read_csv('Data/ORDERS.csv')
orders$ORDER_DATE <- as.character(orders$ORDER_DATE)
orders$DELIVERY_DATE <- as.character(orders$DELIVERY_DATE)
orders$RETURN_DATE <- as.character(orders$RETURN_DATE)

promotion <- readr::read_csv('Data/PROMOTION.csv')
promotion$PROMOTION_START_DATE <- as.character(promotion$PROMOTION_START_DATE)
promotion$PROMOTION_END_DATE <- as.character(promotion$PROMOTION_END_DATE)

sku <- readr::read_csv('Data/SKU.csv')

supplier <- readr::read_csv('Data/SUPPLIER.csv')

# Writing the files to e-commerce DB

RSQLite::dbWriteTable(my_db,"ADS",ads,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"CATEGORY",category,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"CUSTOMERS",customers,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"ORDER_SHIPMENT",order_shipment,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"ORDERS",orders,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"PROMOTION",promotion,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"SKU",sku,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"SUPPLIER",supplier,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_db,"TRANSACTIONS",transactions,overwrite=FALSE,append=TRUE)


