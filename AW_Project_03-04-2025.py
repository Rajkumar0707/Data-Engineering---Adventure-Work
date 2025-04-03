# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER LAYER 

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access Using Python Code

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awdatalakestorageraj.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awdatalakestorageraj.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awdatalakestorageraj.dfs.core.windows.net", "XXXXXXXXXXXXXXXXXX")
spark.conf.set("fs.azure.account.oauth2.client.secret.awdatalakestorageraj.dfs.core.windows.net", "XXXXXXXXXXXXXXXXXXX")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awdatalakestorageraj.dfs.core.windows.net", "https://login.microsoftonline.com/XXXXXXXXXXXXXXXXXXX/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, second, date_format, to_timestamp, to_date, lit, unix_timestamp, initcap


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading All Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Calendar Data

# COMMAND ----------

df_cal = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Calendar.csv')
display(df_cal)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Customer Data

# COMMAND ----------

df_customers = spark.read.format('csv')\
    .option('header', True)\
      .option('inferSchema', True)\
         .load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Customers.csv')
df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Product Categories Data

# COMMAND ----------

df_categories = spark.read.format('csv')\
                          .option('header' ,True)\
                            .option('inferSchema', True)\
                               .load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Product_Categories.csv')
df_categories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Product Subcategories Data

# COMMAND ----------

df_subcategories = spark.read.format('csv')\
                          .option('header', True)\
                              .option('inferSchema', True)\
                                  .load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Product_Subcategories.csv')
df_subcategories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Products Data

# COMMAND ----------

df_products = spark.read.format('csv')\
                   .option('header', True)\
                       .option('inferSchema', True)\
                           .load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Products.csv')
df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Retunrs Data

# COMMAND ----------

df_returns = spark.read.format('csv')\
                  .option('header', True)\
                      .option('inferSchema', True)\
                          .load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Returns.csv')
df_returns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading All Sales Data

# COMMAND ----------

df_sales = spark.read.format('csv')\
                  .option('header', True)\
                      .option('inferSchema', True)\
                          .load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Sales*.csv')
df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Readiong Territories Data

# COMMAND ----------

df_territories = spark.read.format('csv')\
                     .option('header', True)\
                         .option('inferSchema', True)\
                             .load('abfss://bronze@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Territories.csv')
df_territories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation

# COMMAND ----------

df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Currently we do have Date field only so we need to create additional columns like Year, Month, MonthName for further analysis

# COMMAND ----------

df_cal = df_cal.withColumn('Year', year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month', month(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal  = df_cal.withColumn('MonthName', date_format('Date','MMMM'))
df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Quarter', concat(lit("Q"), quarter('Date')))
df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now we going to push this Calendar data into the Silver Layer

# COMMAND ----------

df_cal.write.format('parquet')\
               .mode('append')\
                   .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Calendar')\
                       .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking Customers Data

# COMMAND ----------

df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Here is my business requirement I want to see the customer full name and Marrital Status should update Married or Single of M and S . Similarly in the Gender Column as well.

# COMMAND ----------

df_customers = df_customers.withColumn("FullName", initcap(concat_ws(" ",df_customers.FirstName, df_customers.LastName)))
df_customers.display()

# COMMAND ----------

df_customers = df_customers.withColumn("MaritalStatus", when(col('MaritalStatus') == 'M', 'Married')\
                                                        .when(col('MaritalStatus') == 'S', 'Single')\
                                                        .otherwise(col('MaritalStatus')))
df_customers = df_customers.withColumn('Gender', when(col('Gender') == 'M', 'Male')\
                                                   .when(col('Gender') == 'F', 'Female')\
                                                       .otherwise(col('Gender')))
df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now we going to push this Customer data into the Silver Layer 

# COMMAND ----------

df_customers.write.format('parquet')\
                  .mode('append')\
                      .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Customers')\
                          .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking Product Categories Data for the Transformation

# COMMAND ----------

df_categories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### There is no transformation required. So, we need to push back to this Product Categories Data into the Silver Layer

# COMMAND ----------

df_categories.write.format('parquet')\
                  .mode('append')\
                      .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Product_Categories')\
                          .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking Product Subcategories Data for Transformation

# COMMAND ----------

df_subcategories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### There is no transformation required. So, we need to push back to this Product SubCategories Data into the Silver Layer

# COMMAND ----------

df_subcategories.write.format('parquet')\
                       .mode('append')\
                           .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Product_Subcategories')\
                               .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking Products Data for Transformation

# COMMAND ----------

df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Here is the business requirement Needs to show only the ProductSkU first Two letters similarly in the Product Name wants to see the first part of product name

# COMMAND ----------

df_products = df_products.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])
df_products = df_products.withColumn('ProductName', split(col('ProductName'),' ')[0])
df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transformation has been completd. So, we need to push back to this Products Data Into the Silver Layer

# COMMAND ----------

df_products.write.format('parquet')\
                  .mode('append')\
                      .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Products')\
                          .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking Returns data for Transformation

# COMMAND ----------

df_returns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### There is no transformation required. So, we need to push back to this Returns Data into the Silver Layer

# COMMAND ----------

df_returns.write.format('parquet')\
                  .mode('append')\
                      .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Returns')\
                          .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Checking Sales data for Transformation

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Here is the business requirement we need to create a time stamp for Stock Date column and Needs to replace value for Order Number instead of S replace as T and then we need add additional column like Total orders

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))
df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'),'S', 'T'))
df_sales = df_sales.withColumn('TotalOrders', col('OrderLineItem') * col('OrderQuantity'))
df_sales.display()

# COMMAND ----------

df_sales = df_sales.drop('StoackDate')
df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation has been completed. So, we need to push back to this Sales data into the Silver Layer

# COMMAND ----------

df_sales.write.format('parquet')\
               .mode('append')\
                   .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Sales')\
                       .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking Territories data for Transformation

# COMMAND ----------

df_territories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### There is no transformation required. So, we need to push back to this Territories Data into the Silver Layer

# COMMAND ----------

df_territories.write.format('parquet')\
                     .mode('append')\
                         .option('path','abfss://silver@awdatalakestorageraj.dfs.core.windows.net/AdventureWorks_Territories')\
                             .save()

# COMMAND ----------

