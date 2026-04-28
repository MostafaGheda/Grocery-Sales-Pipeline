# Grocery-Sales-Pipeline
                    ETL PIPELINE FOR GROCERY SALES DATA WAREHOUSE
                            Documentation File
================================================================================

PROJECT OVERVIEW
================================================================================

This project implements a complete ETL (Extract, Transform, Load) pipeline for a 
grocery sales data warehouse. The pipeline extracts data from a staging database 
(GrocerySalesStage), transforms it according to data warehouse modeling best 
practices, and loads it into a dimensional data warehouse (GrocerySales_DWH). 
The fact table loading process is BATCHED to efficiently handle large volumes 
of data without memory issues.

================================================================================
ARCHITECTURE
================================================================================

[Staging Database] -> [ETL Pipeline] -> [Data Warehouse]
(GrocerySalesStage)    (Python)         (GrocerySales_DWH)

Source Tables:
- customers
- employees  
- countries
- cities
- products
- categories
- sales

Target Tables:
- Dim_Customers
- Dim_Employees
- Dim_Cities
- Dim_Products
- Fact_Sales

Technology Stack:
- Python - Core ETL logic
- Pandas - Data manipulation and transformation
- SQLAlchemy - Database connectivity
- PyODBC - SQL Server driver
- Microsoft SQL Server - Source and destination databases

================================================================================
DATABASE SCHEMA
================================================================================

SOURCE TABLES (GrocerySalesStage):

Table: customers
- CustomerID, CityID, FirstName, MiddleInitial, LastName, Address

Table: employees  
- EmployeeID, CityID, FirstName, MiddleInitial, LastName, BirthDate, Gender, HireDate

Table: countries
- CountryID, CountryCode, CountryName

Table: cities
- CityID, CityName, Zipcode, CountryID

Table: products
- ProductID, ProductName, Price, CategoryID, Class, ModifyDate, Resistant, IsAllergic, VitalityDays

Table: categories
- CategoryID, CategoryName, Class

Table: sales
- SalesID, SalesPersonID, CustomerID, CityID, ProductID, Quantity, Discount, SalesDate, TransactionNumber

TARGET TABLES (GrocerySales_DWH):

Table: Dim_Customers (SCD Type 1)
- customer_sk (surrogate key), customer_pk (business key), first_name, middle_initial, last_name, address

Table: Dim_Employees (SCD Type 1)
- employee_sk (surrogate key), employee_pk (business key), first_name, middle_initial, last_name, birth_date, gender, hire_date

Table: Dim_Cities (SCD Type 1)
- country_city_sk (surrogate key), city_id (business key), city_name, zip_code, country_pk, country_name

Table: Dim_Products (SCD Type 2)
- product_sk (surrogate key), product_pk (business key), product_name, price, category_id, class, modify_date, resistant, is_allergic, vitality_days, category_name, is_current, end_time

Table: Fact_Sales
- sales_pk, employee_fk, customer_fk, product_fk, city_fk, date_key, time_key, quantity, price, discount, total_sales, transaction_number

================================================================================
ETL PROCESS FLOW
================================================================================

PHASE 1: DIMENSION EXTRACTION
- extract_dim() -> customers_df, employees_df, countries_df, cities_df, products_df, categories_df

PHASE 2: DIMENSION TRANSFORMATION
- transform_dim() -> dim_customers, dim_employees, dim_cities, dim_products
- Merges cities with countries
- Merges products with categories
- Drops unnecessary staging keys
- Converts date formats and data types

PHASE 3: DIMENSION LOADING
- load_dim() -> Loads all dimensions into DWH
- Uses SCD Type 1 for Customers, Employees, Cities
- Uses SCD Type 2 for Products

PHASE 4: FACT EXTRACTION (BATCHED)
- extract_fact_batched() -> Batched extraction from sales table
- Identifies last loaded SalesID
- Processes data in configurable batch sizes
- Prevents memory overflow for large datasets

PHASE 5: FACT TRANSFORMATION & LOADING
- transform_fact_chunk() + load_fact_chunk() -> Insert into Fact_Sales
- Converts data types
- Calculates total_sales = Price x Quantity x (1 - Discount)
- Maps business keys to surrogate keys from dimensions
- Inserts only new records (avoids duplicates)

================================================================================
SLOWLY CHANGING DIMENSIONS (SCD)
================================================================================

SCD TYPE 1 - OVERWRITE
Used for: Dim_Customers, Dim_Employees, Dim_Cities

Behavior:
- Updates existing records in-place
- Does not maintain history
- Old values are permanently lost

Implementation:
MERGE target USING source
ON target.business_key = source.business_key
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...

SCD TYPE 2 - HISTORICAL TRACKING
Used for: Dim_Products

Behavior:
- Creates new record for each change
- Uses is_current flag (1 = active, 0 = historical)
- Uses end_time to track when record became inactive

Implementation:
-- Expire old records
UPDATE target SET is_current = 0, end_time = GETDATE()
FROM Dim_Products target
INNER JOIN #temp source ON target.product_pk = source.product_pk
WHERE target.is_current = 1 AND (changes detected)

-- Insert new current record
MERGE Dim_Products target
USING #temp source
ON target.product_pk = source.product_pk AND target.is_current = 1
WHEN NOT MATCHED THEN INSERT ...

================================================================================
BATCHED FACT PROCESSING
================================================================================

WHY BATCHING?
- Memory overflow with millions of rows -> Process in chunks of 50,000-100,000 rows
- Long-running transactions -> Smaller, faster transactions
- No checkpoint/resume capability -> Incremental based on SalesID

BATCH CONFIGURATION:
batch_size = 100000  # Adjust based on available RAM

INCREMENTAL LOADING LOGIC:
-- Track last loaded record
SELECT ISNULL(MAX(sales_pk), 0) FROM Fact_Sales

-- Only fetch new records
WHERE s.SalesID > {last_id}

-- Batch using OFFSET/FETCH
ORDER BY s.SalesID
OFFSET {offset} ROWS
FETCH NEXT {batch_size} ROWS ONLY

DUPLICATE PREVENTION:
INSERT INTO Fact_Sales (...)
SELECT ... FROM staging
WHERE NOT EXISTS (
    SELECT 1 FROM Fact_Sales tgt 
    WHERE tgt.sales_pk = src.sales_pk
)

================================================================================
CONFIGURATION
================================================================================

DATABASE CONNECTION:
Modify the connection parameters in Run_ETL_Pipeline():

server = 'YOUR_SERVER_NAME'     # Use '.' for localhost
database = 'GrocerySalesStage'   # Source database
dwh = 'GrocerySales_DWH'         # Target data warehouse

DRIVER CONFIGURATION:
For Windows: driver='ODBC+Driver+17+for+SQL+Server'
For Linux/macOS: driver='ODBC+Driver+17+for+SQL+Server' (may need different name)

================================================================================
USAGE
================================================================================

RUNNING THE PIPELINE:
python etl_pipeline.py

EXPECTED OUTPUT:
=== Starting Dimensions Extraction ===
=== Starting Dimensions Transformation ===
=== Starting Dimensions Load ===

--- Loading Dim_Customers (SCD Type 1) ---
--- Loading Dim_Employees (SCD Type 1) ---
--- Loading Dim_Cities (SCD Type 1) ---
--- Loading Dim_Products (SCD Type 2) ---

=== Dimensions are Loaded Successfully ===

=== Starting Batched Fact Extraction ===

Last loaded Sales ID: 1,234,567
Total rows to process: 500,000

--- Processing Batch 1 (Rows 0 to 100,000) ---
Loaded 100,000 rows into DataFrame
Batch 1 complete: 100,000 rows inserted
Total inserted so far: 100,000

--- Processing Batch 2 (Rows 100,000 to 200,000) ---
...

=== Fact Loading Completed: 500,000 total rows inserted ===

=======================================
= ETL Pipeline Completed Successfully =
=======================================

================================================================================
EXTENDING THE PIPELINE
================================================================================

ADDING NEW DIMENSIONS:

1. Add extraction function
2. Add transformation logic
3. Add column mapping
4. Choose SCD type
5. Call load function

Example:
def extract_new_dim():
    return pd.read_sql('SELECT * FROM new_table', source_engine)

# In load_dim():
scd_type_1(new_dim_df, 'Dim_New', 'new_pk', new_mapping, dwh_engine)

ADDING NEW FACT TABLES:

1. Create batched extraction similar to extract_fact_batched()
2. Create dimension lookup loading
3. Implement chunk transformation
4. Implement chunk loading with duplicate prevention

================================================================================
END OF DOCUMENTATION
================================================================================
