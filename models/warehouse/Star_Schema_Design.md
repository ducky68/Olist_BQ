# **Olist Data Warehouse Star Schema Documentation**

## **Overview**
This document describes the dimensional model for the Olist Brazilian e-commerce data warehouse, implementing a star schema design optimized for BigQuery analytics.

***

## **Fact Table**

### **`fact_order_items`**
**Grain**: One row per order item (most granular level)  
**Business Purpose**: Central fact table containing all measurable metrics for order item analysis

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `order_item_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `order_id` | STRING | NO | Natural key for order identification |
| `order_item_id` | INTEGER | NO | Natural key for item within order |
| `order_sk` | BIGINT | NO | Foreign key to `dim_orders` |
| `customer_sk` | BIGINT | NO | Foreign key to `dim_customer` |
| `product_sk` | BIGINT | NO | Foreign key to `dim_product` |
| `seller_sk` | BIGINT | NO | Foreign key to `dim_seller` |
| `payment_sk` | BIGINT | NO | Foreign key to `dim_payment` |
| `review_sk` | BIGINT | NULLABLE | Foreign key to `dim_order_reviews` |
| `order_date_sk` | INTEGER | NO | Foreign key to `dim_date` |
| `shipping_limit_date_sk` | INTEGER | NULLABLE | Foreign key to `dim_date` |
| `customer_geography_sk` | BIGINT | NO | Foreign key to `dim_geolocation` |
| `seller_geography_sk` | BIGINT | NO | Foreign key to `dim_geolocation` |
| `price` | FLOAT | NO | Item price (additive measure) |
| `freight_value` | FLOAT | NO | Shipping cost (additive measure) |
| `payment_value` | FLOAT | NO | Allocated payment amount (additive measure) |
| `payment_installments` | INTEGER | NULLABLE | Number of payment installments |
| `review_score` | INTEGER | NULLABLE | Customer review score (1-5) |

**Performance Optimization**:
```sql
PARTITION BY order_date_sk 
CLUSTER BY customer_sk, seller_sk, product_sk
```

***

## **Dimension Tables**

### **`dim_date`**
**Purpose**: Time dimension for temporal analysis

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `date_sk` | INT | NO | Surrogate key (Primary Key) - YYYYMMDD format |
| `date_value` | DATE | NO | Actual date value |
| `year` | BIGINT | YES | Year component |
| `quarter` | BIGINT | YES | Quarter component (1-4) |
| `month` | BIGINT | YES | Month component (1-12) |
| `day_of_month` | BIGINT | YES | Day of month (1-31) |
| `day_of_week` | BIGINT | YES | Day of week (1-7) |
| `is_weekend` | BOOL | YES | Weekend indicator |

**Clustering**: `CLUSTER BY year, month`

***

### **`dim_customer`**
**Purpose**: Customer descriptive attributes

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `customer_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `customer_id` | STRING | NO | Natural key from source system |
| `customer_unique_id` | STRING | YES | Unique customer identifier |
| `customer_zip_code_prefix` | STRING | YES | Customer postal code prefix |
| `customer_city` | STRING | YES | Customer city |
| `customer_state` | STRING | YES | Customer state |

**Clustering**: `CLUSTER BY customer_state`

***

### **`dim_geolocation`**
**Purpose**: Geographic information for customers and sellers

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `geolocation_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `geolocation_zip_code_prefix` | STRING | NO | Postal code prefix |
| `geolocation_lat` | FLOAT | NULLABLE | Latitude coordinate |
| `geolocation_lng` | FLOAT | NULLABLE | Longitude coordinate |
| `geolocation_city` | STRING | NULLABLE | City name |
| `geolocation_state` | STRING | NULLABLE | State name |

**Clustering**: `CLUSTER BY geolocation_state`

***

### **`dim_product`**
**Purpose**: Product catalog and attributes

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `product_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `product_id` | STRING | NO | Natural key from source system |
| `product_category_name` | STRING | NULLABLE | Product category (Portuguese) |
| `product_name_length` | INTEGER | NULLABLE | Length of product name |
| `product_description_length` | INTEGER | NULLABLE | Length of product description |
| `product_photos_qty` | INTEGER | NULLABLE | Number of product photos |
| `product_weight_g` | INTEGER | NULLABLE | Product weight in grams |
| `product_length_cm` | INTEGER | NULLABLE | Product length in centimeters |
| `product_height_cm` | INTEGER | NULLABLE | Product height in centimeters |
| `product_width_cm` | INTEGER | NULLABLE | Product width in centimeters |
| `product_category_name_english` | STRING | NULLABLE | Product category (English) |

**Clustering**: `CLUSTER BY product_category_name_english`

***

### **`dim_seller`**
**Purpose**: Seller/merchant information

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `seller_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `seller_id` | STRING | NO | Natural key from source system |
| `seller_zip_code_prefix` | STRING | NULLABLE | Seller postal code prefix |
| `seller_city` | STRING | NULLABLE | Seller city |
| `seller_state` | STRING | NULLABLE | Seller state |

**Clustering**: `CLUSTER BY seller_state`

***

### **`dim_payment`**
**Purpose**: Payment method information

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `payment_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `order_id` | STRING | NO | Order identifier |
| `payment_sequential` | INTEGER | NULLABLE | Payment sequence number |
| `payment_type` | STRING | NULLABLE | Payment method type |

**Clustering**: `CLUSTER BY payment_type`

***

### **`dim_order_reviews`**
**Purpose**: Customer review information

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `review_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `review_id` | STRING | NULLABLE | Natural key from source system |
| `order_id` | STRING | NULLABLE | Associated order identifier |
| `review_comment_title` | STRING | NULLABLE | Review title |
| `review_comment_message` | STRING | NULLABLE | Review message content |
| `review_creation_date` | TIMESTAMP | NULLABLE | Review creation timestamp |
| `review_answer_timestamp` | TIMESTAMP | NULLABLE | Seller response timestamp |

**Clustering**: `CLUSTER BY order_id, review_creation_date`

***

### **`dim_orders`**
**Purpose**: Order lifecycle and status information

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `order_sk` | BIGINT | NO | Surrogate key (Primary Key) |
| `order_id` | STRING | NO | Natural key from source system |
| `order_status` | STRING | NULLABLE | Current order status |
| `order_purchase_timestamp` | TIMESTAMP | NULLABLE | Order purchase time |
| `order_approved_at` | TIMESTAMP | NULLABLE | Order approval time |
| `order_delivered_carrier_date` | TIMESTAMP | NULLABLE | Carrier delivery date |
| `order_delivered_customer_date` | TIMESTAMP | NULLABLE | Customer delivery date |
| `order_estimated_delivery_date` | TIMESTAMP | NULLABLE | Estimated delivery date |

**Clustering**: `CLUSTER BY order_status`

***

## **Schema Design Principles**

### **Star Schema Benefits**
- **Performance**: Direct joins from fact to dimensions
- **Simplicity**: Easy to understand and query
- **Scalability**: Optimized for analytical workloads

### **Key Features**
- **Surrogate Keys**: All dimensions use BIGINT surrogate keys for performance
- **Natural Key Preservation**: Original IDs maintained for data lineage
- **Geography Integration**: Separate geography dimension for flexible location analysis
- **Optimal Partitioning**: Fact table partitioned by date for time-based queries
- **Smart Clustering**: Each table clustered on frequently-filtered columns

### **Business Intelligence Capabilities**
- Revenue analysis by product, customer, seller, geography
- Payment behavior and installment analysis
- Customer satisfaction tracking through review scores
- Delivery performance monitoring
- Geographic market analysis
- Seller performance evaluation