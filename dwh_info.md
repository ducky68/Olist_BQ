# Warehouse Layer Implementation Guide

## 📋 Overview

The **Warehouse Layer** is the second layer in our 3-tier dbt architecture. This layer transforms staging data into clean, business-ready dimensional models following **star schema principles**.

```
Staging Layer → Warehouse Layer → Analytics OBT
     ↓               ↓                ↓
 Quality Flags → Cleaned Data → Business Metrics
```

## 🎯 Warehouse Layer Objectives

### **Primary Goals:**
1. **Apply Data Cleaning Logic** based on staging quality flags
2. **Create Dimensional Models** (Fact & Dimension tables)
3. **Implement Business Rules** and standardizations
4. **Join Related Data** across staging tables
5. **Prepare Analytics-Ready Tables** for the final layer

### **Key Principles:**
- ✅ **Clean data** using quality flags from staging
- ✅ **Create reusable dimensions** following Kimball methodology
- ✅ **Build fact tables** with proper grain definition
- ✅ **Apply business logic** and transformations
- ✅ **Maintain referential integrity** between facts and dimensions

## 🏗️ Data Cleaning Strategy

### **Quality Flag-Based Cleaning**

Based on our staging layer's 185+ quality flags, we'll implement systematic cleaning:

#### **1. Null Value Handling**
```sql
-- Example: Customer state cleaning
case 
    when customer_state_is_null then 'Unknown'
    when customer_state_invalid_value then 'Unknown'
    else customer_state
end as customer_state_clean
```

#### **2. Invalid Data Correction**
```sql
-- Example: Price correction
case 
    when price_is_negative then null
    when price_is_zero and freight_value > 0 then freight_value * 0.1  -- Business rule
    when price_suspiciously_high then null  -- Flag for manual review
    else price
end as price_clean
```

#### **3. Date/Time Standardization**
```sql
-- Example: Order date cleaning
case 
    when order_purchase_timestamp_is_future then null
    when invalid_purchase_timestamp then null
    else order_purchase_timestamp_clean
end as order_purchase_timestamp_final
```

#### **4. Business Rule Application**
```sql
-- Example: Delivery status logic
case 
    when delivered_after_estimated then 'Late Delivery'
    when order_status = 'delivered' and order_delivered_customer_date is not null then 'On Time'
    when approval_before_purchase then 'Data Error'
    else 'Processing'
end as delivery_performance
```

## 📊 Dimensional Model Design

### **Star Schema Architecture**

Our warehouse will implement a **star schema** with clearly defined facts and dimensions:

```
    dim_customers
         |
    dim_products ← fact_orders → dim_sellers
         |              |
    dim_categories   dim_time
         |              |
    dim_geography   fact_order_items
```

## 🗂️ Dimension Tables (SCD Type 1)

### **1. `dim_customers`**
**Purpose:** Customer master data with demographics and location
**Grain:** One row per unique customer

**Key Transformations:**
- Clean customer states using quality flags
- Standardize city names and zip codes
- Create customer segments based on order history
- Add geographic region mappings

**Source Tables:** `stg_customers`, `stg_geolocation`

### **2. `dim_products`**
**Purpose:** Product catalog with categories and attributes
**Grain:** One row per unique product

**Key Transformations:**
- Clean product dimensions (weight, length, height)
- Standardize category names using translation table
- Calculate product complexity scores
- Add product lifecycle flags

**Source Tables:** `stg_products`, `stg_product_category_name_translation`

### **3. `dim_sellers`**
**Purpose:** Seller information with location and performance metrics
**Grain:** One row per unique seller

**Key Transformations:**
- Clean seller locations using geolocation data
- Add seller performance categories
- Calculate seller tenure and activity flags

**Source Tables:** `stg_sellers`, `stg_geolocation`

### **4. `dim_geography`**
**Purpose:** Geographic hierarchy and location intelligence
**Grain:** One row per unique zip code + city combination

**Key Transformations:**
- Clean coordinate data using validation flags
- Create geographic hierarchies (City → State → Region)
- Add location-based business insights

**Source Tables:** `stg_geolocation`

### **5. `dim_time`**
**Purpose:** Time dimension for temporal analysis
**Grain:** One row per day

**Key Transformations:**
- Generate complete date range (2016-2018 + future dates)
- Add business calendar attributes (quarters, holidays)
- Include Brazilian holiday calendar

### **6. `dim_categories`**
**Purpose:** Product category hierarchy and translations
**Grain:** One row per category (Portuguese + English)

**Key Transformations:**
- Link Portuguese and English category names
- Create category hierarchies (if applicable)
- Add category performance metrics

**Source Tables:** `stg_product_category_name_translation`

## 📈 Fact Tables

### **1. `fact_orders`**
**Purpose:** Order header information and metrics
**Grain:** One row per order

**Key Metrics:**
- Order value calculations
- Delivery performance metrics
- Order status tracking
- Customer acquisition flags

**Key Transformations:**
- Clean order timestamps using quality flags
- Calculate order lifecycle durations
- Apply business rules for order status
- Add derived metrics (order_value_total, days_to_delivery)

**Source Tables:** `stg_orders`

### **2. `fact_order_items`**
**Purpose:** Individual order line items and product performance
**Grain:** One row per order item

**Key Metrics:**
- Item-level revenue and costs
- Product performance metrics
- Seller performance tracking
- Shipping and freight analysis

**Key Transformations:**
- Clean price and freight values using quality flags
- Calculate margins and profitability
- Add product and seller performance indicators
- Handle negative/zero prices with business rules

**Source Tables:** `stg_order_items`, `stg_order_payments`

### **3. `fact_reviews`**
**Purpose:** Customer review and satisfaction metrics
**Grain:** One row per review

**Key Metrics:**
- Review scores and sentiment
- Review timing analysis
- Customer satisfaction trends

**Key Transformations:**
- Clean review timestamps
- Standardize review scores
- Calculate review response times
- Add sentiment analysis flags

**Source Tables:** `stg_order_reviews`

## 🛠️ Implementation Plan

### **Phase 1: Dimension Tables (Week 1)**
1. `dim_time` - Base calendar dimension
2. `dim_geography` - Clean location data
3. `dim_categories` - Product categories
4. `dim_customers` - Customer master
5. `dim_products` - Product catalog
6. `dim_sellers` - Seller information

### **Phase 2: Fact Tables (Week 2)**
1. `fact_orders` - Order headers
2. `fact_order_items` - Order line items
3. `fact_reviews` - Review data

### **Phase 3: Data Quality & Testing (Week 3)**
1. Referential integrity tests
2. Data quality monitoring
3. Business rule validation
4. Performance optimization

## 📋 File Structure

```
models/warehouse/
├── dimensions/
│   ├── dim_customers.sql
│   ├── dim_products.sql
│   ├── dim_sellers.sql
│   ├── dim_geography.sql
│   ├── dim_time.sql
│   └── dim_categories.sql
├── facts/
│   ├── fact_orders.sql
│   ├── fact_order_items.sql
│   └── fact_reviews.sql
├── schema.yml (documentation & tests)
└── README.md
```

## 🧪 Data Quality & Testing

### **dbt Tests to Implement:**
```yaml
# Example test configuration
- name: dim_customers
  tests:
    - unique:
        column_name: customer_key
    - not_null:
        column_name: customer_key
    - accepted_values:
        column_name: customer_state_clean
        values: ['SP', 'RJ', 'MG', 'Unknown']

- name: fact_orders
  tests:
    - relationships:
        to: ref('dim_customers')
        field: customer_key
    - not_null:
        column_name: order_key
```

### **Business Rule Validation:**
- Revenue calculations accuracy
- Date logic consistency
- Geographic data integrity
- Customer segmentation logic

## 🎯 Success Criteria

### **Data Quality Metrics:**
- **< 1%** null values in critical business fields
- **100%** referential integrity between facts and dimensions
- **< 0.1%** data quality rule violations
- **Zero** duplicate records in dimension tables

### **Business Readiness:**
- All major business questions answerable
- Clean, standardized data ready for analytics
- Proper grain definition for all fact tables
- Complete audit trail from source to warehouse

## 🚀 Next Steps

After warehouse layer completion:
1. **Validate Data Quality** - Run comprehensive tests
2. **Business User Review** - Verify business logic accuracy
3. **Performance Optimization** - Optimize query performance
4. **Move to Analytics OBT** - Build final business-ready tables

---

*The warehouse layer serves as the foundation for all business intelligence and analytics, providing clean, reliable, and business-ready data for decision making.*
