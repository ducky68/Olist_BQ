# Analytics OBT (One Big Table) Architecture

## Overview
This folder contains **One Big Table (OBT)** models designed for specific business intelligence use cases. Each OBT denormalizes data from the warehouse star schema to create optimized analytical tables that support fast, flexible reporting and analysis.

## Business Intelligence Capabilities

### 🎯 **Core Analytics Models**

#### 1. **Revenue Analytics OBT** (`revenue_analytics_obt.sql`)
- **Purpose**: Comprehensive revenue analysis across all dimensions
- **Grain**: One row per order item (most granular level)
- **Key Features**:
  - Revenue calculations with payment allocation
  - Geographic market segmentation
  - Product performance metrics
  - Customer behavior correlation
  - Time intelligence hierarchies

#### 2. **Customer Analytics OBT** (`customer_analytics_obt.sql`)
- **Purpose**: Customer behavior, satisfaction, and lifecycle analysis
- **Grain**: One row per customer (aggregated customer-level)
- **Key Features**:
  - RFM-style customer segmentation
  - Customer Lifetime Value (CLV) calculations
  - Churn risk assessment
  - Satisfaction correlation analysis
  - Purchase behavior patterns

#### 3. **Seller Analytics OBT** (`seller_analytics_obt.sql`)
- **Purpose**: Seller performance evaluation and marketplace insights
- **Grain**: One row per seller (aggregated seller-level)
- **Key Features**:
  - Performance tier classification
  - Business maturity assessment
  - Market reach analysis
  - Quality metrics tracking
  - Growth trend indicators

#### 4. **Payment Analytics OBT** (`payment_analytics_obt.sql`)
- **Purpose**: Payment behavior and installment analysis
- **Grain**: One row per payment transaction
- **Key Features**:
  - Installment behavior analysis
  - Credit risk assessment
  - Payment method preferences
  - Economic affordability indicators
  - Payment satisfaction correlation

#### 5. **Geographic Analytics OBT** (`geographic_analytics_obt.sql`)
- **Purpose**: Geographic market analysis and regional performance
- **Grain**: One row per state/region
- **Key Features**:
  - Market development classification
  - Regional economic indicators
  - Competition analysis
  - Market opportunity scoring
  - Logistics complexity assessment

#### 6. **Delivery Analytics OBT** (`delivery_analytics_obt.sql`)
- **Purpose**: Delivery performance monitoring and logistics insights
- **Grain**: One row per order item (delivery-focused)
- **Key Features**:
  - Basic delivery performance metrics
  - Order status tracking
  - Geographic shipping context
  - Customer satisfaction correlation
  - Financial metrics integration
- **Note**: Currently implemented as a simplified version focusing on core delivery metrics without complex timestamp calculations

## 🛠 **Technical Architecture**

### **Implementation Status** ✅
- **All 6 OBT Models**: Successfully implemented and tested
- **Data Pipeline**: Fully functional from staging to analytics
- **BigQuery Optimization**: Partitioning and clustering enabled
- **Data Quality**: Comprehensive validation framework
- **Macro Library**: Reusable business logic components

### **Recent Updates & Fixes**
- **Timestamp Handling**: Fixed empty string casting issues in `dim_orders`
- **Data Quality**: Enhanced timestamp field cleaning in staging layer
- **Delivery Model**: Simplified to focus on core metrics due to data quality constraints
- **Performance**: Optimized for BigQuery best practices

### **Jinja Templating & Macros**
All OBTs leverage reusable Jinja macros for:
- **Business Logic**: Consistent classification rules
- **Calculations**: Standardized metric calculations
- **Data Quality**: Unified validation approaches
- **Geographic Classification**: Brazilian market segmentation

### **Key Macros** (`macros/analytics_macros.sql`)
- `calculate_revenue_metrics()`: Revenue calculation logic
- `geographic_metrics()`: Geographic classification and shipping complexity
- `customer_segmentation_logic()`: RFM customer segmentation  
- `clv_calculations()`: Customer Lifetime Value calculation
- `seller_performance_tiers()`: Seller tier classification
- `seller_business_metrics()`: Seller performance calculations
- `payment_analytics_metrics()`: Payment behavior analysis
- `payment_risk_indicators()`: Credit risk assessment
- `regional_economic_metrics()`: Geographic market indicators
- `market_development_tier()`: Market development classification
- `delivery_performance_metrics()`: Basic delivery time calculations
- `delivery_performance_classification()`: Delivery tier classification

### **Data Quality Considerations**
- **Timestamp Fields**: Enhanced cleaning to handle empty strings in source data
- **Safe Casting**: Implemented `safe_cast()` functions for data type conversions
- **Null Handling**: Proper null value management throughout pipeline
- **Validation Tests**: Comprehensive schema and data quality validation

### **Performance Optimizations**
- **Partitioning**: By date for time-based queries
- **Clustering**: By key dimensions for faster filtering
- **Materialization**: All tables materialized for performance
- **Indexing**: Surrogate keys for optimal joins

### **Data Quality Framework**
- **Validation Tests**: Comprehensive schema validation
- **Quality Flags**: Built-in data quality indicators
- **Referential Integrity**: Maintained across all OBTs
- **Audit Fields**: Timestamp tracking for data lineage

## 📊 **Business Use Cases**

### **Revenue Analysis**
- Product performance tracking
- Geographic revenue distribution
- Customer value analysis
- Seasonal trend analysis
- Payment behavior impact on revenue

### **Customer Intelligence**
- Customer segmentation and targeting
- Churn prediction and prevention
- Lifetime value optimization
- Satisfaction improvement
- Personalization strategies

### **Seller Management**
- Performance evaluation and ranking
- Onboarding and training prioritization
- Quality control and monitoring
- Growth opportunity identification
- Market expansion planning

### **Payment Optimization**
- Installment strategy optimization
- Risk management
- Payment method preferences
- Economic accessibility analysis
- Fraud detection support

### **Market Analysis**
- Geographic expansion planning
- Competitive landscape analysis
- Market penetration strategies
- Regional performance comparison
- Investment prioritization

### **Operations Excellence**
- Basic delivery performance monitoring
- Order status tracking and analysis
- Customer satisfaction correlation
- Geographic shipping analysis
- Financial impact assessment

## 🛠 **Technical Implementation Notes**

### **Known Limitations & Considerations**
- **Delivery Analytics**: Currently simplified due to data quality constraints in timestamp fields
- **Timestamp Processing**: Some advanced time-based calculations limited by source data quality
- **Performance**: Optimized for BigQuery with appropriate partitioning and clustering
- **Data Quality**: Enhanced cleaning processes implemented for timestamp fields

### **Future Enhancements**
- **Advanced Delivery Metrics**: Can be enhanced once timestamp data quality is improved
- **Seasonal Analysis**: Additional temporal patterns analysis
- **Predictive Models**: Machine learning integration opportunities
- **Real-time Updates**: Streaming analytics capabilities

## 🎯 **Analytics Patterns**

### **Dimensional Analysis**
- **Time Intelligence**: Year, quarter, month, week hierarchies
- **Geographic Drilling**: Country → Region → State → City
- **Product Hierarchy**: Category → Subcategory → Product
- **Customer Segmentation**: Tier → Segment → Individual

### **Calculated Measures**
- **Growth Rates**: Period-over-period comparisons
- **Market Share**: Relative performance metrics
- **Efficiency Ratios**: Cost and performance indicators
- **Satisfaction Scores**: Quality and experience metrics

### **Advanced Analytics**
- **Customer Lifetime Value**: Predictive revenue modeling
- **Churn Risk Scoring**: Behavioral pattern analysis
- **Market Opportunity Index**: Multi-factor scoring
- **Performance Benchmarking**: Comparative analysis

## 🔄 **Data Pipeline**

### **Source → Staging → Warehouse → Analytics**
1. **Raw Data**: Source system extracts
2. **Staging Layer**: Data cleaning and standardization
3. **Warehouse Layer**: Dimensional modeling (star schema)
4. **Analytics Layer**: Denormalized OBTs for specific use cases

### **Update Frequency**
- **Daily Refresh**: All OBTs updated nightly
- **Incremental Updates**: Where possible for performance
- **Full Refresh**: Weekly for data quality assurance

### **Dependencies**
- All analytics OBTs depend on warehouse layer
- Cross-OBT relationships maintained through surrogate keys
- Audit trails preserved throughout pipeline

## 📈 **Performance & Scalability**

### **Query Performance**
- **Sub-second Response**: For standard dashboard queries
- **Optimized Aggregations**: Pre-calculated business metrics
- **Efficient Joins**: Minimized through denormalization
- **Smart Partitioning**: Time-based data access patterns

### **Storage Optimization**
- **Columnar Storage**: BigQuery native optimization
- **Compression**: Automatic data compression
- **Partitioning**: Reduced scan costs
- **Clustering**: Improved query performance

### **Maintenance**
- **Automated Testing**: Data quality validation
- **Performance Monitoring**: Query performance tracking
- **Cost Optimization**: Storage and compute efficiency
- **Documentation**: Comprehensive metadata management

## 🎨 **Visualization Ready**

These OBTs are optimized for:
- **Tableau/Power BI**: Direct connection capability
- **Looker/Mode**: Semantic layer compatibility
- **Custom Dashboards**: API-friendly structure
- **Excel/Sheets**: Export-friendly formats
- **Jupyter/R**: Data science workflows

## 🔐 **Security & Governance**

### **Data Access Control**
- **Role-Based Access**: Controlled by IAM policies
- **Row-Level Security**: Customer/seller data isolation
- **Column-Level Security**: PII protection
- **Audit Logging**: Access pattern monitoring

### **Data Lineage**
- **Source Tracking**: Full data lineage documentation
- **Transformation Logic**: Documented business rules
- **Impact Analysis**: Change impact assessment
- **Metadata Management**: Comprehensive data catalog

---

## 🚀 **Getting Started**

1. **Enable Analytics Layer**: Set `+enabled: true` in `dbt_project.yml` for analytics_obt
2. **Run Dependencies**: Execute `dbt run --models +analytics_obt` to build all dependencies
3. **Run Models**: Execute `dbt run --models analytics_obt` for just analytics tables
4. **Validate Data**: Run `dbt test --models analytics_obt` to check data quality
5. **Connect BI Tools**: Use BigQuery connector to analytics tables in `dbt_olist_analytics` dataset
6. **Build Dashboards**: Leverage pre-calculated metrics for fast visualizations

### **Model Statistics** (as of latest run)
- **revenue_analytics_obt**: ✅ Working (112.6k rows)
- **customer_analytics_obt**: ✅ Working 
- **seller_analytics_obt**: ✅ Working (3.1k rows)
- **payment_analytics_obt**: ✅ Working (112.6k rows)
- **geographic_analytics_obt**: ✅ Working (27 rows)
- **delivery_analytics_obt**: ✅ Working (112.6k rows)

### **Troubleshooting**
- **Timestamp Issues**: If encountering timestamp casting errors, ensure `dim_orders` table uses cleaned timestamp fields
- **Performance**: Use appropriate date filters to optimize query performance
- **Dependencies**: Ensure staging and warehouse layers are built before analytics layer

The analytics OBT layer provides a powerful foundation for business intelligence, enabling fast, flexible, and comprehensive analysis across all aspects of the Olist marketplace business.
