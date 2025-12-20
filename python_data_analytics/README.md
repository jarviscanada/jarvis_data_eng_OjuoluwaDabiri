# Introduction
London Gift Shop (LGS) is a UK-based online retailer that has operated successfully for over a decade, primarily serving wholesalers and bulk buyers. Despite a stable customer base, LGS has experienced stagnating revenue growth in recent years. To address this challenge, the business seeks to better understand customer purchasing behavior and use data-driven insights to design more effective sales and marketing strategies.

This project delivers a proof of concept (PoC) analytics solution that transforms historical transaction data into actionable customer insights. The core objective is to analyze customer behavior, identify meaningful customer segments, and provide analytical outputs that the LGS marketing team can directly use to design targeted campaigns, such as loyalty programs, win-back offers, and personalized promotions.

The analytics produced in this project enable LGS to:

- Identify high-value and loyal customers
- Detect customers at risk of churn
- Understand purchasing frequency and recency patterns
- Allocate marketing resources more effectively by segment

### Technologies Used

- Python for data processing and analysis
- Jupyter Notebook for exploratory analytics and visualization
- Pandas & NumPy for data wrangling and feature engineering
- PostgreSQL (Dockerized) as a lightweight data warehouse for OLAP exploration
- Matplotlib & Seaborn for visual analytics and plotting


# Implementaion
## Project Architecture
- This proof-of-concept project was implemented using a lightweight analytics architecture designed to analyze historical transactional data without direct access to London Gift Shops production cloud environment. The solution focuses on reproducibility, clarity, and the ability to deliver actionable insights to non-technical stakeholders such as the marketing team.


- LGS Web Application (Production): At the data source level, London Gift Shops online retail platform generates transactional records containing invoice details, products, quantities, pricing, and customer identifiers. Since the Jarvis team was not permitted to access the production Azure environment, the LGS IT team exported anonymized transactional data covering the period from December 2009 to December 2011 into a SQL file. As part of the extraction process, all personally identifiable information was removed to ensure compliance with data privacy requirements. 


- To support analytical workloads, the exported dataset was loaded into a PostgreSQL database provisioned using Docker. Although PostgreSQL is commonly used as a transactional database, in this project it served as a simple analytical data warehouse to support OLAP-style queries such as aggregations, grouping, and time-based analysis. SQL was used at this stage to validate the schema, inspect data ranges, compute basic metrics (e.g., record counts, revenue totals), and gain an initial understanding of customer and product behavior. 


- Following SQL-based exploration, the data was ingested into a Jupyter Notebook environment using Python. Pandas and NumPy were used extensively for data wrangling, transformation, and feature engineering. This stage involved cleaning invalid records, handling missing values, converting timestamps, and generating derived fields such as invoice-level revenue and monthly aggregation keys. These transformations ensured that the dataset was suitable for higher-level customer analytics. 


- Analytics & Segmentation Layer: The core analytical component of the project focused on customer segmentation using RFM (Recency, Frequency, Monetary) analysis. Transaction-level data was aggregated at the customer level to calculate how recently each customer made a purchase, how frequently they placed orders, and how much they spent in total. These metrics were then normalized using quintile-based scoring, allowing customers to be compared relative to one another. The resulting scores were combined and mapped to meaningful customer segments such as Champions, Loyal Customers, At Risk, and Hibernating customers.

The final output of the implementation is a structured customer segmentation dataset and supporting visualizations that clearly describe customer behavior patterns. These results can be directly consumed by the LGS marketing team to design targeted campaigns, prioritize customer retention efforts, and improve marketing efficiency. The complete analytical workflow, including data preparation, analysis, and interpretation, is documented and reproducible through the provided Jupyter Notebook.
- Draw an architecture Diagram (please do not copy-paste any diagram from the project board)

## Data Analytics and Wrangling
- Notebook link: [retail_data_analytics_wrangling](retail_data_analytics_wrangling.ipynb).

The analytics process consists of several key stages:

1. Data Profiling & Exploration

- Verified schema and data types 
- Measured dataset size, date ranges, and uniqueness 
- Explored revenue trends and invoice distributions 
- Identified cancellations and outliers

2. Data Cleaning

- Removed rows with missing customer identifiers 
- Filtered invalid or zero-value transactions for customer segmentation 
- Standardized datetime formats and numerical columns

3. Feature Engineering

Created derived fields such as:

- Invoice revenue 
- Monthly aggregation keys (YYYYMM)
- Aggregated transactional data to the customer level

4. RFM Analysis

RFM analysis was used to quantify customer behavior across three dimensions:

- Recency: Days since last purchase 
- Frequency: Number of unique invoices 
- Monetary: Total spend

Customers were scored using quintile ranking and combined into RFM scores. These scores were then mapped into business-friendly segments, including:

- Champions 
- Loyal Customers 
- Potential Loyalists 
- New Customers 
- Promising 
- Need Attention 
- About to Sleep 
- At Risk 
- Can\'t Lose 
- Hibernating 
- Lost / Inactive

5. Business Impact

The resulting customer segments allow LGS to:

- Reward Champions with loyalty perks and exclusive access 
- Re-engage At Risk and Cant Lose customers through targeted win-back campaigns 
- Use cost-effective promotions for Hibernating customers 
- Focus acquisition efforts on profiles similar to Loyal Customers

This segmentation framework enables data-driven marketing, replacing one-size-fits-all campaigns with targeted strategies that improve ROI.
# Improvements
If given more time, the following enhancements could significantly increase the value of this project:

- Automating and optimizing the data pipeline is one of the first improvements I would make. We can replace manual SQL dumps with scheduled ingestion using tools like Airflow or dbt and this would enable near-real-time customer analytics. 

- Improving customer modeling by incorporating churn prediction or lifetime value (LTV) modeling.

- I could also build a BI dashboard (Tableau, Power BI, or Streamlit) for marketing stakeholders. This would enable self-service exploration of customer segments and KPIs.