# Final Project - Civica Software Course
### Overview
This repository contains the final project I completed during the Data Engineering course at Civica Software. The project was designed to apply the ETL (Extract, Transform, Load) process and practice data modeling techniques using modern tools and technologies.

The pipeline built in this project simulates a real-world workflow, covering all stages from raw data ingestion to a structured and analytical-ready model, culminating in actionable insights through visualization.

![Data Pipeline Graph](https://github.com/miguermv/curso_data_engineering/blob/main/assets/data_pipeline.jpg?raw=true "Data Pipeline Graph")

### Tools Used
- **Fivetran** for data ingestion.
- **DBT** for data transformation.
- **Snowflake** as the data warehouse.
- **Power BI** for data visualization.
<br>

## DBT Lineage Diagram
![DBT Lineage](https://github.com/miguermv/curso_data_engineering/blob/main/assets/Linaje.png?raw=true "DBT Lineage")

The image above illustrates the data lineage within the DBT project, showing the transformation process and the relationships between different layers:

- **Source Tables**: Represent the raw data ingested into the pipeline, stored in the Bronze layer.
- **Snapshots**: Point-in-time captures of critical tables, ensuring data consistency for historical analysis.
- **Base Models**: Serve as intermediary layers, standardizing and preparing raw data for staging.
- **Staging Models**: Clean and enhance data with transformations and additional logic, preparing it for downstream use.
- **Core Models**:
	-  Dimensions: Contain optimized, structured data describing key business entities (e.g., customers, rooms, hotels).
	-  Facts: Aggregate transactional data, such as booking summaries or financial performance, for analytical purposes.
- **Data Marts**: Represent the final outputs derived from the Core models, tailored to specific business use cases. Examples include:
	- Customer segmentation analysis
	- Seasonal revenue trends
	- Financial and operational performance

This diagram provides a clear visual of how raw data is transformed step by step into clean, structured data, ready to be used for analytics.

<br>

## Dashboard: Hotel Booking Insights
![Power BI Dashboard](https://github.com/miguermv/curso_data_engineering/blob/main/assets/PowerBI_Dashboard.jpg?raw=true "Power BI Dashboard")

The entire data modeling process ultimately serves one purpose: to transform raw data into actionable insights. Using Power BI, I developed an interactive dashboard that provides the client with access to high-performance, high-quality data visualizations.

Thanks to the structure and optimization of the Gold layer, the dashboard ensures:
- Quick and responsive performance.
- Reliable and accurate business metrics.

This dashboard helps the client to explore their data, make informed decisions efficiently, and ultimately improve their business performance by leveraging the full potential of their data.
