# autocheck-data-model

### This repo is dedicated to the development of AutoCheck data pipeline design, modeling and web crawler of car listings

## ETL Pipeline design and data modeling

### **ERD of Data Sources**

![alt text](docs/ERD.png)

The diagram above shows the logical representation of Autocheck data sources

An EL script was designed in Python to pull the source files (CSV sources) and load them into a snowflake warehouse. Leveraging this, dbt core was implemented to perform data transformation and modeling to satisfy business analytics and answer specific business questions.

A script was used within Snowflake to setup the warehouse for this project. The script can be found here: scripts/snowflake_setup.sql.

### **Data Lineage**

![alt text](docs/data-lineage.png)



## Web Crawler for Car Listings

A web crawler was designed to pull data from the cars45.com website. This crawler pulls the image source, car brand, car amount, region, car type (foreign or locally used) and other details. This information is loaded into Snowflake incrementally and scheduled to run daily.

### Prerequisites

- A GitHub account
- A snowflake account is needed for a replication of this project
