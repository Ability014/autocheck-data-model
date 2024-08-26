# autocheck-data-model

### This repo is dedicated to the development of AutoCheck data pipeline design, modeling and web crawler of car listings

## ETL Pipeline design and data modeling

### **ERD of Data Sources**

![alt text](docs/ERD.png)

The diagram above shows the logical representation of Autocheck data sources

An EL scripts was designed in python to pull the source files (csv sources) and load into a snowflake warehouse. Leveraging on this, dbt core was implemented to perform data transformation and modeling in order to statisfy business analytics and answer specific business questions.


### **Data Lineage**

![alt text](docs/data-lineage.png)



## Web Crawler for Car Listings

A web crawler was degined to pull data from cars45.com website. This crawler pulls the image source, car brand, car amount, region, car type (foreign or local used) and other details. These information is loaded into snowflake incrementally and schduled to run daily.

### Prerequisites

- A github account
- A snowflake account is needed for a replication of this project