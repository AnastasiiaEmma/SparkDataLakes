
# STEDI Human Balance Analytics

STEDI Human Balance Analytics is a data analytics tool that provides insights into human balance data. It leverages AWS Glue, AWS S3, Python, and Spark to build a lakehouse solution in AWS that satisfies the requirements from the STEDI data scientists.

## Project Instructions

### Requirements

To simulate the data coming from various sources, create the following S3 directories for the landing zones:

-   `customer_landing`: For customer data
-   `step_trainer_landing`: For step trainer data
-   `accelerometer_landing`: For accelerometer data

Copy the relevant data into each landing zone directory as a starting point.

### Data Ingestion

To analyze the data in a semi-structured format, create two Glue tables for the landing zones:

-   `customer_landing`: Represents the customer landing zone
-   `accelerometer_landing`: Represents the accelerometer landing zone

The SQL scripts `customer_landing.sql` and `accelerometer_landing.sql` are in the landing folder.

The created tables are queried using Athena. The screenshots are saved as `customer_landing.png` and `accelerometer_landing.png`.

### Data Sanitization

Create two AWS Glue Jobs to sanitize the data and move it to the trusted zone:

1.  Sanitize the customer data from the website landing zone and store only the customer records who agreed to share their data for research purposes. Create a Glue Table called `customer_trusted`.
2.  Sanitize the accelerometer data from the mobile app landing zone and store only the accelerometer readings from customers who agreed to share their data for research purposes. Create a Glue Table called `accelerometer_trusted`.

The SQL scripts `customer_trusted.sql` and `accelerometer_trusted.sql` are in the trusted folder. The created tables are queried using Athena.
The screenshots are saved as `customer_trusted.png` and `accelerometer_trusted.png`.

### Data Curated Zone

Write a Glue Job to sanitize the customer data in the trusted zone and create a curated Glue Table called `customers_curated`. This table should include only customers who have accelerometer data and have agreed to share their data for research.

### Glue Studio Jobs

Create two Glue Studio jobs to perform the following tasks:

1.  Read the Step Trainer IoT data stream from S3 and populate a Glue Table called `step_trainer_trusted`. This table should contain the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (from `customers_curated`).
2.  Create an aggregated table that includes each Step Trainer Reading and the associated accelerometer reading data for the same timestamp. This table should be named `machine_learning_curated`.