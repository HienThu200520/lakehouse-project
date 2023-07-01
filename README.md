# STEDI Lakehouse Solution

This repository contains the code and scripts to build a lakehouse solution in AWS using Glue, S3, Python, and Spark for the STEDI data scientists.

## Project Structure

The project is organized into the following directories:

- `scripts/`: Contains the Python scripts for Glue Jobs.
- `sql/`: Contains the SQL scripts for creating Glue tables.
- `data/`: Placeholder directory for simulated data sources.

## Getting Started

To set up the project, follow these steps:

1. Clone the repository:

git clone https://github.com/HienThu200520/lakehouse-project.git


2. Create S3 directories for landing zones:

   - `customer_landing/`: The landing zone for customer data.
   - `step_trainer_landing/`: The landing zone for step trainer data.
   - `accelerometer_landing/`: The landing zone for accelerometer data.

3. Copy the simulated data to the respective landing zones.

4. Create Glue tables:

   - Run the SQL script `sql/customer_landing.sql` to create the `customer_landing` table in the landing zone.
   - Run the SQL script `sql/accelerometer_landing.sql` to create the `accelerometer_landing` table in the landing zone.

5. Query the landing zone tables using Athena:

   - Run the query `SELECT * FROM customer_landing` and take a screenshot of the result. Save it as `customer_landing.png`.
   - Run the query `SELECT * FROM accelerometer_landing` and take a screenshot of the result. Save it as `accelerometer_landing.png`.

## Glue Jobs

The project includes the following Glue Jobs:

1. `scripts/customer_landing_to_trusted.py`: Sanitizes the customer data from the landing zone and creates the `customer_trusted` Glue Table in the trusted zone.

2. `scripts/accelerometer_landing_to_trusted_zone.py`: Sanitizes the accelerometer data from the landing zone and creates the `accelerometer_trusted` Glue Table in the trusted zone.

3. `scripts/customer_trusted_to_curated.py`: Sanitizes the customer data in the trusted zone and creates the `customers_curated` Glue Table in the curated zone.

4. `scripts/glue_studio_job1.py`: Reads the Step Trainer IoT data stream from S3, filters the data based on consent, and populates the `step_trainer_trusted` Glue Table in the trusted zone.

5. `scripts/glue_studio_job2.py`: Reads the customer_trusted and accelerometer_trusted tables, performs joins and filters, and creates the `machine_learning_curated` Glue Table in the curated zone.

## Usage

To run the Glue Jobs, follow these steps:

1. Open the AWS Glue console.

2. Create a new Glue Job.

3. Copy the code from the respective script file and paste it into the Glue Job editor.

4. Configure the job settings and parameters as required.

5. Run the Glue Job.

## Results

After running the Glue Jobs, you can verify the results by:

- Querying the `customer_trusted` table using Athena and take a screenshot of the result. Save it as `customer_trusted.png`.

