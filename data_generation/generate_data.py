import pandas as pd
from faker import Faker
import numpy as np 
import random
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.ERROR)

try:
    spark = SparkSession.builder.appName('SimulateData').getOrCreate()
    faker = Faker()

    num_records = 1000

    # Sales Transaction Data
    try:
        sales_data = pd.DataFrame({
            'Transaction_ID': range(1, num_records + 1),
            'Customer_ID': [faker.random_number(digits=5) for _ in range(num_records)],
            'Product_ID': [faker.random_number(digits=5) for _ in range(num_records)],
            'Quantity': [random.randint(1, 10) for _ in range(num_records)],
            'Price': [round(random.uniform(10, 1000), 2) for _ in range(num_records)],
            'Date': [faker.date_between(start_date='-2y', end_date='today') for _ in range(num_records)],
            'Time': [faker.time() for _ in range(num_records)]
        })
    except Exception as e:
        logging.error(f"Error generating sales data: {e}")
    
    # Sales Transaction Data
    try:
        sales_data = pd.DataFrame({
            'Transaction_ID': range(1, num_records + 1),
            'Customer_ID': [faker.random_number(digits=5) for _ in range(num_records)],
            'Product_ID': [faker.random_number(digits=5) for _ in range(num_records)],
            'Quantity': [random.randint(1, 10) for _ in range(num_records)],
            'Price': [round(random.uniform(10, 1000), 2) for _ in range(num_records)],
            'Date': [faker.date_between(start_date='-2y', end_date='today') for _ in range(num_records)],
            'Time': [faker.time() for _ in range(num_records)]
        })
    except Exception as e:
        logging.error(f"Error generating inventory data: {e}")

    # Business Expense Records
    try:
        expense_data = pd.DataFrame({
            'Expense_ID': range(1, num_records + 1),
            'Category': [faker.word().title() for _ in range(num_records)],
            'Amount': [round(random.uniform(100, 5000), 2) for _ in range(num_records)],
            'Date': [faker.date_between(start_date='-2y', end_date='today') for _ in range(num_records)],
            'Description': [faker.sentence() for _ in range(num_records)]
    })
    except Exception as e:
        logging.error(f"Error generating expense data: {e}")
         

    try:
        sales_df = spark.createDataFrame(sales_data)
        inventory_df = spark.createDataFrame(inventory_data)
        expense_df = spark.createDataFrame(expense_data)

        sales_df.show()
        inventory_df.show()
        expense_df.show()
    except Exception as e:
        logging.error(f"Error creating Spark DataFrames: {e}")

finally:
    spark.stop()