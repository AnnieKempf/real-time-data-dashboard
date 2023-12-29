import pandas as pd
import os
from  pyspark.sql import SparkSession
from  pyspark.sql.functions import sum as _sum
from pyspark.sql import functions as F 
from pyspark.sql.types import DateType
import logging

logging.basicConfig(level=logging.ERROR)

spark = SparkSession.builder.appName('SimulateData').getOrCreate()


def combine_csv_files(spark, folder_path):
    combined_df = spark.read.csv(folder_path, header=True, inferSchema=True)
    return combined_df   

combined_sales_data = combine_csv_files(spark, r'data_processing\data_generation\sales_data')
combined_inventory_data = combine_csv_files(spark, r'data_processing\data_generation\inventory_data')
combined_expense_data = combine_csv_files(spark, r'data_processing\data_generation\expense_data')


# Total Sales Over Time
def aggregate_sales_over_time(sales_data, time_period='M'):
    sales_data = sales_data.withColumn('Date', F.col('Date').cast(DateType()))
    aggregated_sales = sales_data.groupBy(F.month('Date')).sum('Price')
    return aggregated_sales.withColumnRenamed("sum(Price)", "Total_Sales")

output_folder = r'data_processing\data_generation\prepared_data'
output_filename = 'monthly_sales_data.csv'
output_path = os.path.join(output_folder, output_filename)

monthly_sales = aggregate_sales_over_time(combined_sales_data, 'M')
monthly_sales.write.csv(output_path, mode='overwrite', header=True)


# Sales Breakdown by Product Category
def sales_breakdown_by_product(sales_data, column_to_sum='Price'):
    sales_by_product = sales_data.groupby('Product_Name').sum()[column_to_sum]
    return sales_by_product.reset_index()

output_folder = 'data_processing\data_generation\prepared_data'
output_filename = 'sales_by_product.csv'
output_path = os.path.join(output_folder, output_filename)

product_sales = sales_breakdown_by_product(combined_sales_data)
product_sales.to_csv(output_path, index=False, mode='w')


# Current Inventory Levels
def classify_stock_level(quantity):
    if quantity < 100:
        return 'LOW'
    elif quantity > 250:
        return 'HIGH'
    else:
        return 'MEDIUM'
    
def prepare_inventory_levels_for_visualizations(inventory_data):
    total_quantity_by_category = inventory_data.groupby('Product_Category').sum()['Quantity']
    inventory_levels = pd.DataFrame({
        'Product_Category': total_quantity_by_category.index,
        'Total_Quantity': total_quantity_by_category.values,
        'Stock_Level': total_quantity_by_category.apply(classify_stock_level)
    })
    return inventory_levels

output_folder = 'data_processing\data_generation\prepared_data'
output_filename = 'inventory_levels.csv'
output_path = os.path.join(output_folder, output_filename)

inventory_levels = prepare_inventory_levels_for_visualizations(combined_inventory_data)
inventory_levels.to_csv(output_path, index=False, mode='w')


# Stock Alerts
output_folder = 'data_processing\data_generation\prepared_data'
output_filename = 'product_inventory_levels.csv'
output_path = os.path.join(output_folder, output_filename)

combined_inventory_data.to_csv(output_path, index=False, mode='w')


# Expense Breakdown by Category
def prepare_expense_breakdown(expense_data):
    total_expenses_by_category = expense_data.groupby('Category').sum()['Amount']
    expense_breakdown = pd.DataFrame({
        'Category': total_expenses_by_category.index,
        'Total_Expenses': total_expenses_by_category.values
    })
    return expense_breakdown

output_folder = 'data_processing\data_generation\prepared_data'
output_filename = 'expense_data.csv'
output_path = os.path.join(output_folder, output_filename)

expense_data = prepare_expense_breakdown(combined_expense_data)
expense_data.to_csv(output_path, index=False, mode='w')


# Monthly Expense Trends
def aggregate_expenses_over_time(expense_data, time_period='M'):
    expense_data['Date'] = pd.to_datetime(expense_data['Date'], utc=True, infer_datetime_format=True)
    expense_data.set_index('Date', inplace=True)
    print("Index type: ", expense_data.index.dtype)
    aggregated_expenses = expense_data.resample(time_period).sum()['Amount']
    return aggregated_expenses.reset_index()

output_folder = 'data_processing\data_generation\prepared_data'
output_filename = 'monthly_expenses_data.csv'
output_path = os.path.join(output_folder, output_filename)

monthly_expenses = aggregate_expenses_over_time(combined_expense_data, 'M')
monthly_expenses.to_csv(output_path, index=False, mode='w')