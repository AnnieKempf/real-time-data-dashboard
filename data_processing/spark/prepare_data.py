import os
from  pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import DateType, StringType
import logging

logging.basicConfig(level=logging.ERROR)

try:
    spark = SparkSession.builder.appName('SimulateData').getOrCreate()

    # Combine the CSV-files into one
    def combine_csv_files(spark, folder_path):
        combined_df = spark.read.csv(folder_path, header=True, inferSchema=True)
        return combined_df   

    # Total Sales Over Time
    def aggregate_sales_over_time(sales_data, time_period='M'):
        sales_data = sales_data.withColumn('Date', F.col('Date').cast(DateType()))
        aggregated_sales = sales_data.groupBy(F.month('Date')).sum('Price')
        return aggregated_sales.withColumnRenamed("sum(Price)", "Total_Sales")

    # Sales Breakdown by Product Category
    def sales_breakdown_by_product(sales_data, column_to_sum='Price'):
        sales_by_product = sales_data.groupby('Product_Name').agg(F.sum(column_to_sum).alias('Total_' + column_to_sum))
        return sales_by_product

    # Current Inventory Levels
    def classify_stock_level_udf():
        def classify_stock_level(quantity):
            if quantity < 100:
                return 'LOW'
            elif quantity > 250:
                return 'HIGH'
            else:
                return 'MEDIUM'
        return F.udf(classify_stock_level, StringType())

    def prepare_inventory_levels_for_visualizations(inventory_data):
        total_quantity_by_category = inventory_data.groupby('Product_Category').agg(F.sum('Quantity').alias('Total_Quantity'))
        classify_udf = classify_stock_level_udf()
        inventory_levels = total_quantity_by_category.withColumn('Stock_Level', classify_udf(F.col('Total_Quantity')))
        return inventory_levels

    # Expense Breakdown by Category
    def prepare_expense_breakdown(expense_data):
        total_expenses_by_category = expense_data.groupby('Category').agg(F.sum('Amount').alias('Total_Expenses'))
        return total_expenses_by_category

    # Monthly Expense Trends
    def aggregate_expenses_over_time(expense_data, time_period='M'):
        expense_data = expense_data.withColumn('Date', F.col('Date').cast(DateType()))
        aggregated_expenses = expense_data.groupBy(F.month('Date')).sum('Amount')
        return aggregated_expenses.withColumnRenamed("sum(Amount)", "Total_Expenses")

    output_folder = r'data_processing\data_generation\prepared_data'
    # output_folder = "hdfs://localhost:8020/user/annie/spark_output"

    try:
        combined_sales_data = combine_csv_files(spark, r'data_processing\data_generation\sales_data')
        combined_inventory_data = combine_csv_files(spark, r'data_processing\data_generation\inventory_data')
        combined_expense_data = combine_csv_files(spark, r'data_processing\data_generation\expense_data')
    except Exception as e:
        logging.error(f"Error creating combined expense data: {e}")


    try:
        output_filename = 'monthly_sales_data'
        output_path = os.path.join(output_folder, output_filename)
        monthly_sales = aggregate_sales_over_time(combined_sales_data, 'M')
        monthly_sales.show()
        monthly_sales.coalesce(1).write.csv(output_path, mode='overwrite', header=True)
    except Exception as e:
        logging.error(f"Error creating monthly sales data: {e}")

    try:
        output_filename = 'sales_by_product'
        output_path = os.path.join(output_folder, output_filename)
        product_sales = sales_breakdown_by_product(combined_sales_data)
        product_sales.coalesce(1).write.csv(output_path, mode='overwrite', header=True)
    except Exception as e:
        logging.error(f"Error creating sales by product data: {e}")

    try:
        output_filename = 'inventory_levels'
        output_path = os.path.join(output_folder, output_filename)
        inventory_levels = prepare_inventory_levels_for_visualizations(combined_inventory_data)
        inventory_levels.coalesce(1).write.csv(output_path, mode='overwrite', header=True)
    except Exception as e:
        logging.error(f"Error creating inventory levels data: {e}")
    
    try:
        output_filename = 'product_inventory_levels'
        output_path = os.path.join(output_folder, output_filename)
        combined_inventory_data.coalesce(1).write.csv(output_path, mode='overwrite', header=True)
    except Exception as e:
        logging.error(f"Error creating product inventory levels data: {e}")

    try:
        output_filename = 'expense_data'
        output_path = os.path.join(output_folder, output_filename)
        expense_data = prepare_expense_breakdown(combined_expense_data)
        expense_data.coalesce(1).write.csv(output_path, mode='overwrite', header=True)
    except Exception as e:
        logging.error(f"Error creating expense data: {e}")

    try:
        output_filename = 'monthly_expenses_data'
        output_path = os.path.join(output_folder, output_filename)
        monthly_expenses = aggregate_expenses_over_time(combined_expense_data, 'M')
        monthly_expenses.coalesce(1).write.csv(output_path, mode='overwrite', header=True)
    except Exception as e:
        logging.error(f"Error creating monthly expense data: {e}")

finally:
    spark.stop()