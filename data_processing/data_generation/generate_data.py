import pandas as pd
from faker import Faker
import random
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.ERROR)

try:
    spark = SparkSession.builder.appName('SimulateData').getOrCreate()
    faker = Faker()

    num_records = 2000

    product_categories = [
    "Hybrid Vehicle", "Electric Vehicle", "Performance Vehicle",
    "Commercial Vehicle", "Standard Vehicle", "Service and Maintenance",
    "Parts and Accessories", "Financing and Insurance", "Special Offers"
    ]

    products_dict = {
        "Hybrid Vehicle": ["Compact Hybrid Sedan", "Midsize Hybrid SUV", "Luxury Hybrid Coupe", "Hybrid Hatchback", "Full-Size Hybrid Pickup Truck"],
        "Electric Vehicle": ["All-Electric Compact Car", "Electric Crossover SUV", "Electric Luxury Sedan", "Electric Sports Car", "Electric Commercial Van"],
        "Performance Vehicle": ["High-Performance Sports Coupe", "Luxury Performance Sedan", "High-Speed Performance SUV", "Track-Focused Sports Car", 
                                "Performance Electric Model"],
        "Commercial Vehicle": ["Heavy-Duty Pickup Truck", "Cargo Van", "Chassis Cab", "Box Truck", "Passenger Wagon"],
        "Standard Vehicle": ["Compact Sedan", "Midsize SUV", "Full-Size Sedan", "Small Hatchback", "Station Wagon"],
        "Service and Maintenance": ["Oil Change and Lubrication Service", "Tire Rotation and Alignment", "Brake Inspection and Replacement", 
                                    "Battery Check and Replacement", "Engine Diagnostic and Repair"],
        "Parts and Accessories": ["Custom Alloy Wheels", "Vehicle Navigation Systems", "All-Weather Floor Mats", "Roof Racks and Carriers", 
                                  "Car Audio and Entertainment Systems"],
        "Financing and Insurance": ["New Vehicle Loan", "Used Vehicle Financing", "Lease Agreements", "Extended Warranty Coverage", "Vehicle Insurance Plans"],
        "Special Offers": ["Limited-Time Discount on New Models", "Trade-In Bonus Offer", "Seasonal Service Package Discount", "Financing Rate Promotion", 
                           "Exclusive Accessory Bundle Deal"]
    }

    expense_categories = [
        "Vehicle Procurement", "Parts and Inventory", "Research and Development", "Manufacturing and Production", "Marketing and Advertising", 
        "Facility Maintenance and Utilities", "Employee Salaries and Benefits", "Logistics and Transportation", "Legal and Compliance", 
        "Technology and IT Infrastructure"
    ]


    # Sales Transaction Data
    try:
        def get_random_product(category):
            return random.choice(products_dict[category])

        sales_data = pd.DataFrame({
            'Transaction_ID': range(1, num_records + 1),
            'Customer_ID': [faker.random_number(digits=5) for _ in range(num_records)],
            'Product_Category': [random.choice(product_categories) for _ in range(num_records)],
            'Quantity': [random.randint(1, 10) for _ in range(num_records)],
            'Price': [round(random.uniform(1000, 3000000), 2) for _ in range(num_records)],
            'Date': [pd.to_datetime(faker.date_between(start_date='-1y', end_date='today')) for _ in range(num_records)]
        }
        )
        sales_data['Product_Name'] = sales_data['Product_Category'].apply(get_random_product)
        print(sales_data.head())

    except Exception as e:
        logging.error(f"Error generating sales data: {e}")


    # Inventory Management
    try:
        inventory_data = []
        for category, products in products_dict.items():
            for product in products:
                inventory_data.append({
                    'Product_Category': category,
                    'Product_Name': product,  # Individual product name
                    'Quantity': random.randint(1, 100)
                })

        inventory_data = pd.DataFrame(inventory_data)

        def determine_stock_level(quantity):
            if quantity < 10:
                return "LOW"
            elif quantity > 50:
                return "HIGH"
            else:
                return "MEDIUM"

        inventory_data['Stock_Level'] = inventory_data['Quantity'].apply(determine_stock_level)

        print(inventory_data.head())

    except Exception as e:
        logging.error(f"Error generating inventory data: {e}")


    # Business Expense Records
    try:
        expense_data = pd.DataFrame({
            'Expense_ID': range(1, num_records + 1),
            'Category': [random.choice(expense_categories) for _ in range(num_records)],
            'Amount': [round(random.uniform(5000, 500000), 2) for _ in range(num_records)],
            'Date': [faker.date_between(start_date='-1y', end_date='today') for _ in range(num_records)],
            'Description': [faker.sentence() for _ in range(num_records)]
        })
        print(expense_data.head())
    except Exception as e:
        logging.error(f"Error generating expense data: {e}")

    try:
        sales_df = spark.createDataFrame(sales_data)
        inventory_df = spark.createDataFrame(inventory_data)
        expense_df = spark.createDataFrame(expense_data)

        sales_df.show()
        inventory_df.show()
        expense_df.show()

        sales_df.write.csv("data_processing\data_generation\sales_data", header=True, mode="overwrite")
        inventory_df.write.csv("data_processing\data_generation\inventory_data", header=True, mode="overwrite")
        expense_df.write.csv("data_processing\data_generation\expense_data", header=True, mode="overwrite")

    except Exception as e:
        logging.error(f"Error creating Spark DataFrames: {e}")


finally:
    spark.stop()
