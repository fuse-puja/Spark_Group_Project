# %% [markdown]
# # Data Analysis on Brazilian E-Commerce Public Dataset by Olist

# %% [markdown]
# Dataset link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("project").getOrCreate()

# %% [markdown]
# # Loading the dataset

# %%
# Defining path to the dataset
customer_data_path = "./Data/olist_customers_dataset.csv"  # Replace with the actual path
order_item_path = "./Data/olist_order_items_dataset.csv"
order_payment_path = "./Data/olist_order_payments_dataset.csv"
product_category_translation_path= "./Data/product_category_name_translation.csv"
product_path = './Data/olist_products_dataset.csv'
seller_path = './Data/olist_sellers_dataset.csv'
geolocation_path = './Data/olist_geolocation_dataset.csv'
orders_path = './Data/olist_orders_dataset.csv'

# Load the Chipotle dataset into a Spark DataFrame
customer_df = spark.read.csv(customer_data_path, header=True, inferSchema=True)
order_item_df = spark.read.csv(order_item_path, header=True, inferSchema=True)
order_payment_df = spark.read.csv(order_payment_path, header=True, inferSchema=True)
product_category_translation_df = spark.read.csv(product_category_translation_path, header=True, inferSchema=True)
seller_df_uncleaned = spark.read.csv(seller_path, header=True, inferSchema=True)
product_df_uncleaned = spark.read.csv(product_path, header=True, inferSchema=True)
geoloacation_df_uncleaned = spark.read.csv(geolocation_path, header=True, inferSchema= True)
orders_df_uncleaned = spark.read.csv(orders_path, header=True, inferSchema= True)

# %% [markdown]
# # Data Cleaning and pre-processing

# %%
from pyspark.sql.functions import col, trim,regexp_replace, when

# %% [markdown]
# ### Removing whitespace  

# %%
# Remove leading and trailing whitespace from all columns
seller_df_uncleaned.select([trim(col(c)).alias(c) for c in seller_df_uncleaned.columns])

# Remove whitespace characters between words in all columns
seller_df = seller_df_uncleaned.select([regexp_replace(col(c), r'\s+', ' ').alias(c) for c in seller_df_uncleaned.columns])


# %%
# Remove leading and trailing whitespace from all columns
geoloacation_df_uncleaned.select([trim(col(c)).alias(c) for c in geoloacation_df_uncleaned.columns])

geoloacation_df_uncleaned.show()

# %% [markdown]
# ### Working with inconsistent data

# %%
# Replace "são paulo" with "sao paulo" in the geolocation dataframe
geolocation_df = geoloacation_df_uncleaned.replace("são paulo", "sao paulo")

# Show the DataFrame with the replaced values
geolocation_df.show()

# %% [markdown]
# ### Drop null values

# %%
# Print the number of rows in the 'orders_df_uncleaned' DataFrame
print("No of rows in uncleaned dataset = ", orders_df_uncleaned.count())

# Drop rows with null values in the 'orders_df_uncleaned' DataFrame
orders_df = orders_df_uncleaned.dropna()

# Print the number of rows in the 'orders_df' DataFrame after dropping null values
print("No of rows of cleaned datset = ", orders_df.count())

# %% [markdown]
# ### Replacing column on product dataset with content from product category translation dataset

# %%
# Perform a left join between the 'product_df_uncleaned' DataFrame and 'product_category_translation_df'
# based on the 'Product_category_name' column. This operation combines the two DataFrames .
product_joined_df= product_df_uncleaned.join(product_category_translation_df, "Product_category_name", "left")

# Drop "product_category_name" will be removed from the DataFrame.
product_df = product_joined_df.drop("product_category_name")

# Rename the "product_category_name_english" column to "product_category_name"
product_df = product_df.withColumnRenamed("product_category_name_english", "product_category_name")

# Show the 'product_df' DataFrame with the dropped and renamed columns.
product_df.show()

# %%
# Set payment_installment to 0 where payment_type is "not_defined"
order_payment_df = order_payment_df.withColumn("Payment_installments",
                                   when(col("Payment_type") == "not_defined", 0)
                                   .otherwise(col("Payment_installments")))


# %% [markdown]
# ## Applying Transformation on the Dataframes 
# 

# %% [markdown]
# ### List of Dataframes:
#     -customer_df 
#     -order_item_df 
#     -order_payment_df 
#     -product_category_translation_df 
#     -seller_df
#     -product_df
#     -geoloacation_df
#     -orders_df

# %% [markdown]
# ### Create a pivot table to find the number of transactions made by customers using different payment_types for each state.

# %%
from pyspark.sql.functions import count

orders_customer_df = orders_df.join(customer_df, "customer_id")

# Joined orders_customer_df with payment_df to get payment type
orders_payment_df = orders_customer_df.join(order_payment_df, "order_id")

# Grouped by 'customer_state' and 'payment_type' and count the orders
pivot_table = orders_payment_df.groupBy('customer_state', 'payment_type').agg(count('order_id').alias('order_count'))

# Pivoted the table to create the desired pivot table
pivot_table = pivot_table.groupBy('customer_state').pivot('payment_type').sum('order_count')

# Filled missing values with 0
pivot_table = pivot_table.na.fill(0)

pivot_table.show()

# %% [markdown]
# ###  Find the total number of active sellers and how they have changed over time. This question helps to find the sellers who are actively selling their product i.e. they have not canceled their orders.

# %%
from pyspark.sql.functions import year, countDistinct

joined_df = order_item_df.join(orders_df, order_item_df.order_id == orders_df.order_id) \
    .join(order_payment_df, order_item_df.order_id == order_payment_df.order_id) \
    .join(seller_df.alias('s'), order_item_df.seller_id == seller_df.seller_id) \
    .join(product_df.alias('p'), order_item_df.product_id == product_df.product_id)

# Extract the year from order_approved_at column
joined_df = joined_df.withColumn("year", year(col("order_approved_at")))

# Calculate the counts
result_df = joined_df.groupBy("year") \
    .agg(countDistinct("s.seller_id").alias("active_seller"), countDistinct("p.product_id").alias("product_count"))

# Order the result
result_df = result_df.orderBy("year", col("active_seller").desc())

result_df.show()

# %% [markdown]
# ### Find the customer shares based on the states. This question helps to provide the insights about the percentage of customers making in each state.

# %%
from pyspark.sql.window import Window
from pyspark.sql.functions import count,desc,col,sum,round

# Define a window specification for the running total calculation
window_spec = Window.orderBy(desc("no_customers"))

cteDF = customer_df.groupBy("customer_state") \
    .agg(count("customer_unique_id").alias("no_customers")) \
    .orderBy(desc("no_customers")) \
    .withColumn("percentage_customer_base", round(col("no_customers") / sum("no_customers").over(Window.partitionBy().orderBy())* 100, 2) ) \
    .withColumn("running_total_percentage", round(sum("no_customers").over(window_spec) / sum("no_customers").over(Window.partitionBy().orderBy())* 100, 2) )

resultDF = cteDF.orderBy(desc("no_customers"))
resultDF.show()

# %% [markdown]
# ### Find the states whose sales value is higher than the buy value.

# %%

customer_df.createOrReplaceTempView("customer")
order_item_df.createOrReplaceTempView("order_item")
order_payment_df.createOrReplaceTempView("order_payment")
geolocation_df.createOrReplaceTempView("location")
seller_df.createOrReplaceTempView("seller")
product_df.createOrReplaceTempView("product")
orders_df.createOrReplaceTempView("orders")

# %%
from pyspark.sql.functions import sum
customer_seller_comparison_SQL  = spark.sql('''SELECT a.order_id, a.price,a.seller_id,b.seller_state, c.customer_id,d.customer_state
                                    FROM order_item a 
                                    INNER JOIN seller b ON a.seller_id = b.seller_id
                                    INNER JOIN orders c ON a.order_id = c.order_id
                                    INNER JOIN customer d ON d.customer_id=c.customer_id
''')
                                            
# customer_seller_comparison_SQL.show()
comparision_df = customer_seller_comparison_SQL

comparision_df = comparision_df.dropDuplicates()

comparision_df.count()

seller_df = comparision_df.groupBy(['seller_state', 'seller_id']).agg(sum('price').alias('sell_value'))

# Group by 'seller_state' and sum 'sell_value'
seller_df = seller_df.groupBy('seller_state').agg(sum('sell_value').alias('sell_value'))

# seller_df.show()

# %%
buyer_df = comparision_df.groupBy(['customer_state', 'customer_id']).agg(sum('price').alias('buy_value'))

# Group by 'customer_state' and sum 'buy_value'
buyer_df = buyer_df.groupBy('customer_state').agg(sum('buy_value').alias('buy_value'))

# buyer_df.show()

# %%
from pyspark.sql.functions import col,udf
from pyspark.sql.types import StringType

# Join buyer_df and seller_df on 'customer_state' and 'seller_state'
compare_buy_sell_activity = buyer_df.alias("buyer").join(
    seller_df.alias("seller"),
    col("buyer.customer_state") == col("seller.seller_state"),
    "left"
)

# Fill missing values with 0 for 'sell_value' and 'seller_state'
compare_buy_sell_activity = compare_buy_sell_activity.fillna(0, subset=["sell_value"])
compare_buy_sell_activity = compare_buy_sell_activity.withColumn(
    "seller_state",
    col("buyer.customer_state")
)

# Calculate the margin activity
compare_buy_sell_activity = compare_buy_sell_activity.withColumn(
    "margin_activity",
    compare_buy_sell_activity["sell_value"] - compare_buy_sell_activity["buy_value"]
)

def encode_margin_udf(value):
    if value < 0:
        return 'consumer_dominant'
    elif value == 0:
        return 'balanced'
    elif value > 0:
        return 'seller_dominant'

# Register the UDF
encode_margin_udf_spark = udf(encode_margin_udf, StringType())

compare_buy_sell_activity = compare_buy_sell_activity.withColumn(
    "margin_category",
    encode_margin_udf_spark(compare_buy_sell_activity["margin_activity"])
)

compare_buy_sell_activity.show()

# %% [markdown]
# ### Find the total number of orders placed by customers every hour in each day of week .
# 

# %%
query_result_order = orders_df.select("order_purchase_timestamp", "order_id")
query_result_order.show(5)

# %%
print("Null Checking")
null_counts = query_result_order.select([col(c).alias(c) for c in query_result_order.columns]).na.drop().count()
print("Number of null values in each column:")
query_result_order.select([col(c).alias(c) for c in query_result_order.columns]).na.drop().show()
print(f"Total null values: {query_result_order.count() - null_counts}")

# Duplicate data checking
print("Duplicate Data Checking")
duplicate_count = query_result_order.count() - query_result_order.dropDuplicates().count()
print(f"Number of duplicate rows: {duplicate_count}")

# Drop duplicate data
query_result_order = query_result_order.dropDuplicates()
print("After Removing Duplicate Data")
query_result_order.show()


# %%
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col,date_format,hour

print("Dtypes:")
query_result_order.printSchema()

# Convert the 'order_purchase_timestamp' column to a timestamp type
query_result_order = query_result_order.withColumn("order_purchase_timestamp", col("order_purchase_timestamp").cast(TimestampType()))

# Parse day name
query_result_order = query_result_order.withColumn("Day", date_format(col("order_purchase_timestamp"), "EEEE"))

# Parse hour
query_result_order = query_result_order.withColumn("Hour", hour(col("order_purchase_timestamp")))

# Check data types after conversion
print("Dtypes After Conversion:")
query_result_order.printSchema()

# %%
from pyspark.sql.functions import col, count
from pyspark.sql import functions as F

custom_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# Use the 'when' function to create a custom sorting column
day_hour_group = query_result_order.groupBy('Hour').pivot('Day').agg(count('order_id'))
day_hour_group = day_hour_group.select(
    ['Hour'] + [F.col(day).alias(day) for day in custom_order]
)

# Order the DataFrame by 'Hour'
day_hour_group = day_hour_group.orderBy('Hour', ascending=False)

# Show the result
day_hour_group.show()

# %% [markdown]
# #### #Payment Analysis: For each payment type (payment_type), calculate the total payment value (sum of payment_value) and the average number of payment installments (payment_installments), rename the columns to 'Total Payment Value' and 'Avg Installments,' and order the result by payment type in ascending order

# %%
# Perform the analysis
from pyspark.sql.functions import avg
payment_analysis = order_payment_df.groupBy("Payment_type") \
    .agg(
        sum("Payment_value").alias("Total Payment Value"),
        avg("Payment_installments").alias("Avg Installments")
    ) \
    .orderBy("Payment_type")

# Show the result
payment_analysis.show()

# %% [markdown]
# #### #Growth analysis: Determine the month-over-month sales growth percentage, using a window function to compare the current month's total sales with the previous month's total sales for each seller (seller_id).

# %%
# Join the orders and order_items datasets
from pyspark.sql.functions import lag
combined_df = orders_df.join(order_item_df, "Order_id")

# Calculate monthly sales for each seller
monthly_sales_window = Window.partitionBy("Seller_id").orderBy("YearMonth")
combined_df = combined_df.withColumn("YearMonth", combined_df["Order_purchase_timestamp"].substr(1, 7))
monthly_sales_df = combined_df.groupBy("Seller_id", "YearMonth").agg(sum("Price").alias("monthly_sales"))
monthly_sales_df = monthly_sales_df.withColumn("monthly_sales", monthly_sales_df["monthly_sales"].cast("float"))
monthly_sales_df = monthly_sales_df.withColumn("prev_month_sales", lag("monthly_sales").over(monthly_sales_window))

# Calculate month-over-month sales growth percentage
monthly_sales_df = monthly_sales_df.withColumn("sales_growth_percentage",
                                               ((monthly_sales_df["monthly_sales"] - monthly_sales_df["prev_month_sales"]) /
                                                monthly_sales_df["prev_month_sales"]) * 100)

# Show the result
monthly_sales_df.select("Seller_id", "YearMonth", "monthly_sales", "prev_month_sales", "sales_growth_percentage").show()

# %% [markdown]
# #### #Total number of orders placed by customers in each state (customer_state), rename the column to 'Order Count,' and order the result in ascending order of order count

# %%
# Join customers and orders datasets to identify unique customers in the orders dataset
unique_customers_df = customer_df.join(orders_df, "Customer_id")

# Group by customer_state and count the number of orders in each state
order_count_by_state = unique_customers_df.groupBy("customer_state").agg(count("Order_id").alias("Order Count"))

# Order the result in ascending order of order count
order_count_by_state = order_count_by_state.orderBy("Order Count")

# Show the result
order_count_by_state.show()




# %% [markdown]
# ### #Calculate the order count per customer, computes the average order value per customer, create a customer and order analysis DataFrame, and display the result. Also print the total number of customers.

# %%
# Join the Orders DataFrame and Order Payments DataFrame based on "Order_id"
order_payment_joined_df = orders_df.join(order_payment_df , "Order_id")

# Calculate order count per customer
order_count = orders_df.groupBy("Customer_id").agg(count("Order_id").alias("Order_Count"))


# Calculate average order value per customer
average_order_value = order_payment_joined_df.groupBy("Customer_id").agg(avg("Payment_value").alias("Average_Order_Value"))


# Create a customer and order analysis DataFrame
customer_behaviour_df = order_count.join(average_order_value, "Customer_id", "inner").orderBy(desc("Average_Order_Value"))

# Show the resulting DataFrame
customer_behaviour_df.show()
print("Total NUmber of Customer = ", customer_behaviour_df.count())


# %% [markdown]
# ### #Create a UDF to categorize products into different size categories based on their dimensions (length, width, and height) and weight? Then, calculate the average order value for each product size category.

# %%
# Define a UDF to categorize products into size categories based on dimensions and weight
def categorize_product_size(length, width, height, weight):
    if length is not None and width is not None and height is not None and weight is not None:
        if length <= 20 and width <= 20 and height <= 20 and weight <= 500:
            return "Small"
        elif length <= 40 and width <= 40 and height <= 40 and weight <= 2000:
            return "Medium"
        else:
            return "Large"
    else:
        return "Unknown"

# Register the UDF
categorize_udf = udf(categorize_product_size, StringType())

# Apply the UDF to create a new column "Product_Size_Category"
product_df = product_df.withColumn("Product_Size_Category", categorize_udf(
    product_df["Product_length_cm"],
    product_df["Product_width_cm"],
    product_df["Product_height_cm"],
    product_df["Product_weight_g"]
))

# Join the necessary datasets
joined_df = order_item_df.join(product_df, "Product_id")

# Calculate the average order value for each product size category
average_order_value_by_size = joined_df.groupBy("Product_Size_Category").agg(
    round(avg("Price"),2).alias("Average_Order_Value")
)

# Show the resulting DataFrame
average_order_value_by_size.show()

# %%



