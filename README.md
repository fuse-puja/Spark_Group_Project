
# Data Analysis of Brazalian Ecommerce Dataset

**Author:**   
Bipin Ghimire - bipin.ghimire@fusemachines.com  
Puja Pathak - puja.pathak@fusemachines.com  

## Prerequisites

- Download Apache Spark
- Create a virtual environment
- Install the requirement.txt file using  
   pip install -r requirements.txt

## Running the Code

- Clone the repo
- Run: spark-submit BrazalianEcommerce.py


# Dataset Overview 
Brazilian ecommerce public dataset

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_geolocation_dataset.csv  

The different CSV files are:

This project involves the analysis of multiple datasets related to Brazilian e-commerce. The datasets provide valuable insights into customer behavior, order details, product information, and more. The goal of this analysis is to gain a deeper understanding of the e-commerce business in Brazil, identify trends, and extract useful information.

## Datasets

### Customers Dataset

- **Customer_id**: Unique identifier for each customer. Links to orders.
- **Customer_unique_id**: A unique identifier for the customer.
- **Customer_zip_code_prefix**: The first five digits of the customer's zip code.
- **Customer_city**: The name of the customer's city.
- **Customer_state**: The state of the customer.

### Order Items Dataset

- **Order_id**: Unique identifier for each order.
- **Order_item_id**: Sequential number identifying the number of items in an order.
- **Product_id**: Unique identifier for the product.
- **Seller_id**: Unique identifier for the seller.
- **Shipping_limit_date**: Seller's shipping limit date.
- **Price**: Item price.
- **Freight_value**: Item freight value.

### Payment Dataset

- **Order_id**: Unique identifier for the order.
- **Payment_sequential**: Sequential number for multiple payment methods.
- **Payment_type**: Chosen payment method.
- **Payment_installments**: Number of installments chosen.
- **Payment_value**: Transaction value.

### Orders Dataset

- **Order_id**: Unique identifier for the order.
- **Customer_id**: Links to customer dataset.
- **Order_status**: Order status (e.g., delivered, shipped).
- **Order_purchase_timestamp**: Purchase timestamp.
- **Order_approved_at**: Payment approved timestamp.
- **Order_delivered_carrier_data**: Order posting timestamp.
- **Order_delivered_customer_date**: Actual order delivery date.
- **Order_estimated_delivery_date**: Estimated delivery date.

### Product Dataset

- **Product_id**
- **Product_category_name**
- **Product_name_length**
- **Product_description_length**
- **Product_photo_qty**
- **Product_weight_g**
- **Product_length_cm**
- **Product_height_cm**
- **Product_width_cm**

### Seller Dataset

- **Seller_zip_code_prefix**
- **Seller_city**
- **Seller_state**

### Product Category Name Translation Dataset

- **Product_category_name**
- **Product_category_name_english**

### Geolocation Dataset

- **Zip_code**
- **Geo_latitude**
- **Geo_longitude**
- **Geolocation_city**
- **Geolocation_state**

## Analysis Tasks

**Question 1:**  
Create a pivot table to find the number of transactions made by customers using different payment methods for each state


**Question 2:**  
Find the total number of active sellers and how they have changed over time. This question helps to find the sellers who are actively selling their product i.e. they have not canceled their orders.


**Question 3:**  
Customer population based on the states.

**Question 4:**  
Compare the states based on their sale values and buy value.

**Question 5:**
Find the total number of orders placed by customers every hour in each day of week 

**Question 6:**  
**Payment Analysis:** For each payment type (payment_type), calculate the total payment value (sum of payment_value) and the average number of payment installments (payment_installments), rename the columns to 'Total Payment Value' and 'Avg Installments,' and order the result by payment type in ascending order

**Question 7:**  
Total number of orders placed by customers in each state (customer_state), rename the column to 'Order Count,' and order the result in ascending order of order count

**Question 8**  
Calculate the order count per customer, computes the average order value per customer, create a customer and order analysis DataFrame, and display the result. Also print the total number of customers

**Question 9:** 
Create a UDF to categorize products into different size categories based on their dimensions (length, width, and height) and weight? Then, calculate the average order value for each product size category

**Question 10:**  
Growth analysis: Determine the month-over-month sales growth percentage, using a window function to compare the current month's total sales with the previous month's total sales for each seller (seller_id).


