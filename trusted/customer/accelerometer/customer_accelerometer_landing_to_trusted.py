from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Create a SparkContext
sc = SparkContext()

# Create a GlueContext
glueContext = GlueContext(sc)

# Create a SparkSession
spark = glueContext.spark_session

# Read the customer data from the landing zone
customer_landing = spark.read.json("s3://balalal/customer_landing/")

# Read the accelerometer data from the landing zone
accelerometer_landing = spark.read.json("s3://balalal/accelerometer_landing/")

# Perform data sanitization and filtering operations
sanitized_customer = customer_landing.filter(customer_landing['shareWithResearchAsOfDate'].isNotNull())

# Select the desired columns from the sanitized customer data
customer_trusted = sanitized_customer.select(
    'customerName', 'email', 'phone', 'birthDay', 'serialNumber', 'registrationDate',
    'lastUpdateDate', 'shareWithResearchAsOfDate', 'shareWithPublicAsOfDate', 'shareWithFriendsAsOfDate'
)

# Filter the accelerometer data for customers who have agreed to share
sanitized_accelerometer = accelerometer_landing.join(
    sanitized_customer, accelerometer_landing['user'] == sanitized_customer['email']
)

# Select the desired columns from the sanitized accelerometer data
accelerometer_trusted = sanitized_accelerometer.select('timeStamp', 'user', 'x', 'y', 'z')

# Write the sanitized customer data to the trusted zone
customer_trusted.write.json("s3://balalal/customer_trusted/")

# Write the sanitized accelerometer data to the trusted zone
accelerometer_trusted.write.json("s3://balalal/accelerometer_trusted/")
