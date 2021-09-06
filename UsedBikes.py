from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Filter the royal enfield bikes which doesn't have fourth and more owner and age is limit to 2 or less than 2
def filter_royal_enfield_used_bikes(input_df):
    return input_df \
        .filter(col("brand") == 'Royal Enfield') \
        .filter(col("Owner") != 'Fourth Owner Or More') \
        .filter(col("age") <= 2)


# Select the Brand Name and Model Name and group by age and owner wise count
def group_by_age_and_owner_wise_count(input_df):
    return input_df \
        .select("bike_name", "Owner", "age") \
        .groupBy("bike_name", "Owner", "age") \
        .count()


# Main Function
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("RoyalEnfieldUsedBikes") \
        .master("local[*]") \
        .getOrCreate()

    source_used_bike_DF = spark.read.option("header", "true").csv("datasets")
    royal_Enfield_used_bikes_DF = source_used_bike_DF \
        .transform(filter_royal_enfield_used_bikes) \
        .transform(group_by_age_and_owner_wise_count)
    royal_Enfield_used_bikes_DF.show(100, False, False)
