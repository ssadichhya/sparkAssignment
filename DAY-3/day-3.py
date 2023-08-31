from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,avg,regexp_extract,coalesce,regexp_replace,struct,create_map,explode,to_json,from_json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()


# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()

# Load the Chipotle dataset into a Spark DataFrame
data_path = "titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = 'chipotle.csv' # Replace with the actual path
chipotle_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = 'kalimati_tarkari_dataset.csv' # Replace with the actual path
kalimati_df = spark.read.csv(data_path, header=True, inferSchema=True)

### Converting to Spark Types:

# Convert the "fare" column from double to integer
titanic_df = titanic_df.withColumn("fare", col("fare").cast("int"))
titanic_df.printSchema()

### Working with Booleans:

titanic_df_age_gt_18=titanic_df.withColumn("IsAdult", when(col("age")>=18,True).otherwise(False))
titanic_df_age_gt_18.show()


### Working with Numbers:

#Load the "titanic" dataset and calculate the average age of male and female passengers separately.
titanic_df_gender=titanic_df.groupBy("Sex").agg(avg("age")).alias("Avg")
titanic_df_gender.show()


### Working with Strings:

# Filter for item names containing the word "Chicken"
chicken_items = chipotle_df.filter(col("item_name").like("%Chicken%"))
chicken_items.show()


### Regular Expressions:

# Filter for item names that starts with "Ch"
chicken_items = chipotle_df.filter(col("item_name").like("Ch%"))
chicken_items.show()


### Working with Nulls in Data:

# Count the number of passengers with missing age informationd
missing_age_count = titanic_df.filter(col("age").isNull()).count()

# Show the count
print("Number of passengers with missing age information:", missing_age_count)


### Coalesce


# Combine the "item_name" and "choice_description" columns using coalesce

combined_df = chipotle_df.withColumn("OrderDetails", coalesce(col("item_name"), col("choice_description")))
combined_df.show()


### ifnull, nullIf, nvl, and nvl2

avg_age=titanic_df.agg(avg(col("age"))).first()[0]
# avg_age

# Replace null values in the "age" column with the average age using ifnull
# titanic_df_ifnull = titanic_df.withColumn("age", ifnull(col("age"), average_age))
# titanic_df_ifnull.show()

# Replace null values in the "age" column with the average age using when
titanic_df_replace_null = titanic_df.withColumn("age", when(col("age").isNull(), avg_age).otherwise(col("age")))
titanic_df_replace_null.show()


### drop

#remove the "Cabin" column from the Titanic dataset.
titanic_df.drop(col("Cabin")).show()


### fill

# Fill null values in the "age" column with a default age of 30 using .na.fill

titanic_df.na.fill(30, subset=["age"]).show()


###  replace
# Replace "male" with "M" and "female" with "F" in the "Sex" column using regexp_replace
titanic_df.na.replace({"male":"m","female":"f"},"sex").show()


### Working with Complex Types: Structs


#struct containing "Minimum" and "Maximum" prices for each commodity
kalimati_df.select(struct("Minimum","Maximum").alias("PriceRange"),"*").show()

### Working with Complex Types: Arrays

#new DataFrame from the Kalimati Tarkari dataset, including a new column "CommodityList" that is an array of all the commodities

complex_array_df = kalimati_df.selectExpr("*","array(Commodity) as CommodityList")
complex_array_df.show()


### Working with Complex Types: explode

#explode commoditylist array using explode method
complex_array_df.withColumn("exploded",explode("CommodityList")).show(5)



### Working with Complex Types: Maps


#Create a new DataFrame from the Kalimati Tarkari dataset, including a new column "PriceMap" that is a map with "Commodity" as the key and "Average" price as the value
kalimati_df.select(create_map(col("Commodity"),col("Average")).alias("PriceMap"),"*").show()


### Working with JSON
kalimatiJSON = kalimati_df.selectExpr("(Sn,Commodity,date,unit,minimum,maximum,average) as KalimatiJson").select(to_json(col("KalimatiJson")))
# jsonKalimati.show()
# kalimatiJSON.write.json("kalimatiJSON.json")