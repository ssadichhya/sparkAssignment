from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, desc, avg, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



# Create a Spark session
spark = SparkSession.builder.appName("day2").getOrCreate()

# Load the Chipotle dataset into a Spark DataFrame
data_path = "occupation.csv"  # Replace with the actual path
occupation = spark.read.csv(data_path, header=True, inferSchema=True)

occupation.printSchema()

occupation.select("user_id","age","occupation").show()

occupation.select("*").filter(col("age")>30).show()

occupation.groupBy("occupation").count().show()

occupation.withColumn("age_group",
                      when((occupation.age>=18) & (occupation.age<=25), lit("18-25"))\
                      .when((occupation.age>=26) & (occupation.age<=35), lit("18-25"))\
                      .when((occupation.age>=36) & (occupation.age<=50), lit("18-25"))\
                      .otherwise(lit("51+"))\
                        ).show()

# Define the schema
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Sample data
data = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1)
]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Print the schema
df.printSchema()

# Display the content without truncation
df.show(truncate=False)


# occupation=occupation.withColumn("gender")
occupation=occupation.withColumnRenamed("age","Years")
occupation.show()

occupation.select("*").filter(col("age")<30).orderBy(desc("age")).show()

# Repartition the DataFrame into 2 partitions without shuffling
df_repartitioned = df.repartition(2)
# Collect and display all rows in the driver
rows = df_repartitioned.collect()
for row in rows:
    print(row)


# Print the number of partitions
num_partitions = df_repartitioned.rdd.getNumPartitions()
print("Number of partitions = ", num_partitions)

# Spark SQL
occupation.createOrReplaceTempView("people")
query="""SELECT *,\
           CASE WHEN age > 30 THEN True ELSE False END as is_elderly,\
           gender as sex\
    FROM people\
    WHERE age > 30"""
spark.sql(query).show()


# Pyspark
# Filter out rows where age is greater than 30
filtered_occupation = occupation.filter(occupation["age"] > 30)

# Add a new column "is_elderly" based on age
result_occupation = filtered_occupation.withColumn("is_elderly", 
when(occupation.age > 30, "True").otherwise("False"))

# Rename the "gender" column to "sex"
result_occupation = result_occupation.withColumnRenamed("gender", "sex")

result_occupation.show()


# Spark SQL
occupation.createOrReplaceTempView("people")
query="""select gender,avg(age) as avg_age
from people
group by gender"""
spark.sql(query).show()


# Pyspark

avg_age_df = occupation.groupBy("gender").agg(avg("age").alias("avg_age"))
avg_age_df.show()

query="""select *, CONCAT(user_id,occupation) as full_name
from new_occupation
"""
spark.sql(query).show()

# Pyspark
# Add a new column "full_name" by concatenating "user_id" and "occupation"
df_with_full_name = occupation.withColumn("full_name", concat(col("user_id"), col("occupation")))

# Rename the "zip_code" column to "postal_code"
df_renamed = df_with_full_name.withColumnRenamed("zip_code", "postal_code")

df_renamed.show()


# Spark SQL
occupation.createOrReplaceTempView("new_occupation")
query="""select user_id,age,age - (SELECT AVG(age) FROM new_occupation WHERE occupation == 'technician') AS age_diff
 from new_occupation where occupation="technician"
"""
spark.sql(query).show()


# Pyspark
filtered_df = occupation.filter(col("occupation") == "technician")

selected_df = filtered_df.select("user_id", "age")

average_age = occupation.select(avg("age")).first()[0]

result_df = selected_df.withColumn("age_diff", col("age") - average_age)

result_df.show()


male_df = df.filter(df["gender"] == "M")
female_df = df.filter(df["gender"] == "F")

repartitioned_df_male=male_df.repartition(2)
repartitioned_df_female=female_df.repartition(2)

combined_df = male_df.union(female_df)

combined_df.show()


user_data_schema = "user_id INT, name STRING, age INT"

user_data = [
    (1, "John", 30),
    (2, "Jane", 25),
    (3, "Michael", 40),
    (4, "Emily", 35),
    (5, "David", 28)
]

user_data_df = spark.createDataFrame(user_data, schema=user_data_schema)

user_ratings_schema = "user_id INT, rating INT"

user_ratings = [
    (1, 8),
    (2, 9),
    (3, 7),
    (5, 10)
]

user_ratings_df = spark.createDataFrame(user_ratings, schema=user_ratings_schema)

combined_df = user_data_df.join(user_ratings_df, on="user_id")

combined_df.show()
