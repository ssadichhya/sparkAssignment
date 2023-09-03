from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, approx_count_distinct,first, last, min, max,sum,sumDistinct,avg,col,desc,asc,struct
from pyspark.sql.window import Window
# Create a Spark session

spark = SparkSession.builder.appName("day4").getOrCreate()


# Load the Chipotle dataset into a Spark DataFrame
data_path = "US_Crime_Rates_1960_2014.csv"  # Replace with the actual path
US_Crime_Rates_1960_2014_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = "titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)

#create temp table for sql
US_Crime_Rates_1960_2014_df.createOrReplaceTempView('us_crime')

#COUNT 

#pyspark

#counts the number of records in US_Crime_Rates_1960_2014 
count=US_Crime_Rates_1960_2014_df.count()
print("Number of Count:", count)


#sql

count = spark.sql("""
                      select count(*) as number_of_count
                      from us_crime
                     """)

# count_sql.show()
print("Number of Count:", count.collect()[0][0])




#DISTINCT COUNT

#pyspark

#counts distinct years
distinct_count=US_Crime_Rates_1960_2014_df.select(countDistinct("Year"))
print("Number of distinct years:",distinct_count.collect()[0][0])


#sql

distinct_count = spark.sql("""
                        select count(distinct 'year') as distinct_count_of_years
                       from us_crime
                         """)   
# distinct_count.show()
print("Number of distinct years:",distinct_count.collect()[0][0])

#the output is wrong



#APPROXIMATE DISTINCT COUNT
#pyspark

#the approximate number of distinct values in the "Total" column 
US_Crime_Rates_1960_2014_df.select(approx_count_distinct("Total", 0.1)).show()


#sql
#the approximate number of distinct values in the "Total" column 

#returns a df
total_count = spark.sql(""" 
                        select approx_count_distinct(Total,0.1)
                        from us_crime

                        """) 
total_count.show()


#pyspark

#got first and last year from us_crime_rates_1960_2014 using first and last functions
first_year_df=US_Crime_Rates_1960_2014_df.select(first("Year"))
last_year_df=US_Crime_Rates_1960_2014_df.select(last("Year"))

print("First year:",first_year_df.collect()[0][0])
print("Last year:",last_year_df.collect()[0][0])





#FIRST AND LAST

#pyspark

#got first and last year from us_crime_rates_1960_2014 using first and last functions
first_year_df=US_Crime_Rates_1960_2014_df.select(first("Year"))
last_year_df=US_Crime_Rates_1960_2014_df.select(last("Year"))

print("First year:",first_year_df.collect()[0][0])
print("Last year:",last_year_df.collect()[0][0])

#sql

#got first and last year from us_crime_rates_1960_2014 using first and last functions

first_last = spark.sql("""
                        select first(Year), last(Year)
                       from us_crime
                        """)
# first_last.show()


#print result
print("First year:",first_last.collect()[0][0])
print("Last year:",first_last.collect()[0][1])


#MAX AND MIN

#pyspark

#got min and max population from us_crime_rates_1960_2014 using first and last functions

min_df=US_Crime_Rates_1960_2014_df.select(min("population"))
max_df=US_Crime_Rates_1960_2014_df.select(max("population"))
print("Minimum population::",min_df.collect()[0][0])
print("Maximum population: ",max_df.collect()[0][0])


#sql

min_max = spark.sql("""
                     select min(population), max(population)
                    from us_crime            
                     """)
# max_min.show()
print("Minimum population::",min_max.collect()[0][0])
print("Maximum population: ",min_max.collect()[0][1])



#SUM DISTINCT

#pyspark

#used sumDistinct function to calculated sum of distinct propery grouped by year
US_Crime_Rates_1960_2014_df.groupBy("Year").agg(sumDistinct(col("Property")).alias("SumDistinctProperty")).orderBy(col('year')).show()


#sql

sum_distinct_property= spark.sql("""
                                    select Year, sum(Property)
                                    from us_crime
                                    group by Year
                                    order by Year
                                         """)
sum_distinct_property.show()


#AVERAGE

#pyspark

#calculate avg murder rate
Avg_murder=US_Crime_Rates_1960_2014_df.select(avg("Murder"))
print("Average murder rate:",Avg_murder.collect()[0][0])


#sql

Avg_murder = spark.sql("""
                            select avg(murder) as average
                           from us_crime
                            """)
# Avg_murder.show()

print("Average murder rate:",Avg_murder.collect()[0][0])



#### Aggregating to Complex Types


#pyspark

# Calculate the total sum of "Violent" and "Property" crimes for each year
#Created struct for two columns and named it CrimeTotals
result = US_Crime_Rates_1960_2014_df.groupBy("Year").agg(
    struct(
        sum(col("Violent")).alias("TotalViolent"),
        sum(col("Property")).alias("TotalProperty")
    ).alias("CrimeTotals")
).orderBy("Year")


result.show()


#sql

result = spark.sql("""
    select Year,
           STRUCT(SUM(Violent), SUM(Property)) AS TotalCrimes
    from us_crime
    group by  Year
    order by Year
""")
                               
result.show()

# GROUPING

#pyspark

#sum of different columns in each row
total_crime_sum = US_Crime_Rates_1960_2014_df.withColumn("TotalCrimeSum",
                                                    col('violent') + col('property') + col('murder') + col('forcible_rape') + 
                                                    col('robbery') + col('Aggravated_assault') + col('Burglary')+ col('Larceny_Theft')
                                                    + col('Vehicle_Theft'))

#avaerage of calculated sum
avegare_crime = total_crime_sum.agg(avg('TotalCrimeSum'))
print("Average of all crime:", avegare_crime.collect()[0][0])
total_crime_sum.select('year','TotalCrimeSum').show()


#sql

total_crime_sum = spark.sql(""" 
                      SELECT *,
                            (Violent + Property + Murder + Forcible_rape + Robbery +
                            Aggravated_assault + Burglary + Larceny_Theft + Vehicle_Theft) AS TotalCrimeSum
                     FROM us_crime
                    """)

# total_crime_sum.show()

average_crime = total_crime_sum.agg(avg('TotalCrimeSum'))
print("Average of all crime:", avegare_crime.collect()[0][0])
total_crime_sum.select('year','TotalCrimeSum').show()


#WINDOW

#PYSPARK

# Define a window specification
window_spec = Window.partitionBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate the cumulative sum of "Property" values over the years
US_Crime_Rates_1960_2014_df_window = US_Crime_Rates_1960_2014_df.withColumn(
    "CumulativePropertySum", sum(col("Property")).over(window_spec)
)

US_Crime_Rates_1960_2014_df_window.show()


#sql

sql = spark.sql("""
                    select *,
                    sum(property) OVER (order by Year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulativePropertySum
                    from us_crime
                        """)

sql.show()

#PIVOT 

#pyspark

#piovot df created to show value of robbery in each year
pivoted = US_Crime_Rates_1960_2014_df.groupBy("Year").pivot("Year").sum("robbery").sort(asc('Year'))
pivoted.show()