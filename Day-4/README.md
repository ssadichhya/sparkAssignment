#DAY-4
## Requirements

Before running the script, ensure that you have the following prerequisites:

- PySpark installed and configured.
- Two CSV datasets: "US_Crime_Rates_1960_2014.csv" and "titanic.csv."

## Overview

The script performs the following data analysis tasks using PySpark and SQL:

### Data Loading

1. **Data Loading:** Loads the two datasets into Spark DataFrames, "US_Crime_Rates_1960_2014_df" and "titanic_df."

### AGGREGATION

2. **Counting Records:**
   - Counts the number of records in the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

3. **Distinct Count:**
   - Calculates the distinct count of years in the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

4. **Approximate Distinct Count:**
   - Estimates the approximate distinct count of values in the "Total" column in "US_Crime_Rates_1960_2014" using PySpark and SQL.

5. **First and Last Values:**
   - Retrieves the first and last year from the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

6. **Min and Max Values:**
   - Finds the minimum and maximum population values from the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

7. **Sum Distinct Values:**
   - Calculates the sum of distinct property values grouped by year in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

8. **Average Calculation:**
   - Computes the average murder rate in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

9. **Aggregating to Complex Types:**
   - Creates a structured aggregation of total violent and property crimes for each year in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

10. **Grouping and Summation:**
    - Summarizes the total crime sum for each year by summing various crime categories in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

11. **Window Functions:**
    - Applies window functions to calculate the cumulative sum of property values over the years in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

12. **Pivoting Data:**
    - Pivots the "US_Crime_Rates_1960_2014" DataFrame to display the value of robbery for each year using PySpark.

## JOIN

### Data Loading

1. **Data Loading:** Loads the provided datasets into Spark DataFrames, namely "US_Crime_Rates_1960_2014_df" and "titanic_df."

2. **Counting Records:**
   - Counts the total number of records in the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

3. **Distinct Count:**
   - Calculates the distinct count of years in the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

4. **Approximate Distinct Count:**
   - Estimates the approximate distinct count of values in the "Total" column of "US_Crime_Rates_1960_2014" using PySpark and SQL.

5. **First and Last Values:**
   - Retrieves the first and last year from the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

6. **Min and Max Values:**
   - Finds the minimum and maximum population values from the "US_Crime_Rates_1960_2014" DataFrame using both PySpark and SQL.

7. **Sum Distinct Values:**
   - Calculates the sum of distinct property values grouped by year in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

8. **Average Calculation:**
   - Computes the average murder rate in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

9. **Aggregating to Complex Types:**
   - Creates structured aggregations for the total violent and property crimes for each year in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

10. **Grouping and Summation:**
    - Summarizes the total crime sum for each year by summing various crime categories in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

11. **Window Functions:**
    - Applies window functions to calculate the cumulative sum of property values over the years in "US_Crime_Rates_1960_2014" using both PySpark and SQL.

12. **Pivoting Data:**
    - Pivots the "US_Crime_Rates_1960_2014" DataFrame to display the value of robbery for each year using PySpark.
