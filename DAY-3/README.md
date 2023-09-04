# PySpark Data Processing Examples

This repository contains a collection of PySpark code examples demonstrating various data processing tasks using the PySpark library.

## Prerequisites

To run the code in this repository, you need the following:

- Apache Spark (PySpark)
- Python 3

## Code Overview

1. **Data Loading**
    - Load the Titanic, Chipotle, and Kalimati Tarkari datasets into Spark DataFrames.

2. **Data Type Conversion**
    - Convert the "fare" column in the Titanic dataset from double to integer.

3. **Working with Booleans**
    - Create a new column "IsAdult" in the Titanic dataset to indicate if a passenger is an adult based on age.

4. **Working with Numbers**
    - Calculate the average age of male and female passengers in the Titanic dataset.

5. **Working with Strings**
    - Filter items containing the word "Chicken" in the Chipotle dataset.

6. **Regular Expressions**
    - Filter items with names starting with "Ch" in the Chipotle dataset.

7. **Working with Null Values**
    - Count passengers with missing age information in the Titanic dataset.

8. **Coalesce**
    - Combine columns "item_name" and "choice_description" in the Chipotle dataset using coalesce.

9. **Handling Nulls with ifnull, nullIf, nvl, and nvl2**
    - Replace null values in the "age" column of the Titanic dataset with the average age using various functions.

10. **Drop**
    - Remove the "Cabin" column from the Titanic dataset.

11. **Fill**
    - Fill null values in the "age" column of the Titanic dataset with a default age of 30.

12. **Replace**
    - Replace "male" with "M" and "female" with "F" in the "Sex" column of the Titanic dataset using regexp_replace.

13. **Working with Complex Types: Structs**
    - Create a new DataFrame with a struct column "PriceRange" containing minimum and maximum prices for commodities in the Kalimati Tarkari dataset.

14. **Working with Complex Types: Arrays**
    - Create a new DataFrame with an array column "CommodityList" containing all commodities in the Kalimati Tarkari dataset.

15. **Working with Complex Types: explode**
    - Explode the "CommodityList" array in the DataFrame.

16. **Working with Complex Types: Maps**
    - Create a new DataFrame with a map column "PriceMap" containing commodity prices in the Kalimati Tarkari dataset.

17. **Working with JSON**
    - Convert a portion of the Kalimati Tarkari dataset to JSON format and save it to a JSON file.

## Running the Code

- Install Apache Spark and set up the environment.
- Clone this repository to your local machine.
- Download the required datasets and update file paths in the code.
- Use `spark-submit` to run the desired Python scripts for each task.
