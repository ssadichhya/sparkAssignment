from pyspark.sql import SparkSession
from pyspark.sql.functions import struct
from pyspark.sql.types import StructField,StructType,StringType,IntegerType



# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()

# Creating the data for employees_df
employee_data = [
    (1, "Pallavi mam", 101),
    (2, "Bob", 102),
    (3, "Cathy", 101),
    (4, "David", 103),
    (5, "Amrit Sir", 104),
    (6, "Alice", None),
    (7, "Eva", None),
    (8, "Frank", 110),
    (9, "Grace", 109),
    (10, "Henry", None)
]

# Defining the schema for employees_df
employee_schema = StructType([
    StructField("Employee_Id",IntegerType(),True),
    StructField("Employee_name",StringType(),True),
    StructField("department_id",IntegerType(),True)
])

# Creating the employees DataFrame using struct
employees_df = spark.createDataFrame(employee_data, employee_schema)

# Creating the data for departments_df
department_data = [
    (101, "HR"),
    (102, "Engineering"),
    (103, "Finance"),
    (104, "Marketing"),
    (105, "Operations"),
    (106, None),
    (107, "Operations"),
    (108, "Production"),
    (None, "Finance"),
    (110, "Research and Development")
]

# Creating the departments DataFrame using struct
department_schema = StructType([
    StructField("department_id",IntegerType(),True),
    StructField("department_name",StringType(),True)
])

department_df = spark.createDataFrame(data=department_data, schema=department_schema)


print("Employees DataFrame:")
employees_df.show()

print("Departments DataFrame:")
department_df.show()


#CREATE TEMP TAVLE FOR EMPLOYEE AND DEPARTMENT
department_df.createOrReplaceTempView("department")
employees_df.createOrReplaceTempView("employee")


#pyspark

joinExpression = department_df['department_id'] == employees_df['department_id']
department_df.join(employees_df, joinExpression).select(department_df['department_id'],employees_df['employee_id'],employees_df['employee_name'],department_df['department_name']).show()

#sql

join_sql = spark.sql("""
                    select d.department_id, e.employee_id, e.employee_name, d.department_name
                    from department d
                    join employee e
                    on d.department_id = e.department_id
""")
                     
join_sql.show()




#pyspark

#inner join expression
joinExpression = department_df['department_id'] == employees_df['department_id']
#filtering engineering department using where
department_df.join(employees_df, joinExpression).select(employees_df['employee_name'],department_df['department_name']).where(department_df['department_name']=='Engineering').show()


#sql

inner_join_sql = spark.sql("""
                           select e.employee_name, d.department_name
                           from department d
                           join employee e
                           on e.department_id = d.department_id
                           where d.department_name = 'Engineering' 
 """)
inner_join_sql.show()



#pyspark

#outer join expression
joinTYpe='outer'
Outer_join_df=department_df.join(employees_df, joinExpression, joinTYpe).select(employees_df['employee_name'],department_df['department_name'])

#fill null values with "No department"

Outer_join_df.na.fill("No department").show()


#sql

outer_join_sql = spark.sql("""
                            select coalesce(e.employee_name, 'no Employee') as employee_name, 
                                   coalesce(d.department_name, 'no department') as department_name
                           from employee e
                           full outer join department d
                           on d.department_id = e.department_id
""")
outer_join_sql.show()




#pyspark

joinType="left_outer"
#left joining two dfs and replacing na with "No department"
employees_df.join(department_df,joinExpression,joinType).select(employees_df['employee_name'],department_df['department_name']).na.fill('No Department').show()


#SQL

left_outer_join_sql = spark.sql("""
                                    select e.employee_name,coalesce(d.department_name, 'No Department') as department_name
                                    from employee e
                                    left outer join department d
                                    on e.department_id = d.department_id       

                                """)
left_outer_join_sql.show()



#pyspark

joinType="right_outer"
#right joining two dfs and replacing na with "No Employees"
employees_df.join(department_df,joinExpression,joinType).select(department_df['department_name'],employees_df['employee_name']).na.fill('No Employees').show()


#sql

right_outer_join_sql = spark.sql("""
                                select d.department_name as department_name,
                                       coalesce(e.employee_name,'No Employees') as employee_name
                                 from employee e
                                 right outer join department d
                                 on e.department_id = d.department_id
                                """)

right_outer_join_sql.show()



#pyspark

joinType="left_semi"
#shows employee name who has matching records in department df
employees_df.join(department_df,joinExpression,joinType).select(employees_df['employee_name']).show()


#sql

left_semi_join_sql = spark.sql("""
                               select e.employee_name
                               from employee e
                               left semi join department d
                               on e.department_id = d.department_id  
                               """ )

left_semi_join_sql.show()




#pyspark

joinType="left_anti"
#shows employee name who doesnot have matching records in department df
employees_df.join(department_df,joinExpression,joinType).select(employees_df['employee_name']).show()


#sql

left_anti_join_sql = spark.sql("""
                               select e.employee_name
                               from employee e
                               left anti join department d
                               on e.department_id = d.department_id       
                               """ )

left_anti_join_sql.show()



#pyspark

joinType = 'cross'
#Cartesian product of two df
#Be careful while using cross joins
employees_df.crossJoin(department_df).show()


#sql

cross_join_sql = spark.sql("""
                           select * 
                           from employee
                           cross join department
                           """)
                           
cross_join_sql.show()

