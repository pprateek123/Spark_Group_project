# %%
#Importing necessary libraries, initiating and starting the spark session 

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import   avg, rank, concat
from pyspark.sql.types import  FloatType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

Initiation of Spark session

spark = SparkSession.builder.appName("International_Trade").getOrCreate()

#revised_data
data_path = "/Users/anujkhadka/Fusemachines47/ALL SPARK/Spark_Group_project/data/revised_final_data1.csv"  
revised_final = spark.read.csv(data_path, header=True, inferSchema=True)

#goods_classifications
data_path = "/Users/anujkhadka/Fusemachines47/ALL SPARK/Spark_Group_project/data/goods_classification.csv"  
goods_final = spark.read.csv(data_path, header=True, inferSchema=True)

#country_classification
data_path = "/Users/anujkhadka/Fusemachines47/ALL SPARK/Spark_Group_project/data/country_classification.csv"  
country_final = spark.read.csv(data_path, header=True, inferSchema=True)

#services_classification
data_path = "/Users/anujkhadka/Fusemachines47/ALL SPARK/Spark_Group_project/data/services_classification.csv"  
services_final = spark.read.csv(data_path, header=True, inferSchema=True)



#1. Generate a pivot table that shows the total import values of Services for the top 4 importing countries in June 2023, with  "Transportation" service categories as columns.
from pyspark.sql.functions import col, sum

#Using filter for the revised dataset for importing

import_df = revised_df.filter((col("account") == "Imports") & (col("time_ref") == "2023-06-30"))

# Joining the dataframe "import_df" & "service_df" on the code(key) column

joined_df = import_df.join(services_final, import_df["code"] == services_final["code"])


# Using 'Group by' for country_code and service_label

grouped_df = joined_df.groupBy("country_code", "service_label").sum("value"))

 # ranking of countries 
 
ranked_df = grouped_df.orderBy(col("sum(value)").desc())
#Top 4 importing countries extraction

top_countries = ranked_df.select("country_code").limit(4)

# creating pivot table

pivot_table = ranked_df \
    .join(top_countries, ["country_code"], "inner") \
    .groupBy("country_code").pivot("service_label", ["Services", "Transportation"]) \
    .sum("sum(value)")
    
    

pivot_table.show()

#2. Analyze the total import value of "Services" for each country in June 2023-06-30, and ranking the countries in descending order of import value.

# Data is stored in 'revised_df' DataFrame 

import_df = revised_final

#'Services' column & import in 2023-06-30 is filtered

import_df = import_df.filter((col("account") == "Imports") & (col("time_ref") == "2023-06-30") & (col("product_type") == "Services"))

# groupBy 'country_code' 
 
import_df = import_df.groupBy("country_code").agg(sum("value").alias("Total Import Value"))

# descending order  ranking

import_df = import_df.orderBy(col("Total Import Value").desc())

# Columns renaming
import_df = import_df.withColumnRenamed("country_code", "Country Code")

import_df.show()

3. Quantify the supreme 20 countries with the highest average export value of "Goods" for each month in the updated time_ref & use window functions to calculate the average and rank the countries accordingly

# Dataframe filtering

df= df.filter(df['country_code'] != 'TOT (OMT CIF)')

df = df.filter(df['country_code'] != 'TOT (OMT VFD)')

# updating the time_ref 

filtered_df = df.filter((col("product_type") == "Goods") & (col("time_ref") == "2023-06-30"))



# Window specification 

window_spec = Window.partitionBy("time_ref").orderBy(col("avg_export_value").desc())
# average export value 

avg_export_df = (
    filtered_df
    .groupBy("time_ref", "country_c5. Compute the total export value for each product type i.e 'Goods & Services' in the "revised_csv" file for the June 2023 quarter using a UDF.ode")
    .agg(avg(col("value")).alias("avg_export_value"))
    .withColumn("rank", rank().over(window_spec))
)

# top 3 countries 

top3_countries_by_month = avg_export_df.filter(col("rank") <= 20)

top3_countries_by_month.show()

#4. Computing the total import value for each country in the 'revised_final' table & presenting the peak 10 countries with the highest total export values.



from pyspark.sql.functions import sum, desc
# total import value 

total_imports = import_data.groupBy("country_code").agg(sum("value").alias("total_import_value"))

# Defining a UDF 

def calculate_total_export_value(values):
    total = 0.0
    
    # Iteration
    
    for val in values:
        try:
            total += float(val)
        except (ValueError, TypeError):
            pass  
    
    return total

# Registering the UDF

calculate_total_export_udf = udf(calculate_total_export_value, DoubleType())

# total export value

result_df = df.filter(df.time_ref == "2023-06-30") \
    .groupBy("product_type") \
    .agg(calculate_total_export_udf(F.collect_list("value")).alias("total_export_value"))


result_df = result_df.withColumn("total_export_value", F.format_number("total_export_value", 1))


# Filtering

time_ref = "2023-06-30"
revised_df_filtered = revised_df.filter(col("time_ref") == time_ref)
df_joined = services_final.join(revised_df_filtered, revised_df_filtered['code']==services_final['code'])

df_imp = df_joined.where(col('account')=='Imports')
df_exp  = df_joined.where(col('account')=='Exports')
df_exp = df_exp.withColumnRenamed('value',"value_exp")
df_joined = df_imp.join(df_exp,df_imp['country_code']==df_exp['country_code'])


df_joined.show()