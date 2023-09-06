# %%
#Importing necessary libraries, initiating and starting the spark session 
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("solutions").getOrCreate()

# %%
#importing some frequently used functions
from pyspark.sql.functions import sum , col ,length, year,month,to_date,desc,asc,when,lit

# %%
#load datasets 

data_path = "./data/revised_final_data1.csv"  
df = spark.read.csv(data_path, header=True, inferSchema=True)

data_path = "./data/goods_classification.csv"  
df_goods = spark.read.csv(data_path, header=True, inferSchema=True)

data_path = "./data/country_classification.csv"  
df_country = spark.read.csv(data_path, header=True, inferSchema=True)
data_path = "./data/services_classification.csv"  
df_services = spark.read.csv(data_path, header=True, inferSchema=True)



# %%
df.printSchema()

# %%
#this has been done for the ease of calculation 
df = df.withColumn('value', col('value').cast('int'))

# %%
"""
<h2>Question1 : Calculate the total export value for each country in the table. Show the top 10 countries with the highest total export values.
 </h2>
"""

# %%
# here, from the main table , we calulate sum of export values for each countries.
# we then order those values on the basis of the total_export_value 
# then we show the table

result = df.where(col('account')=='Exports').groupBy('country_code').agg(sum('value').alias('total_export_value'))
result = result.join(df_country,result['country_code']==df_country['country_code'],'inner')
result = result.select('country_label',df['country_code'],'total_export_value')
result = result.orderBy(result.total_export_value.desc()).limit(10)
result.show()

# %%
"""
## Question 2 : Find the year-month and the name of the good which has been least imported. Show bottom 5 . 

"""

# %%
# firstly , we extract the month and year column from the dataframe
# we join that dataframe to the df_goods to find about goods 
# then , we see finally operate using just the imports .

df_date = df.withColumn('year', year('time_ref'))\
            .withColumn('month',month('time_ref'))
result = df_date.join(df_goods,df['code']==df_goods['Level_1'],'inner')

result = result.where(col('account') =='Imports')\
                .groupBy('year','month','Level_1_desc').agg(sum('value').alias('total_import_value'))
result = result.orderBy(asc('total_import_value'))\
               .limit(3)
result.show()


# %%
"""
## Question 3: Calculate the moving average of the total transactions quarter yearly for each year . 
"""

# %%
from pyspark.sql.window import Window
from pyspark.sql.functions import avg

df_date = df.withColumn('year', year('time_ref'))\
            .withColumn('month',month('time_ref'))
df_date = df_date.groupby('year','month').agg(avg(col('value')).alias('average_quarter_yearly'))
window_spec = Window.partitionBy().orderBy(asc('year'),asc('month')).rowsBetween(-1, 0)
result = df_date.withColumn('moving_average', avg('average_quarter_yearly').over(window_spec))
result.show()


# %%
"""
## Question 4 : Create an udf to find the trade deficit percent with each  country for  this dataset of new zealand
"""

# %%
#here, we use a udf to make a fucntion which calculates the trade deficit percentsage.
#the necessary details are figured out above


from pyspark.sql.types import FloatType,IntegerType
from pyspark.sql.functions import udf


df_imp = df.where(col('account')=='Imports')
df_imp = df_imp.groupBy('country_code').agg(sum('value').alias('Import_sum'))
df_exp  = df.where(col('account')=='Exports')
df_exp = df_exp.groupBy(col('country_code')).agg(sum('value').alias('Export_sum'))
df_joined = df_imp.join(df_exp,df_imp['country_code']==df_exp['country_code'])

df_joined = df_joined.withColumn("Trade_Deficit", df_joined["Import_sum"] - df_joined["Export_sum"])
trade_balance = df_joined.select(sum("Trade_Deficit")).collect()[0][0]

#udf
def deficit_percent(trade_deficit):
    return (trade_deficit/trade_balance)*100

#register
percent_udf = udf(deficit_percent,FloatType())



df_joined = df_joined.withColumn('Deficit_percent',percent_udf(df_joined['Trade_Deficit']))
df_joined.show(500)




# %%
"""
## Question 5: calculate the average transactions of imports and exports in a year combined in a table and show it in columns. 
"""

# %%
#here we find the average transaction of each country and use pivot to have a look at it for each year.
df_date = df.withColumn('year', year('time_ref'))
df_date = df_date.groupby('country_code').pivot('year').agg({'value':'avg'})
df_date.show(100)

# %%
"""
## Question 6 : Find out which service  each country has imported the most from new zealand. 
"""

# %%
#firstly , we take only those values which are in import account 
#we join the services table and  the main table to use the service_label column
#we group the data on the basis of countries and service labels and calculate the sum of the services 
#after this , we join this table to the country table to get the names of the country and order them on the basis of services imported.
# lastly , we select the country and the stype of service they import the most

# df.show()

df_imp = df.where(col('account')=='Imports')
# df_imp.show()
df_imp_services = df_imp.join(df_services, df_imp['code']==df_services['code'],'inner')
# df_imp_services.show()
df_imp_services = df_imp_services.groupby('country_code','service_label').agg(sum(col('value')).alias('Total_import'))
# df_imp_goods.show()

df_imp_services_country  = df_imp_services.join(df_country,df_imp_services['country_code']==df_country['country_code']).orderBy(desc('Total_import'))


df_imp_services_country.select(col('country_label').alias('Country'),col('service_label').alias('Service'))\
                    .show()

# %%
"""
## Question 7 : Find out which  good is related to animal product or not , and find out how many number of transactions(both import and export) of animal related products and non-animal related products

"""

# %%
#here, we have used just the Goods category and then joined it to the goods table 
#the descripetion of goods is then searched for the regex pattern that we have provided below 
#on the basis of that , we have classified the animal category into 1 and non-animal into 0 
#we have extracted count of each transaction for each country related to animal products. 


df_good = df.where(col('product_type')=='Goods')
df_good_desc = df_good.join(df_goods,df_good['code']==df_goods['Level_1'],'inner')
result_df = df_good_desc.withColumn("contains_animal_product",
                         when(col("Level_1_desc").rlike("(?i)(?:animal|meat|fish)"), lit(1))
                         .otherwise(lit(0)))
result_df.select('country_code','Level_1_desc','contains_animal_product').show()
result_df_country = result_df.groupBy('country_code').agg(sum('contains_animal_product').alias('Transaction_animal')).orderBy(desc('Transaction_animal'))
result_df_country.show()



