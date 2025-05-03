#!/usr/bin/env python
# coding: utf-8

# ## nb_counting_coins
# 
# New notebook

# The goal of this script is to compare DAX measures to corresponding SQL queries. 
#  
# I have left some display() lines, feel free to use them to troubleshoot or walk through the code step-by-step.
# 
# Here's a helpful article on the sempy functions: https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python#functions 
# 
# Full instructions available for how to do this from your laptop: https://dataonwheels.wordpress.com/?s=Power+BI%3A+Data+Quality+Checks+Using+Python+%26+SQL
#  

# In[1]:


# Import Libraries
#%pip install semantic-link
import pandas as pd
import sempy.fabric as fabric
from sempy.fabric import FabricDataFrame
from sempy.dependencies import plot_dependency_metadata
from notebookutils import mssparkutils 
from pyspark.sql.functions import concat_ws,col,lit,coalesce
from pyspark.sql import functions as F
from datetime import date, datetime
import pytz as tz 


# In[2]:


#Set up some variables
v_dataset = "Biking Sales in Hobbiton Fabric" #can be swapped for GUIs
v_workspace = "Fabric of Middle-Earth" #can be swapped for GUIs
v_tz = tz.timezone('UTC')
v_run_datetime = datetime.now(v_tz) #grabs the current date and time in UTC


# In[3]:


# How can we run PBI DAX measures?

df = fabric.evaluate_measure(
    dataset="Biking Sales in Hobbiton Fabric", #can be swapped for GUIs
    measure="Cost", #can have multiple measures here
    workspace="Fabric of Middle-Earth")
display(df)


# In[4]:


# How can we run SQL queries against tables in our Lakehouse?
df_sql = spark.sql("SELECT * FROM lh_hobbiton_data.aw_sales LIMIT 10")
display(df_sql)
df_sql_alias = spark.sql("SELECT SUM(Order_Quantity) as order_quantity FROM lh_hobbiton_data.aw_sales")
display(df_sql_alias)


# In[5]:


# How can we easily loop through a list of measures and SQL queries to check? 

# Start by creating a dataframe that houses the values we want to check

df_checker = spark.createDataFrame(
    [  #create data here, be consistent with types
        #metric = lower(replace(pbi_measure," ","_")) = sql_query column name
        ("cost","Cost","SELECT SUM(Total_Product_Cost) AS cost FROM lh_hobbiton_data.aw_sales")
        ,("quantity_ordered","Quantity Ordered","SELECT SUM(Order_Quantity) as quantity_ordered FROM lh_hobbiton_data.aw_sales")
        ,("profit_margin","Profit Margin","SELECT SUM(Sales_Amount) - SUM(Total_Product_Cost) as profit_margin FROM lh_hobbiton_data.aw_sales")
    ], 
    ["metric","pbi_measure","sql_query"] # initial column names here (we will add some after this later in the query)
)

display(df_checker)


# In[6]:


# Now loop over the pbi_measures and sql_queries to compare the two values

# To do this, we are going to create a list of our PBI Measures using a collect function. This will allow us to loop over them.  
for row in df_checker.rdd.collect():
    #create variables to grab current record that we are looping over
    current_measure = row.pbi_measure
    current_metric = row.metric
    #execute the current pbi measure
    df_pbi = fabric.evaluate_measure(
        dataset="Biking Sales in Hobbiton Fabric", #you can use names or guids
        measure=current_measure, 
        workspace="Fabric of Middle-Earth")
    df_pbi=spark.createDataFrame(df_pbi)
    pbi_name = str('pbi_' + current_metric)
    #rename the pbi measure fields and add a column to join to the og df
    df_pbi = df_pbi.withColumnRenamed(current_measure, str(pbi_name)).drop(current_measure).withColumn("new_measure",lit(current_measure))
    display(df_pbi)
    df_checker = df_checker.join(df_pbi,df_checker.pbi_measure == df_pbi.new_measure, "leftouter").drop(df_pbi.new_measure)
    
    #now loop over the sql queries
    df_sql = spark.sql(row.sql_query)
    sql_name = str('sql_' + current_metric)
    #use current_metric instead of current_measure since the sql column name should match the metric field
    df_sql = df_sql.withColumnRenamed(current_metric,str(sql_name)).drop(current_measure).withColumn("new_query",lit(current_metric))
    display(df_sql)
    df_checker = df_checker.join(df_sql,df_checker.metric == df_sql.new_query, "leftouter").drop(df_sql.new_query)
    #display(df_checker)
display(df_checker)


# In[7]:


# now we will convert to pandas dataframe in order to coalesce some values. 
# Splitting our column sets into two new dfs, first we will coalesce the power bi values

df_pd_checker = df_checker.toPandas()
# Exclude columns with prefix 'sql_'
    # ~ is essentially a NOT function
df_pbi_clean_up = df_pd_checker.loc[:, ~df_pd_checker.columns.str.startswith('sql_')]

#coalesce columns without naming them
    #iloc[:,2:] selects all rows (:) and allows us to skip the first two columns for the coalesce 
    #bfill stands for backward fill. It will fill all NULL values backward along our chosen axis
    #axis = 1 means the operation is performed among the columns, so for each row it fills in NaN values with the next non-NaN value in the row.
    #iloc[:,0] occurs after the backward fill. It selects the first column of the new df. This will contain our coalesce
df_pbi_clean_up['pbi_result'] = df_pbi_clean_up.iloc[:, 2:].bfill(axis=1).iloc[:, 0]

#time to pick only our final columns
df_pbi_clean_up = df_pbi_clean_up[['metric','pbi_measure','pbi_result']]
#print(df_pbi_clean_up)

#back to spark df
df_pbi_clean_up = spark.createDataFrame(df_pbi_clean_up)
display(df_pbi_clean_up)

#time to clean up our sql columns
df_sql_clean_up = df_pd_checker.loc[:, ~df_pd_checker.columns.str.startswith('pbi_')]
df_sql_clean_up['sql_result'] = df_sql_clean_up.iloc[:, 2:].bfill(axis=1).iloc[:, 0]
df_sql_clean_up.rename(columns={'metric': 'sql_metric'}, inplace=True) #need to avoid creating dup columns in the final dataframe
df_sql_clean_up = df_sql_clean_up[['sql_metric','sql_query','sql_result']]
df_sql_clean_up = spark.createDataFrame(df_sql_clean_up)
display(df_sql_clean_up)


# In[15]:


#let's bring our two dataframes back together and calc the difference!

df_check = df_pbi_clean_up.join(df_sql_clean_up,df_sql_clean_up.sql_metric == df_pbi_clean_up.metric)
df_check = df_check[['metric','pbi_measure','pbi_result','sql_query','sql_result']]
#creating a column to calc the difference, but rounding a bit to avoid any issues due to rounding in the tools
df_check = df_check.withColumn('difference',F.round(df_check['pbi_result'],4) - F.round(df_check['sql_result'],4))
display(df_check)


# In[11]:


#time to write these results to our lakehouse
#let's add some metadata fields to our dataframe
df_check = df_check.withColumn('run_datetime',lit(v_run_datetime)).withColumn('semantic_model',lit(v_dataset)).withColumn('workspace',lit(v_workspace))
display(df_check)

# Write the DataFrame to a Delta table
delta_table_path = "Tables/data_quality_log"
df_check.write.option("mergeSchema", "true").format("delta").mode("append").save(delta_table_path)

