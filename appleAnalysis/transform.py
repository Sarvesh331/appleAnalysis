# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col,broadcast,collect_list,collect_set

# COMMAND ----------

class Transformer:
    def __init__(self):
        pass 
    def transform(self,inputDFs):
        pass 

class FirstTransformer(Transformer):
    def transform(self,inputDFs):
         # cusotomers who have bought airpods after buying iphone
         transactionInputDF=inputDFs["transactionInputDF"]
         customersInputDF=inputDFs['customersInputDF']
         customersInputDF=customersInputDF.withColumnRenamed('customer_id','customer_id_updated')
         mywindow=Window.partitionBy('customer_id').orderBy('transaction_date')
         """
         transactionInputDF.show()
         transactionInputDF.createOrReplaceTempView('transactions')
         spark.sql('select *,lead(product_name) over(partition by customer_id order by transaction_date) as next_product from transactions').createOrReplaceTempView('transactions')
         spark.sql("select * from transactions where product_name='iPhone' and next_product='AirPods'").show()
         """
         transformed_df=transactionInputDF.withColumn('next_product',lead('product_name').over(mywindow))
         transactions_df=transformed_df.filter((transformed_df.product_name=='iPhone') & (transformed_df.next_product=='AirPods'))
         joined_df=transactions_df.join(broadcast(customersInputDF),transactions_df.customer_id==customersInputDF.customer_id_updated)
         return joined_df
     
class SecondTransformer(Transformer):
    def transform(self,inputDFs):
         # cusotomers who have bought airpods after buying iphone
         transactionInputDF=inputDFs["transactionInputDF"]
         customersInputDF=inputDFs['customersInputDF']
         customersInputDF=customersInputDF.withColumnRenamed('customer_id','customer_id_updated')
         """
         mywindow=Window.partitionBy('customer_id').orderBy('transaction_date')
         transactionInputDF.show()
         transactionInputDF.createOrReplaceTempView('transactions')
         spark.sql('select *,lead(product_name) over(partition by customer_id order by transaction_date) as next_product from transactions').createOrReplaceTempView('transactions')
         spark.sql("select * from transactions where product_name='iPhone' and next_product='AirPods'").show()
         
         transformed_df=transactionInputDF.withColumn('next_product',lead('product_name').over(mywindow))
         """
         transactions_df=transactionInputDF.filter((transactionInputDF.product_name=='iPhone') | (transactionInputDF.product_name=='AirPods'))
         transactions_df=transactions_df.groupBy('customer_id').agg(collect_set('product_name').alias('products'))
         joined_df=transactions_df.join(broadcast(customersInputDF),transactions_df.customer_id==customersInputDF.customer_id_updated)
         return joined_df.orderBy('customer_id')
     





# COMMAND ----------

