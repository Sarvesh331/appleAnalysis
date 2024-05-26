# Databricks notebook source


# COMMAND ----------

# MAGIC %run "./transform"
# MAGIC

# COMMAND ----------

# MAGIC %run "./loader"
# MAGIC

# COMMAND ----------

# MAGIC %run "./extractor"
# MAGIC

# COMMAND ----------

class AirpodsAfterIphoneWorkFlow:
    def __init__(self):
        pass 
    def runner(self):
        
        # cusotomers who have bought airpods after buying iphone
        inputDFs=AirpodsAfterIphone().extract()
        firstTransformed=FirstTransformer().transform(inputDFs)
        #firstTransformer.show()
        firstTransformed.show()
        AirPodsAfterIphoneLoader(firstTransformed).sink()



class IphoneAndAirpodsOnly:
    def __init__(self):
        pass
    def runner(self):
        inputDFs=AirpodsAfterIphone().extract()
        secondTransformed=SecondTransformer().transform(inputDFs)
        #firstTransformer.show()
        secondTransformed.show()
        secondTransformed.write.format('delta').mode('overwrite').saveAsTable("IphoneAndAirpodsOnly_Table")
        IphoneAndAirpodsOnlyLoader(secondTransformed).sink()


class WorkFlowRunner:
    def __init__(self,name):
        self.name=name 
    def runner(self):
        if self.name=='AirpodsAfterIphoneWorkFlow':
            return AirpodsAfterIphoneWorkFlow().runner()
        elif self.name=='IphoneAndAirpodsOnly':
            return IphoneAndAirpodsOnly().runner()
        else:
            return "not implemented"


name='IphoneAndAirpodsOnly'
workflow=WorkFlowRunner(name)
workflow.runner()
    


# COMMAND ----------




# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('firstProject.me').getOrCreate()
input_df=spark.read.format('parquet').option("header",True).load('dbfs:/FileStore/output_datasets/IphoneAndAirpodsOnly/*')
input_df.show()

# COMMAND ----------

input_df.count()


# COMMAND ----------

#dbutils.fs.mkdirs('dbfs:/FileStore/output_datasets/IphoneAndAirpodsOnly')

# COMMAND ----------

