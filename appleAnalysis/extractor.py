# Databricks notebook source
# MAGIC %run "./reader_factory"
# MAGIC

# COMMAND ----------

class Extractor:
    def __init__(self):
        pass 
    def extract(self):
        pass 

class AirpodsAfterIphone(Extractor):
    def extract(self):
        transactionInputDF=get_data_source(data_type='csv',file_path="dbfs:/FileStore/input_datasets/Transaction_Updated.csv").get_data_frame()
        customersInputDF=get_data_source(data_type='delta',file_path="default.customers").get_data_frame()
        InputDFs= {
            "transactionInputDF" : transactionInputDF,
            "customersInputDF" : customersInputDF
        }
        return InputDFs

# COMMAND ----------

