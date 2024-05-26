# Databricks notebook source
# MAGIC %run "./loader_factory"
# MAGIC

# COMMAND ----------

class Loader:
    def __init__(self,firstTransformed):
        self.df=firstTransformed 
    def sink(self):
        pass 

class AirPodsAfterIphoneLoader(Loader):
    def sink(self):
        params={}
        get_sink_source('dbfs',self.df,'dbfs:/FileStore/output_datasets/IpodsAfterIphone','overwrite',params).load_data_frame()

class IphoneAndAirpodsOnlyLoader(Loader):
    def sink(self):
        params={}
        get_sink_source('dbfs',self.df,'dbfs:/FileStore/output_datasets/IphoneAndAirpodsOnly','overwrite',params).load_data_frame()