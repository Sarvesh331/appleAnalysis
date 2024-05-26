# Databricks notebook source
class DataSource:
    """
    Abstract class
    """
    def __init__(self,path):
        self.path=path 

    def get_data_frame(self):
        """
        Abstract method, function will be defined in sub classes
        """
        raise ValueError("Not Implemented")

class CSVDataSource(DataSource):
    def get_data_frame(self):
         df=spark.read.format('csv').option("header",True).load(self.path)
         return df 
     
class ParquetDataSource(DataSource):
    def get_data_frame(self):
         df=spark.read.format('parquet').load(self.path)
         return df 
     
class DeltaDataSource(DataSource):
    def get_data_frame(self):
         df=spark.read.table(self.path) 
         return df 
     
def get_data_source(data_type,file_path):
    if data_type=='csv':
        source=CSVDataSource(file_path)
        return source
    elif data_type=='parquet':
        source=ParquetDataSource(file_path)
        return source
    elif data_type=='delta':
        source=DeltaDataSource(file_path)
        return source
    else:
        raise valueError(f"unsupported file format or not yet implemented {data_type}")


# COMMAND ----------


