# Databricks notebook source
class DataSink:
    def __init__(self,df,path,method,params):
        self.df=df
        self.path=path
        self.method=method
        self.params=params

    def load_data_frame(self):
        pass

class  SinkToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.format('parquet').mode(self.method).save(self.path)

class SinkToDBFSWithPartition(DataSink):
    def load_data_frame(self):
        partitionCols=params.get('partitionByColumns')
        self.df.write.format('csv').mode(self.method.partitionBy(*partitionCols)).save(self.path)
    


def get_sink_source(sink_type,df,path,method,params):
    if sink_type=='dbfs':
        return SinkToDBFS(df,path,method,params)
    elif sink_type=='dbfs_with_partition':
        return SinkToDBFSWithPartition(df,path,method,params)
    else:
        return ValueError('Not implemented')


    