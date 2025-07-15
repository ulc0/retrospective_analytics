import pyspark
import pyspark.sql.functions as F
import itertools as it
import numpy as np

def bart_expanded(time_arr):
#def pymc_bart_censored(event,time,covcols,bartdf):

    ### refactored reorder of columns to _data node
    #TODO reorder columns so time is first, event is last
 ### Create an array of unique time values and sort   
    time_arr.sort()
#TODO log to mlflow
###print(time_arr) should be a log 
# full cartesian product, 1:end x 0:(end-1), time x time
    txref=it.product(time_arr[1:],time_arr[:-1])
# iterable, but only when first element is greater than second
    lxref=[ t for t in txref if t[0]>t[1] ]
###print(lxref) Should be a log
# data frame of ntimes, which will be joined to main set
   # move to call xref=spark.createDataFrame(lxref,[time,'n'+time])
###xref.show() should be a log
# merge drop time, rename ntime to time, sent event to False
 # move to call   censorset=xref.join(bartdf,[time],how='inner').drop(*(time,event)).withColumnRenamed('n'+time,time).withColumn(event,F.lit(0)) #.cast('boolean'))
# so union doesn't like the column ordering
  ###  training=bartdf.union(censorset)
###print(scols)
    return lxref ###censorset ##training
