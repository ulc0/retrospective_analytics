def pymc_bart_censored(event,time,covcols,tbl):
    import pyspark
    import pyspark.sql.functions as F
    import itertools as it
    ### refactored reorder of columns to _data node
    #TODO reorder columns so time is first, event is last
#event="event"
#time="days"
# this is where we put code to use other time periods
#covcols=['inst', 'meal_cal', 'ph_ecog', 'age', 'pat_karno', 'female', 'ph_karno', 'wt_loss']
 #   lcols=list([time]+covcols+[event])        
 #   print(lcols)    
 #   scols=','.join(lcols) #f"{event},{time},{covcols}"
 #   bartdf=spark.sql(f"select {scols}  from {tbl} order by {scols}")
 #   bartdf.show()
    bart_df=spark.table(tbl)
 ### Create an array of unique time values and sort   
    time_arr = bartdf.select(F.collect_set(time)).first()[0]
    time_arr.sort()
###print(time_arr) should be a log 

    sarray=time_arr
###print(sarray)
###print(sarray[1:])
###print(sarray[:-1])
# full cartesian product, 1:end x 0:(end-1), time x time
    txref=it.product(sarray[1:],sarray[:-1])
# iterable, but only when first element is greater than second
    lxref=[ t for t in txref if t[0]>t[1] ]
###print(lxref) Should be a log
# data frame of ntimes, which will be joined to main set
    xref=spark.createDataFrame(lxref,[time,'n'+time])
###xref.show() should be a log
# merge drop time, rename ntime to time, sent event to False
    censorset=xref.join(bartdf,[time],how='inner').drop(*(time,event)).withColumnRenamed('n'+time,time).withColumn(event,F.lit(0)) #.cast('boolean'))
# so union doesn't like the column ordering
  ###  training=bartdf.union(censorset)
###print(scols)

    return censorset ##training

if __name__ == "__main__":
    import doctest
#    doctest.testmod()
