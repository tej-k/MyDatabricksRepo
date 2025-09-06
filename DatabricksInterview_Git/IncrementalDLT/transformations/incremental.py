import dlt

#Adding Expectations
rules = {
    "rule1" : "OrderID IS NOT NULL",
    "rule2" : "Quantity > 0",
    "rule3" : "Price >= 0"
}

@dlt.table(
    name = "inc_table"
)

@dlt.expect_all_or_drop(rules)
def inc_table():

    df = spark.readStream.format("cloudFiles")\
         .option("cloudFiles.format", "csv")\
         .load("/Volumes/dbinterview/source/source_orders")
    
    return df