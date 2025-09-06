# import dlt
# #Create Streaming Table
# @dlt.table(
#     name = "first_stream_table"
# )
# def first_stream_table():
#     df = spark.readStream.table("dlttej.source.orders")
#     return df

# #Create Materialized View
# @dlt.table(
#     name = "first_mat_view"
# )
# def first_mat_view():
#     df = spark.read.table("dlttej.source.orders")
#     return df


# #Create Batch View
# @dlt.view(
#     name = "first_batch_view"
# )
# def first_batch_view():
#     df = spark.read.table("dlttej.source.orders")
#     return df

# #Create Streaming View
# @dlt.view(
#     name = "first_stream_view"
# )
# def first_stream_view():
#     df = spark.readStream.table("dlttej.source.orders")
#     return df

