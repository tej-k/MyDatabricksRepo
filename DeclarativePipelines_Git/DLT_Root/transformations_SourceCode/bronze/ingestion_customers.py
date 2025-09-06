import dlt

# Customers Expectations
customers_rules = {
    "rule_1" : "customer_id IS NOT NULL",
    "rule_2" : "customer_name IS NOT NULL"
}

#Ingesting Customers
@dlt.table(
    name = "customers_stg"
)

@dlt.expect_all_or_drop(customers_rules)

def customers_stg():
    df = spark.readStream.table("dlttej.source.customers")
    return df