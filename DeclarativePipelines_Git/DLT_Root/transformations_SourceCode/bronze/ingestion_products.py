import dlt

# Customers Expectations
products_rules = {
    "rule_1" : "product_id IS NOT NULL",
    "rule_2" : "price >= 0"
}

#Ingesting Products
@dlt.table(
    name = "products_stg"
)

@dlt.expect_all(products_rules)

def products_stg():
    df = spark.readStream.table("dlttej.source.products")
    return df