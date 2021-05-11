import psycopg2
from faker import Faker
fake = Faker()

# Update connection string information
host = "edit me"
dbname = "edit me"
user = "edit me"
password = "edit me"

# Construct connection string
conn_string = "host={0} user={1} dbname={2} password={3}".format(host, user, dbname, password)
conn = psycopg2.connect(conn_string)
print("Connection established")

cursor = conn.cursor()


cursor.execute("TRUNCATE TABLE sales.fact_internet_sales RESTART IDENTITY;")
print("Truncate table..")

insert_count = 1

while insert_count <= 500000:
    print("Count: ", insert_count)
    insert_count += 1
    cursor.execute("INSERT INTO sales.fact_internet_sales (ProductKey, CustomerKey, PromotionKey, CurrencyKey, SalesTerritoryKey, SalesOrderNumber, SalesOrderLineNumber, RevisionNumber, OrderQuantity, UnitPrice, ExtendedAmount, UnitPriceDiscountPct, DiscountAmount, ProductStandardCost, TotalProductCost, SalesAmount, TaxAmt, Freight, CarrierTrackingNumber, CustomerPONumber, OrderDate, DueDate, ShipDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                                          (fake.random_int(min=5000, max=6000), #ProductKey
                                                          fake.random_int(min=1000, max=3000), #CustomerKey
                                                          fake.random_int(min=400, max=500), #PromotionKey
                                                          fake.random_int(min=1, max=5), #CurrencyKey
                                                          fake.random_int(min=1, max=100), #SalesTerritoryKey
                                                          fake.ssn(), #SalesOrderNumber,
                                                          fake.random_int(min=1, max=10), #SalesOrderLineNumber
                                                          fake.random_int(min=1, max=3), #RevisionNumber
                                                          fake.random_int(min=1, max=10), #OrderQuantity
                                                          fake.random_number(), #UnitPrice
                                                          fake.random_number(), #ExtendedAmount
                                                          fake.random_number(), #UnitPriceDiscountPct
                                                          fake.random_number(), #DiscountAmount
                                                          fake.random_number(), #ProductStandardCost
                                                          fake.random_number(), #TotalProductCost
                                                          fake.random_number(), #SalesAmount
                                                          fake.random_number(), #TaxAmt
                                                          fake.random_number(), #Freight
                                                          fake.ssn(), #CarrierTrackingNumber
                                                          fake.ssn(), #CustomerPONumber
                                                          fake.date_time(), #OrderDate
                                                          fake.date_time(), #DueDate
                                                          fake.date_time(), #ShipDate 
                                                          ))

                                                           
print("Inserted of data")
# Insert some data into the table


# Clean up
conn.commit()
cursor.close()
conn.close()