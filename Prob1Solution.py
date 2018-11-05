pyspark --master yarn --conf spark.ui.port=12890 --num-executors 2 --executor-memory 512M

orders = sc.textFile("/data/data-master/retail_db/orders")
for i in orders.take(20): print i

orderItems = sc.textFile("/data/data-master/retail_db/order_items")
for i in orderItems.take(20): print i

products = sc.textFile("/data/data-master/retail_db/products")
for i in products.take(20): print i

productsMap = products.map(lambda x: (int(x.split(',')[0]), x.split(',')[2]))
for i in productsMap.take(20): print i


# Get distinct statuses
for i in orders.map(lambda x:
  x.split(',')[3]
).distinct().take(20): print i

# Filter orders RDD
ordersFilter = orders.filter(lambda x:
    x.split(',')[3] in ['COMPLETE', 'CLOSED']
)
for i in ordersFilter.take(20): print i

ordersMap = ordersFilter.map(lambda x: ( int(x.split(',')[0]), x.split(',')[1].split(' ')[0]))
for i in ordersMap.take(20): print i

orderItemsMap = orderItems.map(lambda x: ( int(x.split(',')[1]), x))
for i in orderItemsMap.take(20): print i

orderItemsJoin = ordersMap.join(orderItemsMap)
for i in orderItemsJoin.take(20): print i

orderProducts = orderItemsJoin.map(lambda x:
    ((int(x[1][1].split(',')[2]), x[1][0]), round(float(x[1][1].split(',')[4]),1) )
)
for i in orderProducts.take(20): print i

ordersReduced = orderProducts.reduceByKey(lambda x, y: x + y)
for i in ordersReduced.take(20): print i

ordersReducedMap = ordersReduced.map(lambda x: (x[0][0], x))
for i in ordersReducedMap.take(20): print i

productsJoin = ordersReducedMap.join(productsMap)
for i in productsJoin.take(20): print i

productsMap2 = productsJoin.map(lambda x:
   ((x[1][0][0][1], -x[1][0][1]), (x[1][0][0][1], x[1][0][1], x[1][1]))
).sortByKey()

for i in productsMap2.take(20): print i

finalList = productsMap2.map(lambda x:
    (str(x[1][0]), x[1][1], str(x[1][2]))
)
for i in finalList.take(20): print i

finalList.saveAsTextFile("/user/jibran/daily_revenue_python")
finalList.toDF()


order_date, daily_revenue, product_name
