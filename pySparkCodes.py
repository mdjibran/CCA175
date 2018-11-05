orders = sc.textFile("/data/data-master/retail_db/orders")
for i in orders.take(10): print i

orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderMap = orders.map(lambda i:( int(i.split(",")[0]), i.split(",")[1].split(" ")[0] ) )
orderItemMap = orderItems.map(lambda i:(int(i.split(",")[1]), float(i.split(",")[4] ) ) )
ordersJoin = orderMap.join(orderItemMap)

# GET TOTAL FROM ID
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderItemsFiltered = orderItems.filter(lambda i: int(i.split(",")[1]) == 2)
orderItemsSubTotal = orderItemsFiltered.map(lambda i: float(i.split(',')[4]))
orderItemsTotal = orderItemsSubTotal.reduce(lambda x, y: x + y)

# GET ORDER ITESM DETAILS O MINIMUM
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderItemsFiltered = orderItems.filter(lambda i: int(i.split(',')[1]) == 2)
orderItemsSubTotals = orderItemsFiltered.map(lambda i: float(i.split(',')[4]))
smallest = orderItemsSubTotals.reduce(lambda x, y: x if (x < y) else y)
--- alternate
row = orderItemsFiltered.reduce(lambda x, y: x if (float(x.split(',')[4]) < float(y.split(',')[4])) else y)

# GET COUNT BY STATUS
orders = sc.textFile("/data/data-master/retail_db/orders")
ordersStatus = orders.map(lambda i: ((i.split(',')[3]), (i.split(',')[0])))
count = ordersStatus.countByKey()

# GET TOTALS BY KEY - GROUP BY KEY
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderItemsMap = orderItems.map(lambda i: (int(i.split(',')[1]), float(i.split(',')[4])))
orderItemsGrp = orderItemsMap.groupByKey()
orderTotals = orderItemsGrp.map(lambda i: (i[0], round(sum(i[1]), 2)))

# GET TOTALS BY KEY - REDUCE BY KEY
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderItemsMap = orderItems.map(lambda i: (int(i.split(',')[1]), float(i.split(',')[4])))
orderItemsSubTotal = orderItemsMap.reduceByKey(lambda x, y: x + y)
orderItemsSubTotal = orderItemsMap.reduceByKey(lambda x, y: x if(x < y) else y)

# GET TOTAL BY KEY AND NUMBER OF ORDER ITEMS - aggregateByKey - (total, count)
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderItemsMap = orderItems.map(lambda x: (int(x.split(',')[1]), float(x.split(',')[4]) ))
orderGroupedCount = orderItemsMap.aggregateByKey((0.0, 0),
lambda x,y: (x[0]+y, x[1]+1),
lambda x,y: (x[0]+y[0], x[1]+y[1])
)

# SORT PRODUCTS LIST BY PRICE - sortbyKey
products = sc.textFile("/data/data-master/retail_db/products")
productsMap = products.map(lambda x: (float(x.split(',')[4]) if x.split(',')[4] !='' else float(x.split(',')[5] ), x.split(',')[2]))
sortedProducts = productsMap.sortByKey()

# Sort data by product category  and then by product price descending - sortByKey
products = sc.textFile("/data/data-master/retail_db/products")
productsMap = products\
.filter(lambda x: x.split(',')[4] != '')\
.map(lambda x: ((int(x.split(',')[1]), float(x.split(',')[4] )), x.split(',')[2] ))\
.sortByKey()

# To get orderby key (x,y) where x is ascending and y is descending
products = sc.textFile("/data/data-master/retail_db/products")
productsMap = products\
.filter(lambda x: x.split(',')[4] != '')\
.map(lambda x: ((int(x.split(',')[1]), -float(x.split(',')[4] )), x.split(',')[2] ))\
.sortByKey()


# Get Top records - top, takeOrdered
filteredProducts = products.filter(lambda x: x.split(',')[4] != '' )
topProducts = filteredProducts.top(5, key=lambda x: float(x.split(',')[4] ))
# above is similar as writing below statement with takeOrdered
takeOrderedProducts = filteredProducts.takeOrdered(5, key=lambda x: -float(x.split(',')[4] ))


#GEt common Products in 2013 Dec and 2014 Jan
orders = sc.textFile("/data/data-master/retail_db/orders")
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orders201312 = orders\
.filter(lambda x: x.split(',')[1][:7] == '2013-12')\
.map(lambda x: (x.split(',')[0], x))

orders201401 = orders\
.filter(lambda x: x.split(',')[1][:7] == '2014-01')\
.map(lambda x:(x.split(',')[0], x) )

orderItemsMap = orderItems.map(lambda x: (x.split(',')[1], x ) )

ordersItems2013Join = orders201312.join(orderItemsMap).map(lambda x: x[1][1].split(',')[2]).distinct()
ordersItems2014Join = orders201401.join(orderItemsMap).map(lambda x: x[1][1].split(',')[2]).distinct()
allProducts = ordersItems2013Join.union(ordersItems2014Join).distinct()

# Get products sold in both years
orders = sc.textFile("/data/data-master/retail_db/orders")
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderItemsMap = orderItems.map(lambda x: (x.split(',')[1], x))

orders201312 = orders.filter(lambda x: x.split(',')[1][:7] == '2013-12').map(lambda x: (x.split(',')[0], x))
orders201401 = orders.filter(lambda x: x.split(',')[1][:7] == '2014-01').map(lambda x: (x.split(',')[0], x))

orderItems2013 = orders201312.join(orderItemsMap)
orderItems2014 = orders201401.join(orderItemsMap)

products2013 = orderItems2013.map(lambda x: int(x[1][1].split(',')[2])).distinct()
products2014 = orderItems2014.map(lambda x: int(x[1][1].split(',')[2])).distinct()
commonProducts = products2013.intersection(products2014)
# Get Products in 2013 but not in 2014

only2013 = products2013.subtract(products2014)
only2014 = products2014.subtract(products2013)

jus1Month = only2013.union(only2014)
for i in jus1Month.take(10): print i

# Saving data to file
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
orderItemsMap = orderItems\
.filter(lambda x: x.split(',')[4] != '')\
.map(lambda x: ( int(x.split(',')[1]), float(x.split(',')[4]) ))

from operator import add
orderSubTotals = orderItemsMap.reduceByKey(add).map(lambda x: str(x[0]) +"\t" + str(x[1]))

orderSubTotals.saveAsTextFile("/data/output/pysparkSample1/")

#######################################PRACTIVEC
orders = sc.textFile("/data/data-master/retail_db/orders")
orderItems = sc.textFile("/data/data-master/retail_db/order_items")

from operator import add
orderItemsMap = orderItems\
        .map(lambda x: (int(x.split(',')[1]), round(float(x.split(',')[4]),2)))\
        .reduceByKey(add)

orderitemsDF = orderItemsMap.toDF(schema=["order_id", "order_revenue"])
orderitemsDF.save("/data/output/pySparkSample2", "json")

# PROBEM STATE<statement

orders = sc.textFile("/data/data-master/retail_db/orders")
orderItems = sc.textFile("/data/data-master/retail_db/order_items")
for i in orderItems.take(20): print i

products = sc.textFile("/data/data-master/retail_db/products")
for i in products.take(20): print i

ordersCompleted = orders.filter(lambda x: x.split(',')[3]
in ("CLOSED", "COMPLETE"))
for i in ordersCompleted.take(20): print i

ordersCompletedMap = ordersCompleted.map(lambda x: (int(x.split(',')[0]), str(x.split(',')[1].split(' ')[0])))
for i in ordersCompletedMap.take(20): print i

orderItemsMap = orderItems.map(lambda x: (int(x.split(',')[1]), x))
for i in orderItemsMap.take(20): print i

ordersJoin = ordersCompletedMap.join(orderItemsMap)
for i in ordersJoin.take(20): print i

dailyProducts = ordersJoin.map(lambda x:
        ((x[1][0], int(x[1][1].split(',')[2])), round(float(x[1][1].split(',')[4]), 1)
    ))
for i in dailyProducts.take(20): print i

from operator import add
dailyProductsReduced = dailyProducts.reduceByKey(add)
for i in dailyProductsReduced.take(20): print i

dailyProductsReducedProductId = dailyProductsReduced.map(lambda x: (x[0][1], x) )
for i in dailyProductsReducedProductId.take(20): print i

productsMap = products.map(lambda x: (int(x.split(',')[0]), x.split(',')[2]))
for i in productsMap.take(20): print i

productsJoined = dailyProductsReducedProductId.join(productsMap)
for i in productsJoined.take(20): print i

productsSimplified = productsJoined.map(lambda x:
    ((x[1][0][0][0], -x[1][0][1]), (x[1][0][0][1], x[1][1]))
).sortByKey()

for i in productsSimplified.take(20): print i

finalLIst = productsSimplified.map(lambda x:
    (x[0][0], -x[0][1], str(x[1][1]))
)
for i in finalLIst.take(20): print i

finalDF = finalLIst.toDF(schema=["Date", "Total", "Product"])
finalDF.write.save("/data/problemsTST/1", "json")
