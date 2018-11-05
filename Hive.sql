pyspark \
--master yarn \
--conf spark.ui.port=12345 \
--executor-memory 512M \
--num-executors 1

show databases;

create database mjibran_retail_db_txt;
use mjibran_retail_db_txt;

CREATE TABLE orders(
order_id int,
order_date string,
order_customer_id int,
order_status string
)
 COMMENT 'Orders Table'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/data-master/retail_db/orders' INTO TABLE orders;
select * from orders;


CREATE TABLE orders_items(
order_items_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
)
 COMMENT 'Order Items Table'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/data-master/retail_db/order_items' INTO TABLE orders_items;
select * from orders_items limit 10;


CREATE TABLE products(
product_id int,
product_category_id int,
product_name string,
product_description string,
product_price float,
product_image string
)
 COMMENT 'Products Table'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA local INPATH '/home/cloudera/Desktop/data-master/retail_db/products' INTO TABLE products;
select * from products limit 10;


##################### ORC ###########
create database mjibran_retail_db_orc;
use mjibran_retail_db_orc;

CREATE TABLE orders(
order_id int,
order_date string,
order_customer_id int,
order_status string
)
 COMMENT 'Orders Table'
STORED AS ORC;

CREATE TABLE orders_items(
order_items_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
)
 COMMENT 'Order Items Table'
STORED AS ORC;

CREATE TABLE products(
product_id int,
product_category_id int,
product_name string,
product_description string,
product_price float,
product_image string
)
 COMMENT 'Products Table'
STORED AS ORC;

insert into table orders select * from mjibran_retail_db_txt.orders;
insert into table orders_Items select * from mjibran_retail_db_txt.orders_items;
insert into table products select * from mjibran_retail_db_txt.products;

select * from orders limit 10;
select * from orders_items limit 10;
select * from products limit 10;

############ PYSPARK #######################

pyspark \
--master yarn \
--conf spark.ui.port=12345 \
--executor-memory 512M \
--num-executors 1

sqlContext.sql("use mjibran_retail_db_txt")
sqlContext.sql("show tables").show()


###### FUNCTIONs
current_date
current_timestamp
date_add
date_format
date_sub
datediff
day
dayofmonth
to_date
to_unix_timestamp
to_utc_timestamp
from_unixtime
from_utc_timestamp
minute
month
months_between
next_day


substr or substring
instr
like
rlike
length
lcase or lower
ucase or upper
initcap
trim, ltrim, rtrim
cast
lpad, rpad
split
concat

##### Queries
-- Create DF from RDD
from pyspark.sql import Row

orderedRead = sc.textFile("/data/data-master/retail_db/orders")
for i in orderedRead.take(10) : print i


ordersRdd = orderedRead.map(lambda x: Row(
  order_id=int(x.split(',')[0]),
  order_date=x.split(',')[1],
  cutomer_id=int(x.split(',')[2]),
  order_status=x.split(',')[3]
)).toDF()
ordersRdd.show()
# to create temp Tables
ordersRdd.registerTempTable("order_temp")
sqlContext.sql("select * from order_temp").show()

#
query = "select count(*), order_status from order_temp where order_status in ('COMPLETE','CLOSED') group by order_status"
sqlContext.sql(query).show()

# Read prducts from local and make DF
products = open("/home/cloudera/Desktop/data-master/retail_db/products/part-00000").readlines()
productsRDD = sc.parallelize(products)
for i in productsRDD.take(10): print i
productsSelected = productsRDD.map(lambda x: Row(
  product_id=int(x.split(',')[0]),
  product_name=x.split(',')[2]
)).toDF()
productsSelected.show()
productsSelected.registerTempTable("products_temp")
query = "select * from products_temp"
sqlContext.sql(query).show()

# Read order_items
order_items = open("/home/cloudera/Desktop/data-master/retail_db/order_items/part-00000").readlines()
order_itemsRDD = sc.parallelize(order_items)
for i in order_itemsRDD.take(10): print i
order_items = order_itemsRDD.map(lambda x: Row(
  order_item_id=int(x.split(',')[0]),
  order_item_order_id=int(x.split(',')[1]),
  order_item_product_id=int(x.split(',')[2]),
  order_item_quantity=int(x.split(',')[3]),
  order_item_subtotal=float(x.split(',')[4]),
  order_item_product_price=float(x.split(',')[5])
)).toDF()

order_items.registerTempTable("order_items_temp")
query = "select * from order_items_temp"
sqlContext.sql(query).show()

# Get daily revenue by products
select \
substring(o.order_date, 1, 10) Date,\
round(sum(oi.order_item_subtotal),2) DailyRevenue, \
p.product_name Product \
from order_temp o \
left join \
order_items_temp oi on (oi.order_item_order_id = o.order_id) \
left join \
products_temp p on (p.product_id = oi.order_item_product_id) \
where \
o.order_status in ('COMPLETE','CLOSED') \
group by \
o.order_date, p.product_id, p.product_name \
order by \
o.order_date, DailyRevenue desc

# Save in ORC Table
create database mjibran_retail_db_orc;
create table mjibran_retail_db_orc.daily_revenue (\
Date string,\
DailyRevenue float,\
Product string\
)stored as ORC

output = sqlContext.sql(query)
output.insertInto("mjibran_retail_db_orc.daily_revenue",overwrite=True)
select * from mjibran_retail_db_orc.daily_revenue
sqlContext.sql("select * from mjibran_retail_db_orc.daily_revenue").show()
