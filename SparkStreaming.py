etc/hadoop/conf/core-site.xml



pyspark \
--master yarn \
--conf spark.ui.port=12345 \
--executor-memory 512M \
--num-executors 1
