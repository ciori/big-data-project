from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/mariadb-java-client-2.3.0.jar") \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

jdbcDF = spark.read.load("test.csv", format="csv", sep=",", inferSchema="true", header="false")

jdbcDF.write \
    .option("createTableColumnTypes", "user_keyword VARCHAR(100), count INT") \
    .jdbc("org.mariadb.jdbc.Driver", "schema.tablename", properties={"user": "ciori", "password": "mariachepsw"})