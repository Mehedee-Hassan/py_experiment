from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test spark pandas ") \
    .getOrCreate()

"""
Constants
"""
args = {}
n_samples = 10E2
f_start_date = "2023-01-01"
f_enda_date = "2026-03-01"

"""
Transformation of constants
"""
list_of_a_a= args.get("list_of_advanced_amount" , "10E1,100E1,19")\
    .split(',')
list_of_r_p = args.get("list_of_repayment_percent","5,50,10").split(',')


p_parametes_grid_by_dim


data = [("John Doe", 30),
        ("Jane Doe", 25)]

columns = ["Name", "Age"]

df = spark.createDataFrame(data, schema=columns)
df.show()
