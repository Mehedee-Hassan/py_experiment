from datetime import datetime
from test_pandas_udf import main_udf_fun
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
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
list_of_c_r = args.get("list_of_commission_rate", "3,18,16").split(',')

t_p = int(args.get("T_P",1))

print("list_of_a_a",list_of_a_a)
print("list_of_r_p",list_of_r_p)
print("list_of_c_r",list_of_c_r)
print("t_p",t_p)

"""
output:
t_of_a_a ['10E1', '100E1', '19']
list_of_r_p ['5', '50', '10']
list_of_c_r ['3', '18', '16']
"""

"""
Setup parameters
"""
list_of_a_a_args = np.linspace(float(list_of_a_a[0]),
                               float(list_of_a_a[1]),
                               int(list_of_a_a[2]),
                               dtype=int)
"""
output:
array([ 100,  150,  200,  250,  300,  350,  400,  450,  500,  550,  600,
        650,  700,  750,  800,  850,  900,  950, 1000])
"""

list_of_r_p_args = np.linspace(float(list_of_r_p[0]),
                               float(list_of_r_p[1]),
                               int(list_of_r_p[2]),
                               dtype=int)

"""
output:
array([ 5, 10, 15, 20, 25, 30, 35, 40, 45, 50])
"""
list_of_cr_args = np.linspace(float(list_of_c_r[0]),
                              float(list_of_c_r[1]),
                              int(list_of_c_r[2]),
                              dtype=int)

"""
output:
array([ 3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18])
"""



po_param_g = [list_of_a_a_args, list_of_r_p_args, list_of_cr_args]

print("po_param_g", po_param_g)

"""
output:
po_param_g [array([ 100,  150,  200,  250,  300,  350,  400,  450,  500,  550,  600,
        650,  700,  750,  800,  850,  900,  950, 1000]), 
        array([ 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]), 
        array([ 3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18])]

"""

t_e_d = datetime.strptime("2023-01-01", "%Y-%m-%d")
h_e_d = datetime.strptime("2025-06-01", "%Y-%m-%d")
h_i_d = (h_e_d - t_e_d).days

m_name = ['a','b','c','d','e','f','g',f'h_{10-t_p}',f'i_{10-t_p}']

print("t_end_d",t_e_d)
print("h_end_d",h_e_d)
print("h_i_d",h_i_d)
print("m_name",m_name)



"""
output:
t_e_d 2023-01-01 00:00:00
h_e_d 2025-06-01 00:00:00
h_i_d 882 --> (h_end_d - t_end_d).days
m_name ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h_9', 'i_9']
"""

data = [(1111111, 100000, 0.234, 10, 2, 0.1),
        (2111112, 150000, 0.111, 20, 2, 0.2),
        (2111113, 150000, 0.111, 20, 2, 0.2),
        (2111114, 150000, 0.111, 20, 2, 0.2),
        (2111115, 150000, 0.111, 20, 2, 0.2),
        (2111112, 150000, 0.111, 20, 2, 0.2),
        (2111116, 150000, 0.111, 20, 2, 0.2),
        (2111112, 150000, 0.111, 20, 2, 0.2),
        (2111116, 150000, 0.111, 20, 2, 0.2),
        (2111117, 150000, 0.111, 20, 2, 0.2),
        (2111117, 150000, 0.111, 20, 2, 0.2),


        ]

columns = ["id", "a" , "b" , "c", "d", "e"]


df = spark.createDataFrame(data, schema=columns)
df.show()

test = df.groupby("id","a").count().show()



df_res = df.groupby("id").applyInPandas(
    main_udf_fun(
        n_samples,
        f_start_date,
        f_enda_date,
        po_param_g=po_param_g,
        horizon=h_i_d,
        M_NAME=m_name

    ),
    schema= "a int, b int"
)
