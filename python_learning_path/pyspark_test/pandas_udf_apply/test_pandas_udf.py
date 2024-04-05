
def main_udf_fun( arg1, start_date, end_date, policy_parm_gird_by_dim, horizon, M_NAME):

  def unit(keys, data):

      
      m = keys[0]
      m2 = keys[1]

      print(m,m2)
      print(data)