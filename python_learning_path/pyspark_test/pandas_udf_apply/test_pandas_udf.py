import pandas as pd

def main_udf_fun(arg1, start_date, end_date, po_param_g, horizon, M_NAME):
    def unit(keys, data):
        print("inside unit")
        print(keys)
        print(data)

        data = [[1, 10], [32, 12], [3, 13]]
        df = pd.DataFrame(data, columns=['a', 'b'])
        print(df)

        return df
    return unit