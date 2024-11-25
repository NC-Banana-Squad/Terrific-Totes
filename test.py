x = "sales_order/2024/11/25/2024-11-25T16:19:13.511122.csv"

year, month, day, file_name = x.split('/')[1:5]

name = "dim_table"

output_path = f"{name}/{year}/{month}/{day}/{file_name[:-4]}.parquet"

print(output_path)
