import csv
import json
import os
def csv_to_json(csv_file_path, json_file_path):
    with open(csv_file_path, 'r') as csv_file:
        csv_data = csv.DictReader(csv_file)
        json_data = []
        for row in csv_data:
            json_data.append(row)

    with open(json_file_path, 'w') as json_file:
        json_file.write(json.dumps(json_data, indent=4))
    print("Data has conveted from csv to json")

def get_files_path(files_path,json_path):
    op=os.listdir(files_path)
    for each in op:
        temp_path=os.path.join(files_path,each)
        json_path1=each.split(".")[0]+".json"
        json_path2=os.path.join(json_path,json_path1)
        csv_to_json(temp_path,json_path2)

if __name__ == '__main__':
    get_files_path(files_path="C:/Users/ravikumar.g/Downloads/salesdataset",json_path="C:/Users/ravikumar.g/Downloads/sales_jsonset")
'''


4.spark.sql("SELECT COUNT(*) AS order_count FROM sales").show()
+-----------+
|order_count|
+-----------+
|      29481|
5.
from pyspark.sql.functions import avg
>>>
>>> averageRevenueDF = spark.sql("SELECT AVG(ProductCost) AS average_revenue FROM sales")
>>> averageRevenueDF.show()
+----------------+
| average_revenue|
+----------------+
|177.301573633858|
+----------------+
7.
8.
# Assuming you have a table or DataFrame named "orders" with columns "revenue" and "date"
revenuePerMonthPerYearDF = spark.sql("SELECT year(date) AS year, month(date) AS month, SUM(ProductCost) AS total_revenue FROM sales GROUP BY year, month")
revenuePerMonthPerYearDF.show()
# Show the total revenue per month per year
revenuePerMonthPerYearDF.show()
9)revenuePerMonthPerYearDF = spark.sql("""SELECT year(date) AS year,month(date) AS month,sum(ProductPrice) AS total_revenue FROM sales GROUP BY year,month ORDER BY year,month """)
10)from pyspark.sql.functions import max

>>> highestPricedProductDF=spark.sql(""" SELECT * FROM sales WHERE ProductPrice = (SELECT MAX(ProductPrice) FROM sales) """)
>>> highestPricedProductDF.show()

11)orderCountByDateDF = spark.sql(""" SELECT OrderDate ,COUNT(*) AS order_count FROM sales GROUP BY OrderDate ORDER BY OrderDate """)
12)t.createOrReplaceTempView("sales")
sortedProductsByCategoryDF = spark.sql("""SELECT * FROM sales ORDER BY ProductSubcategoryKey ASC """)
sortedProductsByCategoryDF.show()


'''