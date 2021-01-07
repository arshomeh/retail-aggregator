import os, sys
from mongoConnector import *
from sparkAggregator import *
from converter import *
from charts import *

if __name__ == "__main__":

	try:
		FilePath = sys.argv[1]
	except Exception as e:
		print(e)
		sys.exit("Wrong argument")

	converter = Converter(FilePath)
	converter.XlsxToCsv()
	
	mongodb = MongoDB(dbName='Online_Retail', collectionName='Retail')
	mongodb.LoadData(converter.csvFile)

	# sparkAgg = SparkAggregator()

	# data = sparkAgg.GroupByInvoice()
	# mongodb.InsertMany("_GroupByInvoice", data)

	# data = sparkAgg.MostSoldProduct()
	# mongodb.InsertMany("_MostSoldProduct", data)

	# data = sparkAgg.TheCustomerWhoSpendMost()
	# mongodb.InsertMany("_TheCustomerWithMostExpenses", data)

	# data = sparkAgg.ProductsDistributionPerCountries()
	# inp = input("Create distribution of the products per countries charts? [Y/else]:")
	# if inp=="Y":
	# 	path = "../charts/ProductsDistributionPerCountries/"
	# 	PlotProductsDistributionPerCountries(data, path)
	# mongodb.InsertMany("_ProductsDistributionPerCountries", data)

	# data = sparkAgg.AvgUnitPrice()
	# mongodb.InsertMany("_AvgUnitPrice", data)

	# data = sparkAgg.PriceDistribution()
	# inp = input("Create price distribution charts? [Y/else]:")
	# if inp=="Y": 
	# 	path = "../charts/PriceDistribution/"
	# 	PlotPriceDistribution(data, path)
	# mongodb.InsertMany("_PriceDistribution", data)

	# jsonString = sparkAgg.PriceQuantityRatioPerInvoiceNo()
	# mongodb.InsertOne("_PriceQuantityRatioPerInvoiceNo", jsonString)
