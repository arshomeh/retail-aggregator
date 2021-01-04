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

	sparkCal = SparkAggregator()

	# data = sparkCal.GroupByInvoice()
	# mongodb.InsertMany("_GroupByInvoice", data)

	# data = sparkCal.MostSoldProduct()
	# mongodb.InsertMany("_MostSoldProduct", data)

	data = sparkCal.TheCustomerWhoSpendMost()
	mongodb.InsertMany("_TheCustomerWithMostExpenses", data)

	# data = sparkCal.ProductsDistributionPerCountries()
	# PlotProductsDistributionPerCountries(data)
	# mongodb.InsertMany("_ProductsDistributionPerCountries", data)

	# data = sparkCal.AvgUnitPrice()
	# mongodb.InsertMany("_AvgUnitPrice", data)

	# data = sparkCal.PriceDistribution()
	# PlotPriceDistribution(data)
	# mongodb.InsertMany("_PriceDistribution", data)

	jsonString = sparkCal.PriceQuantityRatioPerInvoiceNo()
	mongodb.InsertOne("_PriceQuantityRatioPerInvoiceNo", jsonString)



