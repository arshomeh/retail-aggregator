import pyspark.sql.functions as sf
import sys
from pyspark.sql import SparkSession

class SparkAggregator(object):

	def __init__(self, Load=True):
		"""
		:param Load: define either connect to mongo or create an empty list(for testing purposes)
		:initialize connection configurations to mongoDB
		:load the data from mongoDB to spark dataframe
		:return: None
		"""
		inputUri = "mongodb://mongo/Online_Retail.Retail"
		outputUri = "mongodb://mongo/Online_Retail.Retail"

		self.appSpark = SparkSession\
			.builder\
			.appName("Retail")\
			.config("spark.mongodb.input.uri", inputUri)\
			.config("spark.mongodb.output.uri", outputUri)\
			.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
			.getOrCreate()
		if(Load):
			try:
				self.df = self.appSpark.read.format('com.mongodb.spark.sql.DefaultSource').load()
			except Exception as e:
				print(e)
				sys.exit("Could not connect and load data from MongoDB")
		else:
			self.df = []


	def Division(self, Quantity=None, UnitPrice=None):
		"""
		:param Quantity: Quantity column
		:param UnitPrice: UnitPrice column
		:return: the division of UnitPrice over Quantity
		"""
		return sf.when(Quantity<=0, 0).otherwise(UnitPrice/Quantity)


	def Multiplication(self, Quantity=None, UnitPrice=None):
		"""
		:param Quantity: Quantity column
		:param UnitPrice: UnitPrice column
		:return: The multiplication of UnitPrice and Quantity
		"""
		return Quantity*UnitPrice


	def GroupByInvoice(self):
		"""
		:return: The total transactions for each invoice
		"""
		try:
			data = self.df.groupBy("InvoiceNo").agg(sf.count("InvoiceNo").alias("Transactions"))

			return data.toPandas()
		except Exception as e:
			print(e)
			sys.exit("Could not Group Transactions By Invoice")


	def MostSoldProduct(self):
		"""
		:return: The StockCode and the total Sold Amount of the most sold product
		"""
		try:
			data = self.df.groupBy("StockCode").\
				agg(sf.sum(sf.abs(self.df.Quantity)).alias("Sold Amount")).\
				sort(sf.col("Sold Amount").desc()).limit(1)

			return data.toPandas()
		except Exception as e:
			print(e)
			sys.exit("Could not compute Most Sold Product")


	def TheCustomerWhoSpendMost(self):
		"""
		:return: The CustomerID and the total expenses of the customer who spends the most money
		"""
		try:
			dfWithExpenses = self.df.withColumn("Expenses", self.Multiplication(sf.col("Quantity"), sf.col("UnitPrice")))

			data = dfWithExpenses.groupBy("CustomerID").\
				agg(sf.sum("Expenses").alias("Total Expenses")).\
				sort(sf.col("Total Expenses").desc()).limit(1)

			return data.toPandas()
		except Exception as e:
			print(e)
			sys.exit("Could not find The Customer Who Spend Most")


	def ProductsDistributionPerCountries(self):
		"""
		:return: The StockCode and the sold amount of each product per countries
		"""
		try:
			data = self.df.groupBy("StockCode","Country").\
				agg(sf.sum(sf.abs(self.df.Quantity)).alias("Sold Amount")).\
				sort(sf.col("StockCode").desc())

			dataWithStruct = data.withColumn("Distribution",sf.struct(data["StockCode"], data["Sold Amount"])).\
				groupBy("Country").agg(sf.collect_list("Distribution").alias("Distribution"))

			return dataWithStruct.toPandas()
		except Exception as e:
			print(e)
			sys.exit("Could not compute Products Distribution Per Countries")


	def AvgUnitPrice(self):
		"""
		:return: The average unit price
		"""
		try:
			data = self.df.dropDuplicates(["StockCode"]).agg(sf.avg(self.df.UnitPrice).alias("Avg Unit Price"))

			return data.toPandas()
		except Exception as e:
			print(e)
			sys.exit("Could not compute Avg Unit Price")


	def PriceDistribution(self):
		"""
		:return: The sold amount of the products for each unit price
		"""
		try:
			data = self.df.groupBy("UnitPrice").\
				agg(sf.sum(sf.abs(self.df.Quantity)).alias("Sold Amount")).sort(sf.col("UnitPrice"))

			return data.toPandas()
		except Exception as e:
			print(e)
			sys.exit("Could not compute Price Distribution")


	def PriceQuantityRatioPerInvoiceNo(self):
		"""
		:return: The price and quantity ratio per invoice
		"""
		try:
			data = self.df.groupBy("InvoiceNo", "UnitPrice").\
				agg(sf.sum(sf.abs(self.df.Quantity)).alias("Sold Amount")).sort(sf.col("InvoiceNo"))

			dataWithRatio = data.withColumn("Ratio", self.Division(sf.col("Sold Amount"), sf.col("UnitPrice")))

			dataWithStruct = dataWithRatio.withColumn("Ratios",sf.to_json(sf.struct(dataWithRatio["Sold Amount"], dataWithRatio["UnitPrice"], dataWithRatio["Ratio"]))).\
				groupBy("InvoiceNo").agg(sf.collect_list("Ratios").alias("Ratios"))

			return dataWithStruct.toJSON().collect()
		except Exception as e:
			print(e)
			sys.exit("Could not compute Price Quantity Ratio Per InvoiceNo")