import pandas as pd
import numpy as np
import json
import os, sys
from pymongo import MongoClient

class MongoDB(object):

	def __init__(self, dbName=None, collectionName=None):
		"""
		:param dbName: Path os csv File
		:param collectionName: Path os csv File
		:initialize connection configurations
		:return: None
		"""
		self.dbName = dbName
		self.collectionName = collectionName

		self.client = MongoClient("mongodb://mongo", 27017, maxPoolSize=50)

		self.DB = self.client[self.dbName]
		self.collection = self.DB[self.collectionName]


	def LoadData(self, filePath=None):
		"""
		:param filePath: Path to csv File
		:loads the data to MongoDB
		:return: None
		"""
		print("Inserting the data to Mongo DB")
		try:
			df = pd.read_csv(filePath)
			if self.collection.count_documents({}) == 0:	

				for column in df:
					df[column].replace('', np.nan, inplace=True)
					df[column].replace(0.0, np.nan, inplace=True)
					df.dropna(subset=[column], inplace=True)

				df['CustomerID'] = df['CustomerID'].astype(int)
				df['Description'] = df['Description'].astype(str)
				
				data = df.to_dict('records')

				self.collection.insert_many(data, ordered=False)
				print("All the Data has been Exported to Mongo DB")
			else:
				print("Data already exists in Mongo DB")
		except Exception as e:
				print(e)
				sys.exit("Could not load data to MongoDB")


	def InsertMany(self, viewName=None, df=None):
		"""
		:param viewName: The name of the new collection
		:param df: Spark dataframe
		:inserts the data to the new collection by once
		:return: None
		"""
		try:
			collectionView = self.DB[viewName]
			collectionView.delete_many({})

			data = df.to_dict('records')
			collectionView.insert_many(data, ordered=False)
		except Exception as e:
			print(e)
			sys.exit("Could not Insert data to MongoDB")


	def InsertOne(self, viewName=None, jsonString=None):
		"""
		:param viewName: The name of the new collection
		:param jsonString: json string of the data
		:inserts the data to the new collection one by one
		:return: None
		"""
		try:
			collectionView = self.DB[viewName]
			collectionView.delete_many({})

			for x in jsonString:
				x = x.replace("\\", "").replace("\"{", "{").replace("}\"", "}")
				obj = json.loads(x)
				collectionView.insert_one(obj)
		except Exception as e:
			print(e)
			sys.exit("Could not Insert data to MongoDB")
