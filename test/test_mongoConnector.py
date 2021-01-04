# test_mongoConnector.py

import pytest
import mongomock
import sys
import pandas as pd
sys.path.insert(1, '../src')
from mongoConnector import *


def FakeMongo():
	dbName="Test"
	collectionName="test"
	mongodb = MongoDB(dbName, collectionName)

	# override client
	mongodb.client = mongomock.MongoClient()
	mongodb.DB = mongodb.client[dbName]
	mongodb.collection = mongodb.DB[collectionName]

	return mongodb


def CreatePandasDataFrame():

	cars = {'Brand': ['Honda Civic','Toyota Corolla','Ford Focus','Audi A4'],
			'Price': [22000,25000,27000,35000]
			}
	df = pd.DataFrame(cars, columns = ['Brand', 'Price'])

	return df


def CreateJsonString():

	jsonString = ['{"InvoiceNo":"536365","Ratios":["{\\"Sold Amount\\":18,\\"UnitPrice\\":3.39,\\"Ratio\\":0.18833333333333335}"]}',
					'{"InvoiceNo":"536366","Ratios":["{\\"Sold Amount\\":12,\\"UnitPrice\\":1.85,\\"Ratio\\":0.15416666666666667}"]}']

	return jsonString


def test_default_initial():
	mongodb = MongoDB(dbName="Test", collectionName="test")
	assert mongodb.dbName == "Test"
	assert mongodb.collectionName == "test"


def test_LoadData_Fail():
	mongodb = MongoDB(dbName="Test", collectionName="test")
	with pytest.raises(SystemExit) as excinfo:
		mongodb.LoadData("")
	assert str(excinfo.value) == "Could not load data to MongoDB"


def test_LoadData_Pass():
	mongodb=FakeMongo()
	mongodb.LoadData("../test/files/for_mongo.csv")
	assert mongodb.collection.count_documents({}) == 4


def test_InsertMany_Fail():
	mongodb=FakeMongo()
	with pytest.raises(SystemExit) as excinfo:
		mongodb.InsertMany()
	assert str(excinfo.value) == "Could not Insert data to MongoDB"


def test_InsertMany_Pass():
	mongodb=FakeMongo()
	df = CreatePandasDataFrame()
	mongodb.InsertMany("TestInsertMany", df)
	assert mongodb.DB["TestInsertMany"].count_documents({}) == 4


def test_InsertOne_Fail():
	mongodb=FakeMongo()
	with pytest.raises(SystemExit) as excinfo:
		mongodb.InsertOne()
	assert str(excinfo.value) == "Could not Insert data to MongoDB"


def test_InsertOne_Pass():
	mongodb=FakeMongo()
	json = CreateJsonString()
	mongodb.InsertOne("TestInsertOne", json)
	assert mongodb.DB["TestInsertOne"].count_documents({}) == 2