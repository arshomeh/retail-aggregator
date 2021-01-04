# test_sparkAggregator.py

import pytest
import sys
import pandas as pd
sys.path.insert(1, '../src')
from sparkAggregator import *


sparkFail = SparkAggregator(False)
assert sparkFail.df == []


# --------------------------------FAIL------------------------------------------------------


def test_GroupByInvoice_Fail():
	with pytest.raises(SystemExit) as excinfo:
		real_output = sparkFail.GroupByInvoice()
	assert str(excinfo.value) == "Could not Group Transactions By Invoice"


def test_MostSoldProduct_Fail():
	with pytest.raises(SystemExit) as excinfo:
		real_output = sparkFail.MostSoldProduct()
	assert str(excinfo.value) == "Could not compute Most Sold Product"


def test_TheCustomerWhoSpendMost_Fail():
	with pytest.raises(SystemExit) as excinfo:
		real_output = sparkFail.TheCustomerWhoSpendMost()
	assert str(excinfo.value) == "Could not find The Customer Who Spend Most"


def test_ProductsDistributionPerCountries_Fail():
	with pytest.raises(SystemExit) as excinfo:
		real_output = sparkFail.ProductsDistributionPerCountries()
	assert str(excinfo.value) == "Could not compute Products Distribution Per Countries"


def test_AvgUnitPrice_Fail():
	with pytest.raises(SystemExit) as excinfo:
		real_output = sparkFail.AvgUnitPrice()
	assert str(excinfo.value) == "Could not compute Avg Unit Price"


def test_PriceDistribution_Fail():
	with pytest.raises(SystemExit) as excinfo:
		real_output = sparkFail.PriceDistribution()
	assert str(excinfo.value) == "Could not compute Price Distribution"


def test_PriceQuantityRatioPerInvoiceNo_Fail():
	with pytest.raises(SystemExit) as excinfo:
		real_output = sparkFail.PriceQuantityRatioPerInvoiceNo()
	assert str(excinfo.value) == "Could not compute Price Quantity Ratio Per InvoiceNo"


# --------------------------------PASS------------------------------------------------------


#Creating Global Spark DataFrame:
spark = SparkAggregator(False)
spark.df = spark.appSpark.read.csv("./files/for_spark.csv",  header='true')


def test_GroupByInvoice_Pass():
	real_output = spark.GroupByInvoice()

	data = {'InvoiceNo': ['536366','536367','536369','536370','536368','536365'],
		'Transactions': [2,12,1,8,4,7]
		}
	expected_output = pd.DataFrame(data, columns = ['InvoiceNo', 'Transactions'])

	pd.testing.assert_frame_equal(
		expected_output,
		real_output,
		check_like=True,
	)


def test_MostSoldProduct_Pass():
	real_output = spark.MostSoldProduct()

	data = {'StockCode': ['10002'],
		'Sold Amount': [48.0]
		}
	expected_output = pd.DataFrame(data, columns = ['StockCode', 'Sold Amount'])

	pd.testing.assert_frame_equal(
		expected_output,
		real_output,
		check_like=True,
	)


def test_TheCustomerWhoSpendMost_Pass():
	real_output = spark.TheCustomerWhoSpendMost()

	data = {'CustomerID': ['13047'],
		'Total Expenses': [366.63]
		}
	expected_output = pd.DataFrame(data, columns = ['CustomerID', 'Total Expenses'])

	pd.testing.assert_frame_equal(
		expected_output,
		real_output,
		check_like=True,
	)


def test_ProductsDistributionPerCountries_Pass():
	real_output = spark.ProductsDistributionPerCountries()

	data = {'Country': ['France','United Kingdom'],
			'Distribution':[
						[('22728',24.0),('22727',24.0),('22726',12.0),('21883',24.0),('21791',24.0),('21724',12.0),('21035',18.0),('10002',48.0)],
						[('85123A',6.0),('84969',6.0),('84879',32.0),('84406B',8.0),('84029G',6.0),('84029E',6.0),('71053',6.0),('48187',4.0),('22960',6.0),
						('22914',3.0),('22913',3.0),('22912',3.0),('22752',2.0),('22749',8.0),('22748',6.0),('22745',6.0),('22633',6.0),('22632',6.0),
						('22623',3.0),('22622',2.0),('22310',6.0),('21777',4.0),('21756',3.0),('21755',3.0),('21754',3.0),('21730',6.0)]
			]
		}
	expected_output = pd.DataFrame(data, columns = ['Country', 'Distribution'])

	pd.testing.assert_frame_equal(
		expected_output,
		real_output,
		check_like=True,
	)


def test_AvgUnitPrice_Pass():
	real_output = spark.AvgUnitPrice()

	data = {'Avg Unit Price': [3.887059]
		}
	expected_output = pd.DataFrame(data, columns = ['Avg Unit Price'])

	pd.testing.assert_frame_equal(
		expected_output,
		real_output,
		check_like=True,
	)


def test_PriceDistribution_Pass():
	real_output = spark.PriceDistribution()

	data = {'UnitPrice': [0.65,0.85,1.25,1.65,1.69,1.85,2.1,2.55,2.75,2.95,3.39,3.75,4.25,4.95,5.95,7.65,7.95,9.95],
			'Sold Amount': [24.0,60.0,24.0,6.0,32.0,12.0,12.0,6.0,8.0,18.0,18.0,20.0,18.0,12.0,9.0,2.0,8.0,2.0]
		}
	expected_output = pd.DataFrame(data, columns = ['UnitPrice', 'Sold Amount'])
	real_output["UnitPrice"] = pd.to_numeric(real_output["UnitPrice"])

	pd.testing.assert_frame_equal(
		expected_output,
		real_output,
		check_like=True,
	)


def test_PriceQuantityRatioPerInvoiceNo_Pass():
	real_output = spark.PriceQuantityRatioPerInvoiceNo()

	expected_output = [
	'{"InvoiceNo":"536365","Ratios":["{\\"Sold Amount\\":6.0,\\"UnitPrice\\":\\"2.55\\",\\"Ratio\\":0.425}","{\\"Sold Amount\\":8.0,\\"UnitPrice\\":\\"2.75\\",\\"Ratio\\":0.34375}","{\\"Sold Amount\\":2.0,\\"UnitPrice\\":\\"7.65\\",\\"Ratio\\":3.825}","{\\"Sold Amount\\":18.0,\\"UnitPrice\\":\\"3.39\\",\\"Ratio\\":0.18833333333333335}","{\\"Sold Amount\\":6.0,\\"UnitPrice\\":\\"4.25\\",\\"Ratio\\":0.7083333333333334}"]}',
	'{"InvoiceNo":"536366","Ratios":["{\\"Sold Amount\\":12.0,\\"UnitPrice\\":\\"1.85\\",\\"Ratio\\":0.15416666666666667}"]}',
	'{"InvoiceNo":"536367","Ratios":["{\\"Sold Amount\\":8.0,\\"UnitPrice\\":\\"7.95\\",\\"Ratio\\":0.99375}","{\\"Sold Amount\\":12.0,\\"UnitPrice\\":\\"2.1\\",\\"Ratio\\":0.17500000000000002}","{\\"Sold Amount\\":6.0,\\"UnitPrice\\":\\"5.95\\",\\"Ratio\\":0.9916666666666667}","{\\"Sold Amount\\":6.0,\\"UnitPrice\\":\\"4.25\\",\\"Ratio\\":0.7083333333333334}","{\\"Sold Amount\\":2.0,\\"UnitPrice\\":\\"9.95\\",\\"Ratio\\":4.975}","{\\"Sold Amount\\":8.0,\\"UnitPrice\\":\\"3.75\\",\\"Ratio\\":0.46875}","{\\"Sold Amount\\":32.0,\\"UnitPrice\\":\\"1.69\\",\\"Ratio\\":0.0528125}","{\\"Sold Amount\\":3.0,\\"UnitPrice\\":\\"4.95\\",\\"Ratio\\":1.6500000000000001}","{\\"Sold Amount\\":6.0,\\"UnitPrice\\":\\"1.65\\",\\"Ratio\\":0.27499999999999997}"]}',
	'{"InvoiceNo":"536368","Ratios":["{\\"Sold Amount\\":9.0,\\"UnitPrice\\":\\"4.95\\",\\"Ratio\\":0.55}","{\\"Sold Amount\\":6.0,\\"UnitPrice\\":\\"4.25\\",\\"Ratio\\":0.7083333333333334}"]}',
	'{"InvoiceNo":"536369","Ratios":["{\\"Sold Amount\\":3.0,\\"UnitPrice\\":\\"5.95\\",\\"Ratio\\":1.9833333333333334}"]}',
	'{"InvoiceNo":"536370","Ratios":["{\\"Sold Amount\\":24.0,\\"UnitPrice\\":\\"0.65\\",\\"Ratio\\":0.027083333333333334}","{\\"Sold Amount\\":60.0,\\"UnitPrice\\":\\"0.85\\",\\"Ratio\\":0.014166666666666666}","{\\"Sold Amount\\":12.0,\\"UnitPrice\\":\\"3.75\\",\\"Ratio\\":0.3125}","{\\"Sold Amount\\":18.0,\\"UnitPrice\\":\\"2.95\\",\\"Ratio\\":0.1638888888888889}","{\\"Sold Amount\\":24.0,\\"UnitPrice\\":\\"1.25\\",\\"Ratio\\":0.052083333333333336}"]}']

	assert real_output == expected_output