# test_charts.py

import pytest
import sys, os
sys.path.insert(1, '../src')
from charts import *


def test_PlotProductsDistributionPerCountries_Fail():
	path = "../test/files/"
	data = {'CustomerID': ['13047'],
		'Total Expenses': [366.63]
		}
	with pytest.raises(SystemExit) as excinfo:
		PlotProductsDistributionPerCountries(data, path)
	assert str(excinfo.value) == "Could not generate the distribution of the products per countries charts"


def test_PlotPriceDistribution_Fail():
	path = "../test/files/"
	data = {'CustomerID': ['13047'],
		'Total Expenses': [366.63]
		}
	with pytest.raises(SystemExit) as excinfo:
		PlotPriceDistribution(data, path)
	assert str(excinfo.value) == "Could not generate the price distribution charts"


def test_PlotProductsDistributionPerCountries_Pass():
	path = "../test/files/"
	data = {'Country': ['TEST1','TEST2'],
		'Distribution':[
					[('22728',24.0),('22727',24.0),('22726',12.0),('21883',24.0),('21791',24.0),('21724',12.0),('21035',18.0),('10002',48.0)],
					[('85123A',6.0),('84969',6.0),('84879',32.0),('84406B',8.0),('84029G',6.0),('84029E',6.0),('71053',6.0),('48187',4.0),('22960',6.0),
					('22914',3.0),('22913',3.0),('22912',3.0),('22752',2.0),('22749',8.0),('22748',6.0),('22745',6.0),('22633',6.0),('22632',6.0),
					('22623',3.0),('22622',2.0),('22310',6.0),('21777',4.0),('21756',3.0),('21755',3.0),('21754',3.0),('21730',6.0)]
		]
	}
	df = pd.DataFrame(data, columns = ['Country', 'Distribution'])
	PlotProductsDistributionPerCountries(df, path)
	assert os.path.exists("./files/TEST1.jpg") == True
	assert os.path.exists("./files/TEST2.jpg") == True
	os.remove("./files/TEST1.jpg")
	os.remove("./files/TEST2.jpg")


def test_PlotPriceDistribution_Pass():
	path = "../test/files/"
	data = {'UnitPrice': [0.65,0.85,1.25,1.65,1.69,1.85,2.1,2.55,2.75,2.95,3.39,3.75,4.25,4.95,5.95,7.65,7.95,9.95],
			'Sold Amount': [24.0,60.0,24.0,6.0,32.0,12.0,12.0,6.0,8.0,18.0,18.0,20.0,18.0,12.0,9.0,2.0,8.0,2.0]
		}
	df = pd.DataFrame(data, columns = ['UnitPrice', 'Sold Amount'])
	PlotPriceDistribution(df, path)
	assert os.path.exists("./files/1_UnitPrice.jpg") == True
	os.remove("./files/1_UnitPrice.jpg")