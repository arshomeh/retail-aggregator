import os, sys
import pandas as pd
from matplotlib import pyplot as plt

def PlotPriceDistribution(df=None, path=None):
	"""
	:param df: Pandas data frame
	:param path: Path to the direcory where to save the chart
	:creates the Price Distribution chart
	:return: None
	"""
	maxRows = 100
	dataFrames = []

	try:
		if not os.path.exists(path):
			os.makedirs(path)

		while len(df) > maxRows:
			top = df[:maxRows]
			dataFrames.append(top)
			df = df[maxRows:]
		else:
			dataFrames.append(df)

		count = 1
		for adf in dataFrames:
			adf.plot(kind='line', x='UnitPrice', y='Sold Amount')
			plt.savefig(path+str(count)+"_UnitPrice.jpg")
			count = count+1
	except Exception as e:
		print(e)
		sys.exit("Could not generate the price distribution charts")


def PlotProductsDistributionPerCountries(df=None, path=None):
	"""
	:param df: Pandas data frame
	:param path: Path to the direcory where to save the chart
	:creates the Products Distribution Per Countries chart
	:return: None
	"""
	try:
		if not os.path.exists(path):
			os.makedirs(path)

		for index, row in df.iterrows():
			country = row[0]
			newDf = pd.DataFrame(row[1])
			newDf.columns = ['StockCode', 'Sold Amount']
			stockCodeList = (list(newDf['StockCode']))
			soldAmount = (list(newDf['Sold Amount']))
			fig, ax = plt.subplots(figsize =(140, 45))
			ax.bar(stockCodeList, soldAmount)
			ax.set_xlabel('StockCode',fontsize=100)
			ax.set_ylabel('Sold Amount',fontsize=100)
			plt.title('Products Distribution of ' + country, fontsize=100)
			plt.xticks(rotation=90, fontsize=20)
			plt.yticks(fontsize=50)
			plt.savefig(path+country+".jpg", dpi=100)
			plt.cla()
			plt.clf()
			plt.close(fig)
	except Exception as e:
		print(e)
		sys.exit("Could not generate the distribution of the products per countries charts")
