import os
import pandas as pd
from matplotlib import pyplot as plt

def PlotPriceDistribution(df=None):
	"""
	:param df: Pandas data frame
	:creates the Price Distribution chart
	:return: None
	"""
	df1 = df[df['UnitPrice']<1]

	df2 = df[df['UnitPrice']>=1]
	df2 = df2[df2['UnitPrice']<20]

	df3 = df[df['UnitPrice']>=20]
	df3 = df3[df3['UnitPrice']<100]


	df4 = df[df['UnitPrice']>=100]
	df4 = df4[df4['UnitPrice']<1000]

	df5 = df[df['UnitPrice']>=1000]

	path = "../charts/PriceDistribution/"
	if not os.path.exists(path):
		os.makedirs(path)

	df1.plot(kind='line', x='UnitPrice', y='Sold Amount')
	plt.savefig(path+"1_UnitPrice_1.jpg")

	df2.plot(kind='line', x='UnitPrice', y='Sold Amount')
	plt.savefig(path+"2_UnitPrice_20.jpg")

	df3.plot(kind='line', x='UnitPrice', y='Sold Amount')
	plt.savefig(path+"3_UnitPrice_100.jpg")

	df4.plot(kind='line', x='UnitPrice', y='Sold Amount')
	plt.savefig(path+"4_UnitPrice_1000.jpg")

	df5.plot(kind='line', x='UnitPrice', y='Sold Amount')
	plt.savefig(path+"5_UnitPrice_40000.jpg")


def PlotProductsDistributionPerCountries(df=None):
	"""
	:param data: Pandas data frame
	:creates the Products Distribution Per Countries chart
	:return: None
	"""
	path = "../charts/ProductsDistributionPerCountries/"
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
