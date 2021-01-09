import os, sys
import pandas as pd

class Converter(object):
	
	def __init__(self, filePath=None):
		"""
		:param filePath: Path to input .xlsx file
		:cretes the path to .csv file with the same name as the input file
		:return: None
		"""
		self.xmlFile = filePath

		if os.path.exists(filePath):
			name, ext = os.path.splitext(filePath)
			if ext!=".xlsx":
				raise Exception("Wrong input file")

			self.csvFile = name + ".csv"
		else:
			sys.exit("Not existing file")



	def XlsxToCsv(self):
		"""
		:generates the .csv file at the same location with the .xlsx file
		:return: None
		"""
		print("Converting XLSX file to CSV")

		if os.path.exists(self.csvFile) and os.path.getsize(self.csvFile) > 0:
			print(self.csvFile + " already exists")
		else:
			try:
				data_xls = pd.read_excel(self.xmlFile)
				data_xls.to_csv(self.csvFile, encoding='utf-8', index = False, header=True)
			except Exception as e:
				print(e)
				sys.exit("Could not convert xlsx to csv file")