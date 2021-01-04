# test_converter.py

import pytest
import sys, os
sys.path.insert(1, '../src')
from converter import *


def test_default_initial():
	conv = Converter("../test/files/conv.xlsx")
	assert conv.csvFile == "../test/files/conv.csv"


def test_default_initial_wrong_path():
	with pytest.raises(SystemExit) as excinfo:
		conv = Converter("/files/wrong.xlsx")
	assert str(excinfo.value) == "Not existing file"


def test_default_initial_wrong_file():
	with pytest.raises(Exception) as excinfo:
		conv = Converter("../test/files/conv.xml")
	assert str(excinfo.value) == "Wrong input file"


def test_XlsxToCsv_Fail():
	conv = Converter("../test/files/conv.xlsx")
	with pytest.raises(SystemExit) as excinfo:
		conv.XlsxToCsv()
	assert str(excinfo.value) == "Could not convert xlsx to csv file"


def test_XlsxToCsv_Pass():
	conv = Converter("../test/files/valid.xlsx")
	conv.XlsxToCsv()
	conv.XlsxToCsv()
	os.remove("./files/valid.csv")
