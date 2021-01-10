# test_retailAggregator.py

import mock
import pytest
import sys
sys.path.insert(1, '../src')
import retailAggregator

def test_init_fail():
	with pytest.raises(SystemExit) as excinfo:
		testargs = ["retailAggregator"]
		with mock.patch.object(sys, 'argv', testargs):
			with mock.patch.object(retailAggregator, "__name__", "__main__"):
				retailAggregator.init()
	assert str(excinfo.value) == "Wrong argument"


def test_init_pass():
	with mock.patch.object(retailAggregator, "main", return_value=0):
		with mock.patch.object(retailAggregator, "__name__", "__main__"):
			with mock.patch.object(retailAggregator.sys,'exit') as mock_exit:
				retailAggregator.init()

	assert mock_exit.call_args[0][0] == 0