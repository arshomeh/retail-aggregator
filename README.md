# retail-aggregator


[![Actions Status](https://github.com/arshomeh/retail-aggregator/workflows/Build%20and%20Test/badge.svg)](https://github.com/arshomeh/retail-aggregator/actions)


### Installation

You will need docker to run.

```sh
sudo docker-compose up

sudo docker-compose run retailapp /bin/bash
```

Then you will be in the /app directory inside the docker.
Navigate to the src directory and run the main file (calculation.py)

```sh
root@292eda958eec:/app/ cd src
root@292eda958eec:/app/src/ python calculation.py ../resources/Online\ Retail.xlsx
```

To run the test you need to navigate to the test directory. Then run the pytest command

```sh
root@292eda958eec:/app/ cd test
root@292eda958eec:/app/test/ python -m pytest --cov=../src
```

The result of the tests are:

```sh
----------- coverage: platform linux, python 3.6.9-final-0 -----------
Name                                                     Stmts   Miss  Cover
----------------------------------------------------------------------------
root@292eda958eec:/app/src/converter.py            21      0   100%
root@292eda958eec:/app/src/mongoConnector.py       50      1    98%
root@292eda958eec:/app/src/sparkAggregator.py      72      5    93%
----------------------------------------------------------------------------
```
