# retail-aggregator

[![Actions Status](https://github.com/arshomeh/retail-aggregator/workflows/Build%20and%20Test/badge.svg)](https://github.com/arshomeh/retail-aggregator/actions)

### Documentation

Please check the [Wiki page](https://github.com/arshomeh/retail-aggregator/wiki) for the documentation.

### Notes

In order to run the application, you will need docker.
Or install manually MongoDB and the required python packages from the [requirements.txt](https://github.com/arshomeh/retail-aggregator/blob/main/requirements.txt) file. Also, you'll need to change the URI for the MongoDB connection in [mongoConnector.py](https://github.com/arshomeh/retail-aggregator/blob/main/src/mongoConnector.py) and [sparkAggregator.py](https://github.com/arshomeh/retail-aggregator/blob/main/src/sparkAggregator.py) files.

### Installation

Run the following command:
```sh
$ sudo docker-compose up
```
This will build your application container and will locate the necessary files under the app directory.
Also, it will pull and run the mongo image.

Then you can run:
```sh
$ sudo docker-compose run retailapp /bin/bash
```
Now you'll run inside the docker container and you will be able to execute the application.

### Execution

From the app directory navigate to the src directory and run [retailAggregator.py](https://github.com/arshomeh/retail-aggregator/blob/main/src/retailAggregator.py) by giving as an argument the path to your xlsx file.

```sh
$ cd src/
$ python retailAggregator.py ../resources/Online\ Retail.xlsx
```
Connect with Robomongo to localhost to see the results.

### Pytest

From the app directory navigate to the test directory and run the pytest command

```sh
$ cd test/
$ python -m pytest --cov=../src
```
