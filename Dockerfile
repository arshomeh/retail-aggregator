FROM python:3.7

RUN apt-get update
RUN apt-get install default-jdk -y

WORKDIR /app

RUN apt install nano

ADD requirements.txt  .
RUN pip3 install -r requirements.txt

ADD ./resources/*.xlsx ./resources/
ADD ./src/*.py ./src/
ADD ./test/*.py ./test/
ADD ./test/files/* ./test/files/
