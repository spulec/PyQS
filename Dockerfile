FROM python:3.5.1

MAINTAINER Soloman Weng "soloman.weng@simplehq.com.au"
ENV REFRESHED_AT 2016-05-16

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD requirements.txt /usr/src/app/requirements.txt
ADD development.txt /usr/src/app/development.txt
RUN pip install -r requirements.txt & pip install -r development.txt

ADD . /usr/src/app
