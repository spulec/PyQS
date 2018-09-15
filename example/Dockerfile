FROM python:3.6-slim-stretch
  
# install supervisord
RUN apt-get update \
&& apt-get install -y supervisor libcurl4-openssl-dev gcc libssl-dev libffi-dev python3-dev curl \
&& apt-get clean

COPY . /api
WORKDIR /api

# install requirements
RUN pip install -r requirements.txt

# expose the app port
EXPOSE 8000

ENV PYTHONPATH="$PYTHONPATH:/api"

# run supervisord
CMD ["/usr/bin/supervisord"]
