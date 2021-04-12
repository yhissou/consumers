FROM python:3.8-slim-buster

WORKDIR /consumers

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENV PYTHONPATH /consumers/

CMD [ "python", "-u",  "./src/consumers/hr_consumer.py" ]
