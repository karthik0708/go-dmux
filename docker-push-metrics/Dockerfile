FROM python:2

WORKDIR /app

RUN pip install requests

COPY . .

CMD [ "python", "./cosmos_metrics.py" ]