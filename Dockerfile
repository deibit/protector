FROM python:3.9.9-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH "/usr/src/app"

RUN chmod a+x ./start.sh

CMD ["/bin/sh", "start.sh" ]