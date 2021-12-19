FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch ./
COPY bot ./
COPY utils ./
COPY start.sh ./
COPY logs ./

RUN chmod a+x start.sh

ENTRYPOINT [ "start.sh" ]