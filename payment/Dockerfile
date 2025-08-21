FROM python:3.9

EXPOSE 8080
WORKDIR /opt/server

# copy only requirements first (better cache)
COPY requirements.txt .

# install python dependencies
RUN pip3 install -r requirements.txt && pip3 install uwsgi

# then copy rest of the files
COPY *.py .
COPY payment.ini .

# start application
CMD ["uwsgi", "--ini", "payment.ini"]
