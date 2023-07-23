#Dockerfile, Image, Container
FROM python:3.8

#Adding pages to docker
ADD . /main.py 
ADD . /configure.txt 
ADD . /db_connect.py

RUN pip install pandas requests psycopg2 

#Specify the entry command when we start container
CMD ["python","./main.py"]

