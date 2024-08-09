FROM python:3.10

# Installation de Java
RUN apt-get update && apt-get install -y default-jdk

# Définition de JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY /test_probe . 
COPY /bestmodel ./bestmodel

RUN pip install -r requirements.txt
