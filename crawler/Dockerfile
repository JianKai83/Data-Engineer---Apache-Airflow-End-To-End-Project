# 由於 continuumio/miniconda3:4.3.27 中的 Debian
# 版本太舊，因此改用 ubuntu 系統
FROM ubuntu:18.04

# 系統升級、安裝 python
RUN apt-get update && apt-get install python3.7 -y && apt-get install python3-pip -y

RUN mkdir /JobProject
COPY . /JobProject/
COPY ./airflow.cfg /JobProject/dataflow/airflow.cfg
WORKDIR /JobProject/

# env
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# install package
RUN pip3 install pipenv==2020.6.2
RUN pipenv install --skip-lock beautifulsoup4
RUN pipenv install --skip-lock jieba
RUN pipenv sync

# airflow
# AIRFLOW_HOME:設定 airflow 應該要被安裝在哪裏
ARG AIRFLOW_USER_HOME=/JobProject/dataflow 
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

WORKDIR /JobProject/
# genenv
RUN VERSION=RELEASE python3 genenv.py