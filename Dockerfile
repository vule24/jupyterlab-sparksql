# FROM --platform=linux/amd64 jupyter/all-spark-notebook:latest
FROM jupyter/all-spark-notebook:latest

USER root
RUN echo "${NB_USER} ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/${NB_USER}

# Install dependencies / Set environment variables as root here

USER ${NB_USER}

ENV JUPYTER_TOKEN 123

