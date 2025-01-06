FROM python:3.8-slim

# using root user
USER root:root

# Set env variables
ENV AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT google-cloud-platform://
ENV AIRFLOW_CONN_SLACK_CI_VARIATION_ALERT https://hooks.slack.com/services/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
ENV AIRFLOW_CONN_SLACK_F_MANUAL_NOTIFY https://hooks.slack.com/services/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
ENV AIRFLOW_HOME /root/airflow

# seting apt get
RUN apt-get update \
&& apt-get install curl -y

# gcloud install
RUN apt-get install apt-transport-https ca-certificates gnupg -y \
&& echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
| tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
| apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
&& apt-get update && apt-get install google-cloud-cli -y

# install additional packages
RUN pip3 install --upgrade pip \
&& pip3 install protobuf==3.19.5 \
&& pip3 install --upgrade google-cloud-bigquery \
&& pip3 install apache-airflow \
&& pip3 install google-cloud-dlp==3.9.2 \
&& pip3 install apache-airflow-providers-google==8.6.0 \
&& pip3 install apache-airflow-providers-ssh

# define workspace
COPY ./dags /root/airflow/dags
WORKDIR /root/airflow
RUN airflow db init

# define workspace
#CMD ["airflow db init"]