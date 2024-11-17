ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-slim-buster as base

RUN apt-get update && \
  apt-get install -y curl gpg

# --- Install SQL Server ODBC Drivers ---
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
RUN curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && \
  ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

# RUN apt-get update \
#   && apt-get dist-upgrade -y \
#   && apt-get install -y --no-install-recommends \
#   build-essential=12.9 \
#   ca-certificates=20210119 \
#   git=1:2.30.2-1+deb11u2 \
#   libpq-dev=13.16-0+deb11u1 \
#   make=4.3-4.1 \
#   openssh-client=1:8.4p1-5+deb11u3 \
#   software-properties-common=0.96.20.2-2.1 \
#   && apt-get clean \
#   && rm -rf \
#   /var/lib/apt/lists/* \
#   /tmp/* \
#   /var/tmp/*

# ENV PYTHONIOENCODING=utf-8
# ENV LANG=C.UTF-8

FROM base as dbt-sqlserver

ARG DBT_SQLSERVER_VERSION=1.8.4

WORKDIR /usr/app/dbt/

RUN python -m pip install --no-cache-dir "dbt-sqlserver==${DBT_SQLSERVER_VERSION}"

COPY src/invoice-transform/packages.yml src/invoice-transform/dbt_project.yml ./

RUN dbt deps

COPY src/invoice-transform .

ENTRYPOINT [ "dbt" ]
