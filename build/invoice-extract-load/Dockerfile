ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-slim-buster as base

RUN apt-get update && \
  apt-get install -y curl gpg

# --- Install SQL Server ODBC Drivers ---
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
RUN curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && \
  ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

FROM base as builder

# --- Install Poetry ---
ARG POETRY_VERSION=1.7.1

ENV PYTHONBUFFERED=1 \
  POETRY_VIRTUALENVS_OPTIONS_ALWAYS_COPY=true 
ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="${VIRTUAL_ENV}/bin:/root/.local/bin:$PATH" 

RUN curl -sSL https://install.python-poetry.org | python3 - --version ${POETRY_VERSION}

# enable venv and poetry
RUN python -m venv ${VIRTUAL_ENV}

WORKDIR /app
COPY pyproject.toml poetry.lock /app/

RUN poetry install --only main --no-root

COPY invoice_extract_load /app/invoice_extract_load

RUN poetry install

ENTRYPOINT [ "poetry", "run", "pipeline" ]
