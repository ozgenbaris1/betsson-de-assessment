FROM mcr.microsoft.com/mssql/server:2022-latest

USER root  

# Create a config directory
RUN mkdir -p /usr/config
WORKDIR /usr/config

# Bundle config source
COPY  . /usr/config

# Grant permissions for the our scripts to be executable
RUN chmod +x /usr/config/entrypoint.sh
RUN chmod +x /usr/config/configure-db.sh

USER 1000
ENTRYPOINT ["./entrypoint.sh"]
