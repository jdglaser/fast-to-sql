FROM mcr.microsoft.com/mssql/server:2017-latest

USER root

ENV SA_PASSWORD=Pass@word
ENV ACCEPT_EULA=Y

COPY setup.sql setup.sql
COPY setup.sh setup.sh
COPY entrypoint.sh entrypoint.sh

RUN chmod +x setup.sh
RUN chmod +x entrypoint.sh

CMD /bin/bash ./entrypoint.sh