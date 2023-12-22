FROM mcr.microsoft.com/azure-sql-edge:latest

USER root

ENV SA_PASSWORD=Pass@word
ENV ACCEPT_EULA=Y

COPY ./scripts/setup.sql setup.sql
COPY ./scripts/setup.sh setup.sh
COPY ./scripts/entrypoint.sh entrypoint.sh

RUN chmod +x setup.sh
RUN chmod +x entrypoint.sh

CMD /bin/bash ./entrypoint.sh