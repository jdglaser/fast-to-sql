#!/bin/bash

for i in {1..25};
do 
docker exec test-db /opt/mssql-tools/bin/sqlcmd -S db -U sa -P Pass@word -q "SELECT 1;" &> /dev/null
    if [ $? -eq 0 ]
        then
            echo "SQL Server ready"
            exit 0
        else
            echo "Not ready yet..."
            sleep 2;
    fi
done

docker exec test-db /opt/mssql-tools/bin/sqlcmd -S db -U sa -P Pass@word -q "SELECT 1;" &> /dev/null
if [ $? -eq 0]
    then
        echo "SQL Server ready"
        exit 0
    else
        echo "SQL Server failed to start"
        exit 1
fi