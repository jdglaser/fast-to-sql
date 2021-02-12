for i in {1..2};
do 
docker exec test-db /opt/mssql-tools/bin/sqlcmd -S db -U sa -P Pass@word -q "SELECT 1;" &> /dev/null
    if [ $? -eq 0 ]
        then
        echo "SQL Server ready"
    else
        echo "Not ready yet..."
        sleep 1;
    fi
done
echo "SQL server failed to start"
exit 1