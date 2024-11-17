#!/bin/bash

# Wait 60 seconds for SQL Server to start up by ensuring that 
# calling SQLCMD does not return an error code, which will ensure that sqlcmd is accessible
# and that system and user databases return "0" which means all databases are in an "online" state
# https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-databases-transact-sql?view=sql-server-2017 

echo "configure-db.sh started"

DBSTATUS=1
ERRCODE=1
i=0

while [[ $DBSTATUS -ne 0 ]] && [[ $i -lt 60 ]] && [[ $ERRCODE -ne 0 ]]; do
	i=$i+1
	DBSTATUS=$(/opt/mssql-tools18/bin/sqlcmd -C -S mssql -h -1 -t 1 -U sa -P $MSSQL_SA_PASSWORD -Q "SET NOCOUNT ON; Select SUM(state) from sys.databases")
  echo "DBSTATUS =>>>> $DBSTATUS"
	ERRCODE=$?
  echo "ERRCODE =>>>> $ERRCODE"
  echo "sleep 1"
	sleep 1
done

if [[ $DBSTATUS -ne 0 ]] || [[ $ERRCODE -ne 0 ]]; then 
	echo "SQL Server took more than 60 seconds to start up or one or more databases are not in an ONLINE state"
	exit 1
fi

# Run the setup script to create the DB and the schema in the DB
/opt/mssql-tools18/bin/sqlcmd -C -U sa -P $MSSQL_SA_PASSWORD -d master -v MSSQL_DATABASE="$MSSQL_DATABASE" -v MSSQL_USER="$MSSQL_USER" -v MSSQL_PASSWORD="$MSSQL_PASSWORD" -i setup.sql

echo "configure-db.sh ended"