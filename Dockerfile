FROM mcr.microsoft.com/mssql/server:2019-latest

# Switch to root user to create directories and set permissions
USER root

# Create a directory to hold the initialization script
RUN mkdir -p /usr/src/app
VOLUME /var/opt/mssql

# Copy the initialization script to the container
COPY ./Db_setup/init.sql /usr/src/app

# Grant permissions to the script
RUN chmod +x /usr/src/app/init.sql


# Switch back to the mssql user
USER mssql

# Run the script after SQL Server starts
CMD /bin/bash -c '/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "YourStrongPassw0rd!" & /opt/mssql/bin/sqlservr'
#-i /usr/src/app/init.sql && fg'

