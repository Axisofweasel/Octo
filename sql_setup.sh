#! /bin/zsh

# Load environment variables from .env file
#!/bin/zsh

# Path to your .env file
ENV_FILE=".env"

# Check if the .env file exists
if [[ -f $ENV_FILE ]]; then
  # Read the .env file and export each variable
  while read -r line; do
    # Ignore lines starting with # or empty lines
    if [[ ! "$line" =~ ^# ]] && [[ -n "$line" ]]; then
      # Export the variable
      export $line
    fi
  done < $ENV_FILE
else
  echo "$ENV_FILE not found."
fi

docker volume create my-sql-volume

docker build --build-arg MSSQL_SA_PASSWORD=$SA_PASSWORD -t my-sql-server .

docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=$SA_PASSWORD" -p 1433:1433  --name my-sql-server --hostname sql1 -d my-sql-server 
