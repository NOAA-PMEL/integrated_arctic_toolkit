#!/bin/bash
# =====================================================================
# ERDDAP Datasets.xml Generator Script - POSTGRES VERSION
# =====================================================================
# This script runs ERDDAP's GenerateDatasets.xml utility for PostgreSQL
# =====================================================================

# ---------------------------------------------------------------------
# CONFIGURATION VARIABLES
# ---------------------------------------------------------------------
export JAVA_HOME=/home/users/zalmanek/java/jdk-25.0.2+10
export PATH=$JAVA_HOME/bin:$PATH
export JAVA_OPTS="-Xmx4G -Xms4G -Djava.awt.headless=true"

# 1. Save your current directory so you can come back
PARENT_DIR=$(pwd)
# 2. Move into the ERDDAP WEB-INF directory
cd "/home/users/zalmanek/apache-tomcat-10.1.52/webapps/erddap/WEB-INF"

# Directory where the output XML is saved to
export newloc=${1:-/home/users/zalmanek/integrated_arctic_toolkit/erddap/output_xml}

ERDDAP_SCRIPT="/home/users/zalmanek/apache-tomcat-10.1.52/webapps/erddap/WEB-INF/GenerateDatasetsXml.sh"

# 1. The ERDDAP Function to Use
ERDDAP_FUNCTION="EDDTableFromDatabase"

# --- DATABASE SPECIFIC ANSWERS (The 13+ Prompts) ---
# 2. Connection URL
DB_URL="jdbc:postgresql://localhost:5432/arctic_toolkit_test"
# 3. Driver Name
DB_DRIVER="org.postgresql.Driver"
# 4. Connection Properties (usually blank)
DB_PROPS="user|erddap|password|butterfly321"
# 5. Catalog Name (Database name)
DB_CATALOG="arctic_toolkit_test"
# 6. Schema Name
DB_SCHEMA="public"
# 7. Table Name (use .* for all tables)
DB_TABLE="occurrence"
# 8. OrderBy (e.g., "time")
DB_ORDER=""
# 9. ReloadEveryMinutes - 1 week = 10,080
DB_RELOAD="10080"
# 10. infoUrl - leaving blank for now
DB_INFO_URL=""
# 11. Institution
DB_INSTITUTION="NOAA PMEL"
# 12. Summary
DB_SUMMARY="Integrated biological data from OBIS and GBIF. Right now there may be overlap and duplicates between the two data. Check data_source to see where the data originates from."
# 13. Title
DB_TITLE="Integrated OBIS and GBIF data"

# ---------------------------------------------------------------------
# RUN ERDDAP GENERATOR
# ---------------------------------------------------------------------
echo "Running ERDDAP generator for PostgreSQL..."

# 3. We pipe the arguments into the script to answer the interactive prompts
# Note: The extra "" at the end are for any trailing questions the script might ask
sh "$ERDDAP_SCRIPT" \
"$ERDDAP_FUNCTION" \
"$DB_URL" \
"$DB_DRIVER" \
"$DB_PROPS" \
"$DB_CATALOG" \
"$DB_SCHEMA" \
"$DB_TABLE" \
"$DB_ORDER" \
"$DB_RELOAD" \
"$DB_INFO_URL" \
"$DB_INSTITUTION" \
"$DB_SUMMARY" \
"$DB_TITLE" 


# 4. Move back to your original directory
cd "$PARENT_DIR"

# ---------------------------------------------------------------------
# MOVE OUTPUT FILE
# ---------------------------------------------------------------------
output_file="/home/users/zalmanek/erddapData/logs/GenerateDatasetsXml.out"

if [ -f "$output_file" ]; then
    mkdir -p "$newloc"
    mv "$output_file" "$newloc/GenerateDatasetsXml_Postgres.out"
    echo "SUCCESS: Output saved to $newloc/GenerateDatasetsXml_Postgres.out"
else
    echo "ERROR: Output file not found at $output_file"
fi