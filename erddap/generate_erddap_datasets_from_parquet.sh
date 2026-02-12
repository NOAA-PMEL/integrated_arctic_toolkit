# =====================================================================
# ERDDAP Datasets.xml Generator Script
# =====================================================================
# This script run ERDDAP's GenerateDatasets.xml utility to create 
# dataset configurations from parquet files
# IMPORTANT: This script must be run from the directory where
# GenerateDatasetsXml.sh is located (/home/users/zalmanek/tomcat/webapps/erddap/WEB-INF)
# Examples of usage: 
#   /home/users/zalmanek/arctic_postgres/erddap/generate_erddap_datasets.sh (using default directories)
#   /home/users/zalmanek/arctic_postgres/erddap/generate_erddap_datasets.sh /path/to/parquet/files
#   /home/users/zalmanek/arctic_postgres/erddap/generate_erddap_datasets.sh /path/to/parquet/files path/to/output
# =====================================================================
# ---------------------------------------------------------------------
# CONFIGURATION VARIABLES
# ---------------------------------------------------------------------
# Point to your specific modern JDK # Stay the same
# Manually apply Tomcat-style memory settings to this session Don't change these!
export JAVA_HOME=/home/users/zalmanek/java/jdk-25.0.2+10
export PATH=$JAVA_HOME/bin:$PATH
export JAVA_OPTS="-Xmx4G -Xms4G -Djava.awt.headless=true"


# Directory containg the parquet data files
# Use ${1} to accept it as the first command-line argument. Default set
export directory=${1:-/home/users/zalmanek/arctic_postgres/get_test_data/test_data}

# Directory where the output XML is saved to
# Use ${2} to accept it as the second command-line argument. Default set
export newloc=${2:-/home/users/zalmanek/arctic_postgres/erddap/output_xml}

# Path to ERDDAP's GenerateDatasetXml.sh script
# This matched Tomcat/webapps installation path
ERDDAP_SCRIPT="/home/users/zalmanek/apache-tomcat-10.1.52/webapps/erddap/WEB-INF/GenerateDatasetsXml.sh"

# The ERDDAP Function to Use
ERDDAP_FUNCTION="EDDTableFromParquetFiles"

# File pattern to match 
FILE_PATTERN=".*mof.*\.parquet"

# Check if the ERDDAP script exists
if [ ! -f "$ERDDAP_SCRIPT" ]; then
    echo "ERROR: GenerateDatasetsXml.sh not found at: $ERDDAP_SCRIPT"
    echo "Please update the ERDDAP_SCRIPT variable in this script."
    exit 1
fi

# Check if the data directory exists
if [ ! -d "$directory" ]; then
    echo "ERROR: Data directory not found $directory"
    echo "Please provide a valid directory path."
    exit 1
fi

# Check if output directory exists
if [ ! -d "$newloc" ]; then
    echo "ERROR: newloc directory not found $newloc"
    echo "Please provide a valid newloc directory path."
    exit 1
fi
# ---------------------------------------------------------------------3
# RUN ERDDAP GENERATOR
# ---------------------------------------------------------------------
echo "Running ERDDAP datasets.xml generator..."

# Run the ERDDAP script
# The arguments are ERDDAP function name, data directory, file pattern
# (regex), and any additional parameter
sh "$ERDDAP_SCRIPT" \
    "$ERDDAP_FUNCTION" \
    "$directory" \
    "$FILE_PATTERN" \
    $EXTRA_PARAMS \
    "" "" "" "" "" "" "" "" "" "" "" "" "" "" "" ""

# ---------------------------------------------------------------------
# MOVE OUTPUT FILE
# ---------------------------------------------------------------------
# The output file is typicall created in current directory
output_file="/home/users/zalmanek/erddapData/logs/GenerateDatasetsXml.out"

if [ -f "$output_file" ]; then
    echo ""
    echo "Moving output file to: $newloc"
    mv "$output_file" "$newloc"

    echo "SUCESS"
    echo "Output saved to $newloc/$output_file"
    echo "copy relevant dataset sections to /home/users/zalmanek/apache-tomcat-10.1.52/content/erddap/datasets.xml"
else
    echo ""
    echo "WARNING: Output file not found"

fi

