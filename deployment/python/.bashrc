# Enable unbuffered Python output
export PYTHONUNBUFFERED=1

# Set Java home directory
export JAVA_HOME=/opt/java/openjdk

# Set Spark home directory
export SPARK_HOME=/opt/spark

# Update PATH to include Java and Spark binaries
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Set locale settings
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8