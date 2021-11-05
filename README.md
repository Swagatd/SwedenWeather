# Read the Sweden Weather Data and load into Hive table using scala and spark

## Login to Hadoop cluster using ssh and copy to HDFS location

```bash
ssh root@192.168.0.108 -p 2222
scp -P 2222 -r Data root@192.168.0.108:/home/swedenweather/data 
```

## Update input file locations and input file schema in application.conf under resource folder

## Build the jar using Maven build tool

## Upload the jar and any dependency config file to Hadoop cluster 

```bash
scp -P 2222 config-1.2.1.jar SwedenWeatherv5-1.0-SWEDENWEATHER.jar root@192.168.0.108:/home/swedenweather/code 
```

## Submit the spark jobs for presure and temperature data processing

```bash
spark-submit --master yarn --deploy-mode cluster --jars config-1.2.1.jar --class SparkMain SwedenWeatherv5-1.0-SWEDENWEATHER.jar 
```

## Push the code into Git

```bash
git init 
git add . && git commit -m "Adding the project"
git push
```