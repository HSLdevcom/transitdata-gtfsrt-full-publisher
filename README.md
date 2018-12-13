## Description

Application for building GTFS-RT Full dataset from Pulsar TripUpdate or ServiceAlert messages. 
Final bundle will be uploaded to remote file server. Currently supported is Azure Blob Storage. 
There is an option also to save the file locally for easier debugging.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Either use released versions from public maven repository or build your own and install to local maven repository:
  - ```cd transitdata-common && mvn install```  

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container
- If you're using Azure as storage you'll need Azure storage account related info:
  - Private key is read from file. Set filepath via env variable AZURE_ACCOUNT_KEY_PATH
  - Azure Storage Account name is read from env variable AZURE_ACCOUNT_NAME 
  - Container to store the file is read from env variable AZURE_CONTAINER_NAME. Container is created if not found.


All other configuration options are configured in the [config file](src/main/resources/environment.conf)
which can also be configured externally via env variable CONFIG_PATH

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   
