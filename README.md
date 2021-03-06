## Description

Application for building GTFS-RT Full dataset from Pulsar TripUpdate, ServiceAlert or VehiclePosition messages. 
Final bundle will be uploaded to remote file server. Currently supported is Azure Blob Storage. 
There is an option also to save the file locally for easier debugging.

Currently application has separate logic for publishing ServiceAlerts, TripUpdates and VehiclePositions. 
- With ServiceAlerts we can just publish the latest FeedMessage since it contains the whole state
- With TripUpdates we need to cache all incoming FeedMessages which contain information about a single trip.
  - When publishing we aggregate FeedEntities from each FeedMessage and publish them under one FeedMessage 
  - We also want to filter past events from the output feed (and the cache). 
    - Currently we do this by filtering the entire FeedEntity once all events are old. We could also filter single TripUpdates once they deprecate?
 - VehiclePositions are cached using unique vehicle ID as a key
   - Vehicles that have not published data recently are filtered
   - All cached vehicle positions are published in one FeedMessage
  


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
