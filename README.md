# transitdata-gtfsrt-full-publisher [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-gtfsrt-full-publisher/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-gtfsrt-full-publisher/actions/workflows/test-and-build.yml)

## Description

Application for creating GTFS-RT Full dataset from GTFS-RT TripUpdate, ServiceAlert or VehiclePosition messages published in Pulsar. 
The full dataset will be then uploaded to a remote file server regularly. Currently supported is Azure Blob Storage. 
There is an option also to save the file locally for easier debugging.

Currently application has separate logic for publishing ServiceAlerts, TripUpdates and VehiclePositions. 
- With ServiceAlerts we can just publish the latest FeedMessage since it contains the whole state
- With TripUpdates we need to cache all incoming FeedMessages which contain information about a single trip.
  - When publishing we aggregate FeedEntities from each FeedMessage and publish them under one FeedMessage 
  - We also want to filter past events from the output feed (and the cache)
    - Currently we do this by filtering the entire FeedEntity once all events are old or for cancellations when the trip start time is too old 
 - VehiclePositions are cached using unique vehicle ID as a key
   - Vehicles that have not published data recently are filtered
   - All cached vehicle positions are published in one FeedMessage
   
## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Either use released versions from the GitHub Packages repository (Maven) or build your own and install to local Maven repository:
  - `cd transitdata-common && mvn install`

### Locally

- `mvn compile`
- `mvn package`  

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

### Dependencies

* Pulsar
* Azure Storage account (if publishing to Azure)
  * Azurite emulator can be used for testing locally
  
### Environment variables

#### General

* `OUTPUT_DESTINATION`: where to publish the file, either `local` or `azure`
* `DATA_TYPE`: type of the data, either `TripUpdate`, `ServiceAlert` or `VehiclePosition`
* `DUMP_INTERVAL`: interval for publishing the data
* `UNHEALTHY_TIMEOUT`: timeout when to consider the service unhealthy if no data has been published

#### Local

* `OUTPUT_LOCAL_PATH`: path where to publish the file if using local output destination

#### Azure

* `AZURE_ACCOUNT_NAME`: Azure Storage account name
* `AZURE_ACCOUNT_KEY_PATH`: path to the file that contains Azure Storage account connection string
* `CACHE_MAX_AGE`: value to use for HTTP caching header

#### Data

* `TRIP_UPDATE_MAX_AGE`: maximum age for trip update
* `TRIP_UPDATE_MAX_AGE_AFTER_START`: maximum age for trip update starting from its scheduled departure time. This option is used to filter cancellation messages which don't have any stop estimates
* `TRIP_UPDATE_TIMEZONE`: timezone used in the trip updates
* `TRIP_UPDATE_FULL_DATASET_CONTAINER`: name of the container where to publish trip updates (Blob Container or local directory)
* `TRIP_UPDATE_GOOGLE_DATASET_CONTAINER`: name of the container where to publish Google-specific trip updates
* `VEHICLE_POSITION_MAX_AGE`: maximum age for vehicle position
* `VEHICLE_POSITION_FULL_DATASET_CONTAINER`: name of the container where to publish full vehicle position feed
* `VEHICLE_POSITION_BUSTRAM_DATASET_CONTAINER`: name of the container where to publish vehicle positions for buses and trams
* `VEHICLE_POSITION_TRAINMETRO_DATASET_CONTAINER`: name of the container where to publish vehicle positions for trains and metros
* `SERVICE_ALERT_CONTAINER`: name of the container where to publish service alerts
