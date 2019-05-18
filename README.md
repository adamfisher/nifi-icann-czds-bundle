# nifi-icann-czds-bundle
A NiFi custom controller service to parse the contents of an RFC 1035 compliant zone file.

This package exposes a **GetCentralZoneDataServiceFile** processor. It creates FlowFiles by downloading zone files from ICANN's Central Zone Data Service (CZDS) program.

## Getting Started

Unfortunately the `zone-file-downloader.jar` had to be directly embedded in this project at compile time until [ICANN publishes their Java client](https://github.com/icann/czds-api-client-java/issues/4) into the Maven repository. 

1. Open the `pom.xml` file in the project root and update the local repository URL to point to the location of the `repo` folder in this project.

2. Build the NAR file using:

```
mvn clean install
```

Place the NAR file inside your NiFi installation's `lib` directory.