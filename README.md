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

## Updating the ICANN CZDS Client

The ICANN CZDS client is located here: https://github.com/icann/czds-api-client-java

1. Download and compile a new version of the `zonefile-downloader.jar` file and place it in the root of this project
2. Modify the following command to update the version number. This will publish the file into the local repo folder of this project:

```
mvn deploy:deploy-file -Durl=file:///D:\Projects\nifi-icann-czds-bundle\repo -Dfile="./zonefile-downloader.jar" -DgroupId="org.icann.czds" -DartifactId="zonefile-downloader" -Dpackaging=jar -Dversion="1.9.0"
```