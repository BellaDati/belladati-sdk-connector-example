# BellaDati SDK Connector Example

This repository contains the sample implementation of the BellaDati SDK for custom connectors with 2 data providers:

* [RandomDataProvider](RandomDataProvider.java) - generates random data
* [PostgreDataProvider](PostgreDataProvider.java) - loads data from PostgreSQL database

Related repository is [belladati-sdk-connector-api](https://github.com/BellaDati/belladati-sdk-connector-api/) containing the API Interfaces Definitions.

## Build Instructions

A Java 7 JDK and [Apache Maven](http://maven.apache.org/) are required to build the BellaDati SDK Connector. Maven is included in most Eclipse for Java distributions.

To prepare building the SDK sample, clone [this repository](https://github.com/BellaDati/belladati-sdk-connector-example).

You will need [GnuPG and a signing key](https://docs.sonatype.org/display/Repository/How+To+Generate+PGP+Signatures+With+Maven) in order to build signed jars. If you're fine with unsigned jars, you can go to each project's `pom.xml` and remove the plugin setup for `maven-gpg-plugin`.

When you're ready, call `mvn clean install` to build this project. Maven will create a `target` directory for this repository, containing the project's jar file and other build artifacts.

## Detailed instructions

For all detailed instructions please refer to the [BellaDati SDK Connector documentation](http://support.belladati.com/techdoc/Connector+SDK).
