# Luzzu - A Quality Assessment Framework for Linked Open Datasets

Luzzu is a Quality Assessment Framework for Linked Open Datasets. It is a generic framework based on the Dataset Quality Ontology (daQ), allowing users to define their own quality metrics. Luzzu is an integrated platform that:
- assesses Linked Data quality using a library of generic and user-provided domain specific quality metrics in a scalable manner;
- provides queryable quality metadata on the assessed datasets;
- assembles detailed quality reports on assessed datasets.

Furthermore, the infrastructure:
- scales for the assessment of big datasets;
- can be easily extended by the users by creating their custom and domain-specific pluggable metrics, either by employing a novel declarative quality metric specification language or conventional imperative plugins;
- employs a comprehensive ontology framework for representing and exchanging all quality related information in the assessment workflow;
- implements quality-driven dataset ranking algorithms facilitating use-case driven discovery and retrieval.

More information regarding the framework can be found at our website (https://Luzzu.github.io/Framework)

Luzzu can either be downloaded as a single distribution file (see section Downloading the Framework or build the whole Luzzu framework (see section Building Luzzu)

## Downloading the Framework

## Building the Framework

### Pre-requisites
In order to run Luzzu, you need to have Java 1.8 installed together with maven. Maven will take care of all required dependencies.

In version 4.0 we have added support to RDF HDT files, however, this library has to be downloaded and built manually:
  1. Open your command line.
  2. `git clone https://github.com/rdfhdt/hdt-java.git`
  3. `cd hdt-java`
  4. `mvn clean install`

### Building Luzzu
You can build Luzzu using maven or download the [](latest distribution) which also includes a set of quality metrics.

Steps to build Luzzu:
  1. Clone this repository: `git clone https://github.com/Luzzu/Framework.git`
  2. Navigate to the Luzzu's framework directory: `cd Framework`
  3. Change settings from `luzzu-lowlevel-operations/src/main/resources/properties/luzzu.properties` (Note: these can also be changed later)
  4. Build the framework: `mvn clean install`

### Preparing to use Luzzu for the first time
We provide a set of quality metrics that can be downloaded and used within Luzzu.
  1. Intrinsic Metrics: [Download](http://s001.adaptcentre.ie/FrameworkMetrics/LDMetrics/intrinsic.zip)
  2. Representational Metrics: [Download](http://s001.adaptcentre.ie/FrameworkMetrics/LDMetrics/representational.zip)
  3. Contextual Metrics: [Download](http://s001.adaptcentre.ie/FrameworkMetrics/LDMetrics/contextual.zip)

The downloaded files should be uncompressed and be placed in the `luzzu-communications\externals\metrics\` folder.

Furthermore, in order for these metrics to work, you would need to download the vocabularies as well from [here](http://s001.adaptcentre.ie/FrameworkMetrics/Vocabs/). These should be uncompressed, and the ttl files should be placed in the `luzzu-communications\externals\vocabs\` folder. The `dqm.zip` contain all semantic definitions for the quality metrics in the previous section.

The source code of these metrics are also Open Source and could be found in [the Luzzu Linked Data quality metrics GitHub repository](https://github.com/Luzzu/LDqualitymetrics). To create your own metrics we refer to the Luzzu's Framework [wiki page] ().

Details on these metrics can be found in [Debattista et al. - Evaluating the Quality of the LOD Cloud: An Empirical Investigation](http://www.semantic-web-journal.net/content/evaluating-quality-lod-cloud-empirical-investigation-1)

### Executing the Application
In order to start Luzzu, you can use the provided `start.sh` script. Don't forget that each shell script requires permission to run (`chmod +x start.sh`). If you enabled the Web UI, you should now be able to navigate to [the Luzzu Web UI (experimental)](http://localhost:8080/). The source code for the UI can be found in [this repository](https://github.com/Luzzu/webapp).

Luzzu has a number of APIs. We refer the user to the [API wiki page](https://github.com/Luzzu/Framework/wiki/Restful-APIs). You could also be able to navigate to [http://localhost:8080/Luzzu/application.wadl](http://localhost:8080/Luzzu/application.wadl) and view a simplified Web Application Description Language (WADL) descriptior for the application with user and core resources only.

To get full WADL with extended resources use the query parameter detail e.g [http://localhost:8080/Luzzu/application.wadl?detail=true](http://localhost:8080/Luzzu/application.wadl?detail=true)

## Citing Luzzu and Quality metrics
If you are using Luzzu, please cite as follows:
```
Debattista, J., Auer, S., Lange, C.: Luzzu – a methodology and framework for Linked Data quality assessment. Data and Information Quality, 8(1), Oct. 2016.
```

If you are also using the quality metrics, please cite as follows:
```
Debattista, J., Lange, C., Auer, S., Cortis, D.: Evaluating the quality of the LOD Cloud: An empirical investigation. Semantic Web 9(6): 859-901 (2018)
```
