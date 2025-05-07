<p align="center">
  <img src="./logo.jpeg" alt="Preview" width="200"/>
</p>

# <p align="center"> **Data Publisher**</p>

#### **A java application that validates datasets based on the FAIR principles (Findability, Accessibility, Interoperability, Reusability)**

Data Publisher is a Spark-based Application with the responsibility of validating the correction of the datasets. After registering a dataset, the system executes some FAIR checks and generates a report with the results.

## **Set-Up**
Choose the IDE of preference and import the project as a Maven Project. Make sure to set-up the JAVA_HOME property to the correct location of a Java 8 or above installation.

Afterwards, check the Main class inside the App package for an example on the creation, execution and report production of multiple checks.

## **Features**
- Registration of Datasets from CSV files.
- Global FAIR checks, that checks the whole file.
- Column FAIR checks, that validate per-column.
- Detailed report generation for each executed check in TXT File.

## **Run tests**
All tests are stored within the test folder. To execute all of them simply run:
```sh
./mvnw test
```
Or run individual test classes like:
<pre>Right-click on FacadeTests.java > Run As > JUnit Test</pre>

## **Usage**
Consider that you want to publish the dataset that follows this schema:

|fruit      | price | weight (g) |
|-----------|-------|------------|
|banana     | 1     | 150        |
|strawberry | 3     |            |
|kiwi       | 5.5   | 35         |
|banana     | 1     | 150        |

This dataset contains several quality issues.
With the help of our application, we can now check if this dataset is written correctly.

First, we need to register the dataset.

```sh
FacadeFactory factory = new FacadeFactory(); 
IDataPublisherFacade facade = factory.createDataPublisherFacade();

boolean hasHeader = true;
String frameName= "dataset";
facade.registerDataset("path//of//file//dataset.csv", frameName, hasHeader);
```

With the dataset registered via oir facade, we proceed with the validation checks:
```sh
Map<String, Boolean> globalResults = facade.executeGlobalChecks(frameName);
Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks(frameName);
```

Finally, we generate a report with the results of the checks:
```sh
facade.generateGlobalReport(frameName, globalResults, "output//path//directory.txt");
facade.generateColumnReport(frameName, columnResults, "output//path//directory.txt");
```
  
## **Contributors**
- Themistokleia Siakavara
- Panos Vassiliadis
