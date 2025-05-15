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
- Detailed report generation for each executed check in TXT or MD File.

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

With the dataset registered via our facade, we proceed with the validation checks
If we want to execute all the global checks and all the column checks:
```sh
Map<String, Boolean> globalResults = facade.executeGlobalChecks(frameName);
Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks(frameName);
```
If we want to execute a specific global check:
```sh
String checkId = "ID of the global check";
Map<String, Boolean> globalResultsForcheckId = facade.executeGlobalChecks(frameName, checkId);
```
If we want to execute a specific column check for all the columns:
```sh
String checkId = "ID of the column check";
Map<String, Map<String, List<FairCheckResult>>> columnResultsForcheckId = facade.executeColumnChecks(frameName, "all", checkId);
```

If we want to execute all column checks for a specific column:
```sh
String columnName = "the name of the column we want to check";
Map<String, Map<String, List<FairCheckResult>>> specificColumnResults = facade.executeColumnChecks(frameName, columnName, "all");
```
And, if we want to execute a specific column check in a specific column:
```sh
String columnName = "the name of the column we want to check";
String checkId = "ID of the column check";
Map<String, Map<String, List<FairCheckResult>>> specificColumnResultsForCheckId = facade.executeColumnChecks(frameName, columnName, checkId);
```
Then, to generate a report we define the folder of the output and the type of the file:
```sh
String outputFolder = "output//path//directory//"
ReportType reportTypeTXT = ReportType.TEXT;
ReportType reportTypeMD = ReportType.MARKDOWN;
```
Finally, we generate a report with the results of the checks. We can choose either Markdown or Text type:
```sh
facade.generateGlobalReport(frameName, globalResults, outputFolder + nameOfFile.md, reportTypeMD);
facade.generateColumnReport(frameName, columnResults, outputFolder + nameOfFile.md, reportTypeMD);

facade.generateGlobalReport(frameName, globalResultsForcheckId, outputFolder + nameOfFile.txt, reportTypeTXT);
facade.generateColumnReport(frameName, columnResultsForcheckId, outputFolder + nameOfFile.txt, reportTypeTXT);
facade.generateColumnReport(frameName, specificColumnResults, outputFolder + nameOfFile.md, reportTypeMD);
facade.generateColumnReport(frameName, specificColumnResultsForCheckId, outputFolder + nameOfFile.txt, reportTypeTXT);
```
  
## **Contributors**
- Themistokleia Siakavara
- Panos Vassiliadis
