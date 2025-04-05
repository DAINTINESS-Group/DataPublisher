package app;

import engine.DataPublisherFacade;

public class Main {
    public static void main(String[] args) {
        DataPublisherFacade facade = new DataPublisherFacade();

        facade.registerDataset("src/test/resources/datasets/countries.csv", "myDataset", true);

        //facade.executeGlobalChecks("myDataset");
    }
}
