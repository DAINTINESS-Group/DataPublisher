package app;

import engine.FacadeFactory;
import engine.IDataPublisherFacade;

public class Main {
    public static void main(String[] args) {
    	FacadeFactory factory = new FacadeFactory(); 
        IDataPublisherFacade facade = factory.createDataPublisherFacade();

        facade.registerDataset("src/test/resources/datasets/countries.csv", "myDataset", true);
        facade.executeGlobalChecks("myDataset");
        facade.executeColumnChecks("myDataset");
    }
}
