package com.publisherdata.PublisherData;

import engine.DataPublisherFacade;

public class App
{
    public static void main( String[] args )
    {
    	DataPublisherFacade facade = new DataPublisherFacade();

        facade.registerDataset("src/test/resources/datasets/countries.csv", "myDataset", true);

        facade.executeGlobalChecks("myDataset");
        
        //facade.executeColumnChecks("myDataset");
    }
}
