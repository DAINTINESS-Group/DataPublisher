package engine;

public class FacadeFactory {
	
	public IDataPublisherFacade createDataPublisherFacade()
    {
        return new DataPublisherFacade();
    }

}
