package fairchecks.api;

import java.util.List;

public interface IGenericCheckWithInvalidRows extends IGenericCheck{
    public default List<String> getInvalidRows() { return null; }

}
