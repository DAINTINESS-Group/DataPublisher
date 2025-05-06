package fairchecks.api;

import java.util.List;

public interface IGenericCheckWithInvalidRows extends IGenericCheck{
    public List<String> getInvalidRows();
}
