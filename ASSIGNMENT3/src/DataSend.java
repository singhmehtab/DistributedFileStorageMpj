import java.io.Serializable;

public class DataSend implements Serializable {

    public String function;
    public String hashId;
    public byte[] data;

    public DataSend(){};

    public DataSend(String function, String hashId, byte[] data){
        this.function = function;
        this.data = data;
        this.hashId = hashId;
    }

}
