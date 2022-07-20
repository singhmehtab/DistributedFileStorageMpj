import com.healthmarketscience.rmiio.RemoteInputStream;

import java.rmi.Remote;

public interface ServiceApi extends Remote {

    String saveFile(RemoteInputStream remoteInputStream, Long fileSize, String fileName) throws Exception;
    RemoteInputStream fetchFile(String fileName) throws Exception;
    String listFiles() throws Exception;
    String removeFile(String fileName) throws Exception;
}
