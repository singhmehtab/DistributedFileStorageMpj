import com.healthmarketscience.rmiio.*;

import java.io.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {

    public static void main(String[] args) {
        try {
            Registry registry = LocateRegistry.getRegistry(4552);
            ServiceApi server = (ServiceApi) registry.lookup("ServiceApiImpl");
            File file = new File("M:\\Interview Preperation\\Banga.mp4");
            System.out.println("Saving files");
            InputStream inputStream = new FileInputStream(file);
            RemoteInputStreamServer remoteFileData = new SimpleRemoteInputStream(inputStream);
            System.out.println(server.saveFile(remoteFileData.export(), file.length(), file.getName()));
            file = new File("M:\\Concordia\\Subjects.txt");
            inputStream = new FileInputStream(file);
            remoteFileData = new SimpleRemoteInputStream(inputStream);
            System.out.println(server.saveFile(remoteFileData.export(), file.length(), file.getName()));
            file = new File("C:\\Users\\singh\\Pictures\\Poster A2 60x30 сm.png");
            inputStream = new FileInputStream(file);
            remoteFileData = new SimpleRemoteInputStream(inputStream);
            System.out.println(server.saveFile(remoteFileData.export(), file.length(), file.getName()));
            file = new File("M:\\Concordia\\SUMMER 2022\\SOEN 6611\\9780429106224_webpdf.pdf");
            inputStream = new FileInputStream(file);
            remoteFileData = new SimpleRemoteInputStream(inputStream);
            System.out.println(server.saveFile(remoteFileData.export(), file.length(), file.getName()));
            file = new File("M:\\Concordia\\ASSIGNMENT3\\results\\BangaNew.mp4");
            System.out.println("Fetching files");
            saveFile(server.fetchFile("Banga.mp4"), file);
            file = new File("M:\\Concordia\\ASSIGNMENT3\\results\\Subjects_New.txt");
            saveFile(server.fetchFile("Subjects.txt"), file);
            file = new File("M:\\Concordia\\ASSIGNMENT3\\results\\9780429106224_webpdf_New.pdf");
            saveFile(server.fetchFile("9780429106224_webpdf.pdf"), file);
            file = new File("C:\\Users\\singh\\Pictures\\new Image.png");
            saveFile(server.fetchFile("Poster A2 60x30 сm.png"), file);
            System.out.println("Listing files");
            System.out.println(server.listFiles());
//          System.out.println(server.removeFile("Banga.mp4"));
//          System.out.println(server.listFiles());
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            System.out.println("Shutting Down client......");
        }
    }
    public static void saveFile(RemoteInputStream remoteInputStream, File file) throws IOException {
        System.out.println("Saving file " + file.getName());
        InputStream inputStream = RemoteInputStreamClient.wrap(remoteInputStream);
        try (FileOutputStream fos = new FileOutputStream(file)) {

            if (!file.exists()) {
                file.createNewFile();
            }
            fos.write(inputStream.readAllBytes());
            fos.flush();
        }
        System.out.println(file.getName() + " is saved successfully");
    }

}
