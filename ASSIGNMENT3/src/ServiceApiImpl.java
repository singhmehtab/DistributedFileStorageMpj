import com.healthmarketscience.rmiio.*;
import mpi.MPI;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class ServiceApiImpl implements ServiceApi{

    @Override
    public String saveFile(RemoteInputStream remoteInputStream, Long fileSize, String fileName) throws Exception {
        String hash = fileName + System.currentTimeMillis();
        if(MpjNodes.nameHashLengthRecord.containsKey(fileName)){
            removeFile(fileName);
            throw new Exception("File already exist with the same name");}
        MpjNodes.nameHashLengthRecord.put(fileName, new ArrayList<>(){{add(hash);add(String.valueOf(fileSize));}});
        long numberOfClustersRequired =  (fileSize/ MpjNodes.bufferSize);
        long numberOfClustersInOneNode = numberOfClustersRequired/MpjNodes.fileNodes;
        long numberOfNodesRequired =numberOfClustersInOneNode != 0 ? numberOfClustersRequired/numberOfClustersInOneNode : 1;
        long currentlyDataReceived = 0;
        System.out.println("nodes required " + numberOfNodesRequired);
        System.out.println("number of clusters " + numberOfClustersRequired);
        System.out.println("number of clusters in one node " + numberOfClustersInOneNode);
        System.out.println("File name is " + fileName);

        byte[] array = new byte[MpjNodes.bufferSize];
        ArrayList<Integer> randomNodes = MpjNodes.getRandomNodes(numberOfClustersRequired, numberOfClustersInOneNode);
        System.out.println("array length is " + randomNodes.size());
        System.out.println("file length is " + fileSize);
        InputStream fileInputStream = RemoteInputStreamClient.wrap(remoteInputStream);
        for(Integer i : randomNodes){
            fileInputStream.read(array);
            currentlyDataReceived+= array.length;
            if(currentlyDataReceived > fileSize) {
                removeFile(fileName);
                throw new Exception("File size greater than provided");
            }
            byte[] bytes = MpjNodes.convertDataSendtoByteArray(new DataSend("Data Save", hash, array));
            RMIServer.MPIProxy.Send(bytes,0,bytes.length,MPI.BYTE,i, MpjNodes.tag);
            MpjNodes.materRecord.putIfAbsent(hash, new ArrayList<>());
            if(MpjNodes.materRecord.containsKey(hash)){
                if(MpjNodes.materRecord.get(hash).size()==0){
                    MpjNodes.materRecord.get(hash).add(i);
                }
                else if(!Objects.equals(MpjNodes.materRecord.get(hash).get(MpjNodes.materRecord.get(hash).size() - 1), i)){
                    MpjNodes.materRecord.get(hash).add(i);
                }
            }
        }
        array = new byte[MpjNodes.bufferSize];
        while(fileInputStream.read(array)!=-1){
            array = MpjNodes.trim(array);
            currentlyDataReceived += array.length;
            if(currentlyDataReceived > fileSize) throw new Exception("File size greater than provided");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out;
            out = new ObjectOutputStream(bos);
            DataSend dataSend = new DataSend("Data Save",hash,array);
            out.writeObject(dataSend);
            byte[] bytes = bos.toByteArray();
            if(randomNodes.size()==0){
                int random = ThreadLocalRandom.current().nextInt(1,5);
                RMIServer.MPIProxy.Send(bytes,0,bytes.length,MPI.BYTE,random, MpjNodes.tag);
                MpjNodes.materRecord.put(hash,new ArrayList<>());
                MpjNodes.materRecord.get(hash).add(random);
            }
            else {
                RMIServer.MPIProxy.Send(bytes,0,bytes.length,MPI.BYTE,randomNodes.get(randomNodes.size()-1), MpjNodes.tag);
            }
            array = new byte[MpjNodes.bufferSize];
        }
        saveDataNodesDistribution();
        return "Task Done";
    }

    @Override
    public RemoteInputStream fetchFile(String fileName) throws Exception {
        System.out.println("Fetching file " + fileName);
        if(!MpjNodes.nameHashLengthRecord.containsKey(fileName))throw new Exception("File Does not exist");
        String hash = MpjNodes.nameHashLengthRecord.get(fileName).get(0);
        byte[] buffer = new byte[Integer.parseInt(MpjNodes.nameHashLengthRecord.get(fileName).get(1))];
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for(Integer i : MpjNodes.materRecord.get(hash)){
                byte[] bytes = MpjNodes.convertDataSendtoByteArray(new DataSend("Data Fetch",hash,null));
                RMIServer.MPIProxy.Send(bytes,0,bytes.length,MPI.BYTE,i, MpjNodes.tag);
                RMIServer.MPIProxy.Recv(buffer,0, Integer.parseInt(MpjNodes.nameHashLengthRecord.get(fileName).get(1)),MPI.BYTE,i,1);
                outputStream.write(MpjNodes.trim(buffer));
            }
            InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        return new SimpleRemoteInputStream(inputStream).export();
    }

    @Override
    public String listFiles(){
        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String, ArrayList<String>> map : MpjNodes.nameHashLengthRecord.entrySet()){
            builder.append(map.getKey() + "\n");
        }
        return builder.toString();
    }

    @Override
    public String removeFile(String fileName) throws IOException {
        System.out.println("Removing file " + fileName);
        String hash = MpjNodes.nameHashLengthRecord.get(fileName).get(0);
        for(Integer i : MpjNodes.materRecord.get(hash)){
            byte[] bytes = MpjNodes.convertDataSendtoByteArray(new DataSend("Remove Data",hash,null));
            RMIServer.MPIProxy.Send(bytes,0,bytes.length,MPI.BYTE,i, MpjNodes.tag);
        }
        MpjNodes.materRecord.remove(hash);
        MpjNodes.nameHashLengthRecord.remove(fileName);
        saveDataNodesDistribution();
        return "File Removed";
    }

    public void saveDataNodesDistribution() throws IOException {
           File file = new File("M:\\Concordia\\ASSIGNMENT3\\results\\Data_Record.txt");
           if(!file.exists()) file.createNewFile();
           else{
               file.delete();
               file.createNewFile();
           }
           PrintWriter fileOutputStream = new PrintWriter(file);
           for(Map.Entry<String,ArrayList<String>> mapEntry : MpjNodes.nameHashLengthRecord.entrySet()){
               fileOutputStream.write(mapEntry.getKey() + " -> ");
               String hash = mapEntry.getValue().get(0);

               for(Integer i : MpjNodes.materRecord.get(hash)){
                   fileOutputStream.write(i + ",");
               }
               fileOutputStream.write("\n");
           }
           fileOutputStream.flush();
           fileOutputStream.close();
    }
}
