import mpi.MPI;
import mpi.Status;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class MpjNodes {

    static int fileNodes = 4;
    static int bufferSize=4096;
    static int extraBuffer = 4096;
    static int PORT = 4552;
    static int tag = 1;

    static HashMap<String, ArrayList<byte[]>> node1DataMap = new HashMap<>();
    static HashMap<String, ArrayList<byte[]>> node2DataMap = new HashMap<>();
    static HashMap<String, ArrayList<byte[]>> node3DataMap = new HashMap<>();
    static HashMap<String, ArrayList<byte[]>> node4DataMap = new HashMap<>();
    static HashMap<String, ArrayList<Integer>> materRecord = new HashMap<>();
    static HashMap<String,ArrayList<String>> nameHashLengthRecord = new HashMap<>();

    public static void main(String[] args) throws Exception {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        System.out.println("My Rank is " + rank + ". I have started");
        if(rank==0){
            System.out.println("Master");
            System.out.println("TRY start listening");
            ServiceApi api = new ServiceApiImpl();
            try {
                RMIServer.start(PORT);
                RMIServer.register(api);
            }
            catch(Exception e){
                System.out.println("ERR " + e.getMessage());
                e.printStackTrace();
            }
        }
        else if(rank==1){
            byte[] buffer = new byte[bufferSize+extraBuffer];
            while(true) {
                Status s = MPI.COMM_WORLD.Recv(buffer, 0, bufferSize+extraBuffer, MPI.BYTE, 0, MPI.ANY_TAG);
                DataSend recv = convertByteToDataSend(buffer);
                if(recv.function.equals("Data Save")){
                    if(node1DataMap.containsKey(recv.hashId)){
                        node1DataMap.get(recv.hashId).add((recv.data));
                    }
                    else{
                        ArrayList<byte[]> list = new ArrayList<>();
                        list.add((recv.data));
                        node1DataMap.put(recv.hashId,list);
                    }
                }
                else if(recv.function.equals("Data Fetch")){
                    ArrayList<byte[]> arrayList= node1DataMap.get(recv.hashId);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    for(byte[] bytes : arrayList){
                        outputStream.write(bytes);
                    }
                    byte[] outputBytes = outputStream.toByteArray();
                    MPI.COMM_WORLD.Send(outputBytes,0,outputBytes.length,MPI.BYTE,0,s.tag);
                }

                else if(recv.function.equals("Remove Data")){
                    node1DataMap.remove(recv.hashId);
                }
            }
        }
        else if(rank==2){
            byte[] buffer = new byte[bufferSize+extraBuffer];
            while (true) {
                Status s = MPI.COMM_WORLD.Recv(buffer, 0, bufferSize+extraBuffer, MPI.BYTE, 0, MPI.ANY_TAG);
                DataSend recv = convertByteToDataSend(buffer);
                if(recv.function.equals("Data Save")){
                    if(node2DataMap.containsKey(recv.hashId)){
                        node2DataMap.get(recv.hashId).add((recv.data));
                    }
                    else{
                        ArrayList<byte[]> list = new ArrayList<>();
                        list.add((recv.data));
                        node2DataMap.put(recv.hashId,list);
                    }
                }
                else if(recv.function.equals("Data Fetch")){
                    ArrayList<byte[]> arrayList= node2DataMap.get(recv.hashId);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    for(byte[] bytes : arrayList){
                        outputStream.write(bytes);
                    }
                    byte[] outputBytes = outputStream.toByteArray();
                    MPI.COMM_WORLD.Send(outputBytes,0,outputBytes.length,MPI.BYTE,0,s.tag);
                }
                else if(recv.function.equals("Remove Data")){
                    node2DataMap.remove(recv.hashId);
                }
            }
        }
        else if(rank==3){
            byte[] buffer = new byte[bufferSize+extraBuffer];
            while (true) {
                Status s = MPI.COMM_WORLD.Recv(buffer, 0, bufferSize+extraBuffer, MPI.BYTE, 0, MPI.ANY_TAG);
                DataSend recv = convertByteToDataSend(buffer);
                if(recv.function.equals("Data Save")){
                    if(node3DataMap.containsKey(recv.hashId)){
                        node3DataMap.get(recv.hashId).add((recv.data));
                    }
                    else{
                        ArrayList<byte[]> list = new ArrayList<>();
                        list.add((recv.data));
                        node3DataMap.put(recv.hashId,list);
                    }
                }
                else if(recv.function.equals("Data Fetch")){
                    ArrayList<byte[]> arrayList= node3DataMap.get(recv.hashId);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    for(byte[] bytes : arrayList){
                        outputStream.write(bytes);
                    }
                    byte[] outputBytes = outputStream.toByteArray();
                    MPI.COMM_WORLD.Send(outputBytes,0,outputBytes.length,MPI.BYTE,0,s.tag);
                }
                else if(recv.function.equals("Remove Data")){
                    node3DataMap.remove(recv.hashId);
                }
            }
        }
        else if(rank==4){
            byte[] buffer = new byte[bufferSize+extraBuffer];
            while (true) {
                Status s = MPI.COMM_WORLD.Recv(buffer, 0, bufferSize+extraBuffer, MPI.BYTE, 0, MPI.ANY_TAG);
                DataSend recv = convertByteToDataSend(buffer);
                if(recv.function.equals("Data Save")){
                    if(node4DataMap.containsKey(recv.hashId)){
                        node4DataMap.get(recv.hashId).add((recv.data));
                    }
                    else{
                        ArrayList<byte[]> list = new ArrayList<>();
                        list.add((recv.data));
                        node4DataMap.put(recv.hashId,list);
                    }
                }
                else if(recv.function.equals("Data Fetch")){
                    ArrayList<byte[]> arrayList= node4DataMap.get(recv.hashId);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    for(byte[] bytes : arrayList){
                        outputStream.write(bytes);
                    }
                    byte[] outputBytes = outputStream.toByteArray();
                    MPI.COMM_WORLD.Send(outputBytes,0,outputBytes.length,MPI.BYTE,0,s.tag);
                }
                else if(recv.function.equals("Remove Data")){
                    node4DataMap.remove(recv.hashId);
                }
            }
        }
        MPI.Finalize();
    }

    public static DataSend convertByteToDataSend(byte[] bytes){
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        DataSend recv = null;
        try
        {
            in = new ObjectInputStream(bis);
            Object obj = in.readObject();
            recv = (DataSend) obj;
        }
        catch(IOException | ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return recv;
    }

    public static byte[] convertDataSendtoByteArray(DataSend dataSend){
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(dataSend);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }
    static byte[] trim(byte[] bytes)
    {
        int i = bytes.length - 1;
        while (i >= 0 && bytes[i] == 0)
        {
            --i;
        }

        return Arrays.copyOf(bytes, i + 1);
    }

    public static ArrayList<Integer> getRandomNodes(long numberOfClustersRequired, long numberOfClustersInOneNode){
        ArrayList<Integer> randomNodes = new ArrayList<>();
        while (randomNodes.size()<numberOfClustersRequired){
            int random = ThreadLocalRandom.current().nextInt(1,5);
            if(!randomNodes.contains(random)){
                long clusters = numberOfClustersInOneNode;
                while ((clusters>=0 && randomNodes.size() < numberOfClustersRequired)){
                    randomNodes.add(random);
                    clusters--;
                }
            }
        }
        return randomNodes;
    }
}
