/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package genericnode;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.stream.Collectors;
import java.rmi.NoSuchObjectException;


interface KeyValueStore extends Remote {

    void put(String key, String value) throws RemoteException;

    String get(String key) throws RemoteException;

    void delete(String key) throws RemoteException;

    String store() throws RemoteException;

    void exit() throws RemoteException;
}

public class GenericNode extends UnicastRemoteObject implements KeyValueStore {
    /**
     * @param args the command line arguments
     */


     private static final long serialVersionUID = 1L;
     private ConcurrentHashMap<String, String> keyValueStore;
 
     public GenericNode() throws RemoteException {
         keyValueStore = new ConcurrentHashMap<>();
     } 

     @Override
    public void put(String key, String value) throws RemoteException {
        keyValueStore.put(key, value);
    }

    @Override
    public String get(String key) throws RemoteException {
        if (keyValueStore.containsKey(key)) {
            return keyValueStore.get(key);
        } else {
            return "NULL";
        }
    }

    @Override
    public void delete(String key) throws RemoteException {
        if (keyValueStore.containsKey(key)) {
            keyValueStore.remove(key);
        }
    }

    @Override
    public String store() throws RemoteException {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, String> entry : keyValueStore.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            result.append("key:").append(key).append(":").append("value:").append(value).append(":\n");
        }
        if (result.length() > 65000) {
            return "TRIMMED: " + result.substring(0, 65000);
        }
        return result.toString();
    }

    @Override
    public void exit() throws RemoteException {
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
    }

    // Using same HashMap for TCP and UDP
    static final Map<String, String> dataMap = new ConcurrentHashMap<>();
    private volatile static boolean isShutdownRequested = false;

    public static void main(String[] args) throws IOException {

        if (args.length > 0) {
            if (args[0].equals("rmis")) {
                System.out.println("RMI SERVER");
                try {
                    java.rmi.registry.LocateRegistry.createRegistry(1099);
                    GenericNode server = new GenericNode();
                    java.rmi.Naming.rebind("KeyValueStore", server);
                } catch (IOException e) {
                    System.out.println("Error initializing RMI server.");
                    
                }
            }
            if (args[0].equals("rmic")) {
                //System.out.println("RMI CLIENT");
                String addr = args[1];
                String cmd = args[2];
                String key = (args.length > 3) ? args[3] : "";
                String val = (args.length > 4) ? args[4] : "";

                try {
                    Registry registry = LocateRegistry.getRegistry(addr);
                    KeyValueStore stub = (KeyValueStore) registry.lookup("KeyValueStore");
                    switch (cmd) {
                        case "put":
                            if(val.isEmpty() || val == null)
                                System.out.println("server response:Please enter the value");
                            else{
                                stub.put(key, val);
                                System.out.println("server response: " + "put key=" + key);
                            }
                            break;
                        case "get":
                            String result = stub.get(key);
                            System.out.println("Server response: " + "get key=" + key + "get val=" + result);
                            break;
                        case "del":
                            if(!stub.get(key).equals("NULL")){
                                stub.delete(key);
                                System.out.println("Server response: " + "del key=" + key);
                            }
                            else
                                System.out.println("server response:Key does not exist");
                            break;
                        case "store":
                            String storeResult = stub.store();
                            System.out.println("Server response: ");
                            System.out.println(storeResult);
                            break;
                        case "exit":
                            System.out.println("Closing client...");
                            stub.exit();
                            break;
                        default:
                            System.out.println("Invalid command.");
                            break;
                    }
                } catch (Exception e) {
                    System.err.println("Client exception: " + e.toString());
                    e.printStackTrace();
                }
            }
            if (args[0].equals("tc")){
                String addr = args[1];
                int port = Integer.parseInt(args[2]);
                String command = Arrays.stream(args).skip(3).collect(Collectors.joining(" "));
                try (Socket socket = new Socket(addr, port);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    out.println(command); // Send the command to the server
                    out.flush();
                    String trimmed_command = command.trim();
                    String response;
                    if(trimmed_command.equals("store")) {
                        System.out.print("server response:");
                        while((response = in.readLine()) != null){
                            System.out.println(response);
                        }

                    }else{
                        response = in.readLine();
                        System.out.println("server response:"+response);

                    }

                }   catch (UnknownHostException e) {
                    System.err.println("Host not found " + addr);
                    System.exit(1);
                }   catch (IOException e) {
                    System.err.println("Couldn't get I/O for the connection to " + addr);
                    System.exit(1);
                }
            }
            if (args[0].equals("ts")) {
                System.out.println("TCP SERVER");
                int port = Integer.parseInt(args[1]);
                // insert code to start TCP server on port
                // insert code to start TCP server on port
                ExecutorService clientHandlingExecutor = Executors.newCachedThreadPool();
                // ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();                
                // ArrayList<Long> responseTimes = new ArrayList<>();
                try (ServerSocket serverSocket = new ServerSocket(port)) {
                    System.out.println("TCP Server started on port " + port);
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        clientHandlingExecutor.execute(() -> {
                            try (
                                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(
                                            new InputStreamReader(clientSocket.getInputStream()));
                                            ) {
                                String inputLine =  in.readLine();
                                String[] tokens = inputLine.split(" ");
                                String command = tokens[0].toLowerCase();
                                String key;
                                String value;
                                String response = "";

                                switch (command) {
                                    case "put":
                                        if (tokens.length == 3) {
                                            key = tokens[1];
                                            value = tokens[2];
                                            dataMap.put(key, value);
                                            response = "put key=" + key;
                                            out.println(response); // Make sure this line is there
                                        }
                                        else{
                                            response = "Please enter the value";
                                            out.println(response);
                                        }
                                        break;
                                    case "get":
                                        if (tokens.length == 2) {
                                            key = tokens[1];
                                            value = dataMap.get(key); // Use get() to retrieve the value
                                            response = "get key=" + key + " get val=" + value;
                                        } 
                                        out.println(response);
                                        break;
                                    case "del":
                                        if (tokens.length == 2) {
                                            key = tokens[1];
                                            // Check if the key exists before attempting to remove it.
                                            if (dataMap.containsKey(key)) {
                                                dataMap.remove(key);
                                                // If the key exists and is removed, format the response.
                                                response = "delete key=" + key;
                                            } else {
                                                // If the key does not exist, return an error message.
                                                response = "Key does not exist";
                                            }
                                        } 
                                        out.println(response);
                                        break;
                                    case "store":
                                        //System.out.println("Gets into store");
                                        StringBuilder sb = new StringBuilder();
                                        dataMap.forEach((k, v) -> sb.append("\nkey:").append(k)
                                                .append(":value:").append(v).append(":"));
                                        response = sb.toString();
                                        //System.out.println("Store result: " + response);
                                        if (sb.length() > 65000) {
                                            response = "TRIMMED:\n" + sb.substring(0, 65000);
                                        } else {
                                            response = sb.toString();
                                        }
                                        out.println(response); // Send the response to the client
                                        out.flush(); // Ensure the response is sent immediately
                                        break;

                                    case "exit":
                                        response = "Server shutting down....";
                                        out.println(response);
                                        System.out.println("Server Shutting down....");
                                        System.exit(0); 
                                        break;                             
                                    default:
                                        response = "Unknown command.";
                                        out.println(response);
                                        break;
                                }

                            } catch (IOException e) 
                            {
                                System.out.println("Error when receiving from client!");
                            } 
                            finally 
                            {
                              try 
                                {
                                    clientSocket.close();
                                }
                                catch (IOException e) 
                                {
                                    System.out.println("could not close client socket");
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                    
                }catch (IOException e) 
                {
                   
                    System.out.println(port+" is already in use");
                    
                } 
            }
            if (args[0].equals("uc")) {
                // System.out.println("UDP CLIENT");
                String addr = args[1];
                int sendport = Integer.parseInt(args[2]);
                int recvport = sendport + 1;
                String cmd = args[3];
                String key = (args.length > 4) ? args[4] : "";
                String val = (args.length > 5) ? args[5] : "";
                SimpleEntry<String, String> se = new SimpleEntry<String, String>(key, val);
                // insert code to make UDP client request to server at addr:send/recvport

                try (DatagramSocket clientSocket = new DatagramSocket();) {
                    InetAddress serverAddress = InetAddress.getByName(addr);
                    String reqData = cmd + " " + key + " " + val;
                    byte[] sendData = reqData.getBytes();
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, serverAddress, sendport);
                    clientSocket.send(sendPacket);

                    byte[] receiveBuffer = new byte[65535]; // theoretical limit of 65,535 bytes (8-byte header + 65,527 bytes of data) for a UDP datagram.
                    DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                    clientSocket.receive(receivePacket);

                    String received = new String(receivePacket.getData(), 0, receivePacket.getLength());
                    int newlineCount = 0;

                    for (int i = 0; i < received.length(); i++) {
                        if (received.charAt(i) == '\n') {
                            newlineCount++;
                        }
                    }

                    // System.out.println("Store command had newline of: "+newlineCount);
                    System.out.println("server response:" + received);
                    clientSocket.close();
                }

            }
            if (args[0].equals("us")) {
                System.out.println("UDP SERVER");
                int port = Integer.parseInt(args[1]);
                // insert code to start UDP server on port
                try (DatagramSocket serverReceiverSocket = new DatagramSocket(port);
                DatagramSocket serverSenderSocket = new DatagramSocket(port + 1);) {
                    
                    System.out.println("UDP Server started on port " + port);
                    // System.out.println("UDP Server started on port " + port);
                    ExecutorService executorService = Executors.newCachedThreadPool();

                    Thread receiverThread = new Thread(() -> {
                        try {

                            while (!isShutdownRequested) {
                                byte[] receiveData = new byte[1024];
                                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                                try {
                                    serverReceiverSocket.receive(receivePacket);
                                } catch (SocketException e) {
                                    if (isShutdownRequested) {
                                        break; // Break out of the loop if shutdown is requested
                                    } else {
                                        e.printStackTrace();
                                    }
                                }

                                executorService.submit(
                                        new UDPHandler(serverReceiverSocket, receivePacket, serverSenderSocket));

                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });

                    receiverThread.start();

                    while (true) {
                        // Check if shutdown is requested
                        if (isShutdownRequested) {
                            // Close the sockets and shut down the executor service
                            serverReceiverSocket.close();
                            serverSenderSocket.close();
                            executorService.shutdown();

                            receiverThread.interrupt();
                            try {
                                receiverThread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            break;
                        }
                    }

                } catch (Exception e) {
                    System.out.println(port + " is already in use");
                }

            }

        } else {
            String msg = "GenericNode Usage:\n\n" +
                    "Client:\n" +
                    "uc/tc <address> <port> put <key> <msg>  UDP/TCP CLIENT: Put an object into store\n" +
                    "uc/tc <address> <port> get <key>  UDP/TCP CLIENT: Get an object from store by key\n" +
                    "uc/tc <address> <port> del <key>  UDP/TCP CLIENT: Delete an object from store by key\n" +
                    "uc/tc <address> <port> store  UDP/TCP CLIENT: Display object store\n" +
                    "uc/tc <address> <port> exit  UDP/TCP CLIENT: Shutdown server\n" +
                    "rmic <address> put <key> <msg>  RMI CLIENT: Put an object into store\n" +
                    "rmic <address> get <key>  RMI CLIENT: Get an object from store by key\n" +
                    "rmic <address> del <key>  RMI CLIENT: Delete an object from store by key\n" +
                    "rmic <address> store  RMI CLIENT: Display object store\n" +
                    "rmic <address> exit  RMI CLIENT: Shutdown server\n\n" +
                    "Server:\n" +
                    "us/ts <port>  UDP/TCP SERVER: run udp or tcp server on <port>.\n" +
                    "rmis  run RMI Server.\n";
            System.out.println(msg);
        }
    }

    // Method for safely shutting down the server
    public static void initiateShutdown() {
        isShutdownRequested = true;
    }
}


class UDPHandler implements Runnable {
    private DatagramSocket receiverSocket;
    private DatagramSocket senderSocket;
    private DatagramPacket receivePacket;

    public UDPHandler(DatagramSocket receiverSocket, DatagramPacket packet, DatagramSocket senderSocket) {
        this.receiverSocket = receiverSocket;
        this.senderSocket = senderSocket;
        this.receivePacket = packet;
    }

    @Override
    public void run() {
        try {
            // Process the received data as needed
            String received = new String(receivePacket.getData(), 0, receivePacket.getLength());
            String msg[] = received.split(" ");

            String cmd = msg[0];
            String key = (msg.length > 1) ? msg[1] : "";
            String value = (msg.length > 2) ? msg[2] : "";

            byte[] sendData;
            DatagramPacket sendPacket;
            InetAddress clientAddress = receivePacket.getAddress();
            int clientPort = receivePacket.getPort();

            String response;

            switch (cmd) {
                case "put":
                    if(msg.length == 3){
                    // System.out.println("The value inserted:"+value);
                    GenericNode.dataMap.put(key, value);
                    response = "put key=" + key;
                    }
                    else
                        response = "Please enter the value";
                    sendData = response.getBytes();
                    sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
                    senderSocket.send(sendPacket);
                    break;
                case "get":
                    String getValue = GenericNode.dataMap.get(key);
                    response = "get key=" + key + " get val=" + getValue;
                    sendData = response.getBytes();
                    sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
                    senderSocket.send(sendPacket);
                    break;
                case "del":
                    String get = GenericNode.dataMap.get(key);
                    if(get == null){
                            // If the key does not exist, return an error message.
                            response = "Key does not exist";
                    }
                    else{
                        GenericNode.dataMap.remove(key);
                        response = "delete key=" + key;
                    }
                        sendData = response.getBytes();
                        sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
                        senderSocket.send(sendPacket);
                    break;
                case "store":
                    StringBuilder resultBuilder = new StringBuilder();
                    GenericNode.dataMap.forEach((k, v) -> {
                        resultBuilder.append("\nkey:").append(k).append(":value:").append(v).append(":");
                    });
                    response = resultBuilder.toString();
                    if (response.length() > 65000) {
                        // Truncate the output and prepend "TRIMMED:"
                        response = "\nTRIMMED:" + response.substring(0, 65000);
                    }

                    int newlineCount = 0;

                    for (int i = 0; i < response.length(); i++) {
                        if (response.charAt(i) == '\n') {
                            newlineCount++;
                        }
                    }

                    sendData = response.getBytes();
                    sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
                    senderSocket.send(sendPacket);
                    break;
                case "exit":
                    response = "Server Shutting Down....";
                    sendData = response.getBytes();
                    sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
                    senderSocket.send(sendPacket);
                    GenericNode.initiateShutdown();
                    System.out.println("Server Shutting Down....");
                    return;

                default:
                    response = "Unknown command.";
                    sendData = response.getBytes();
                    sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
                    senderSocket.send(sendPacket);
                    break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
