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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

public class GenericNode {
    /**
     * @param args the command line arguments
     */

    // Using same HashMap for TCP and UDP
    static final Map<String, String> dataMap = new ConcurrentHashMap<>();
   public ConcurrentHashMap<String, String> nodeAddresses = new ConcurrentHashMap<>();

    public static Boolean sendCommanddput1(String key, String value) throws IOException {
        Boolean isAborted = false;

        // Step 1: Get all members from member directory
        // Step 2: Loop through all members and send dput1 command to each member
        // Step 3: If any member aborts, set isAborted to true and return
        try (Socket socket = new Socket("localhost", 4410);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                            out.println("dput1 " + key + " " + value);
                            out.flush();
                    }

        return isAborted;
    }
    public void loadNodeAddresses() {
        try {
            List<String> lines = Files.readAllLines(Paths.get("/tmp/nodes.cfg"));
            nodeAddresses.clear(); // Clear previous entries
            for (String line : lines) {
                String[] parts = line.split(":");
                if (parts.length == 2) {
                    nodeAddresses.put(parts[0], parts[1]); // IP as key, PORT as value
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public  void startConfigurationReloading() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(GenericNode::loadNodeAddresses, 0, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {

        if (args.length > 0) {
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
                                                Boolean isAborted  = sendCommanddput1(key, value);
                                                // if no member aborts, then do the following
                                                dataMap.put(key, value);
                                                response = "put key=" + key;
                                                out.println(response);

                                                // if any member aborts, then do the following
                                                if (isAborted) {
                                                    response = "transaction aborted";
                                                    out.println(response);
                                                }
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

                                        case "dput1":
                                            System.out.println("dput1 command received");
                                            System.out.println("Key: " + tokens[1]);
                                            System.out.println("Value: " + tokens[2]);

                                            // Step 1: Check if key is locked
                                            // Step 2: If key is locked, then send abort transaction message to leader
                                            // Step 3: If key is not locked, then send key availability message to leader
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
            

        } else {
            String msg = "GenericNode Usage:\n\n" +
                    "Client:\n" +
                    "tc <address> <port> put <key> <msg>  TCP CLIENT: Put an object into store\n" +
                    "tc <address> <port> get <key>  TCP CLIENT: Get an object from store by key\n" +
                    "tc <address> <port> del <key>  TCP CLIENT: Delete an object from store by key\n" +
                    "tc <address> <port> store  TCP CLIENT: Display object store\n" +
                    "tc <address> <port> exit  TCP CLIENT: Shutdown server\n" +
                    "Server:\n" +
                    "ts <port>  TCP SERVER: run tcp server on <port>.\n";
            System.out.println(msg);
        }
    }
}