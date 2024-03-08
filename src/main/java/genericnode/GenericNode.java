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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericNode {
    /**
     * @param args the command line arguments
     */
    // Using same HashMap for TCP and UDP
    static final Map<String, String> dataMap = new ConcurrentHashMap<>();
    static final Map<String, String> ipAddressMap = new ConcurrentHashMap<>();
    // create a set to store all locked keys
    static final Set<String> lockedKeys = new HashSet<>();
    
    private static Boolean sendCommandDput1(String key, String value, Map<String, String> ipAddressMap)
    throws IOException {
        Boolean isAborted = false;

        // Step 1: Get all members from member directory
        // Step 2: Loop through all members and send dput1 command to each member
        // Step 3: If any member aborts, set isAborted to true and return

        ExecutorService dput1CommandExecutor = Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : ipAddressMap.entrySet()) {

            // For local testing

            String ipAddressPost = entry.getValue();
            String ipAddress = ipAddressPost.split(":")[0];
            String port = ipAddressPost.split(":")[1];

            // For testing in docker
            
            // String ipAddress = entry.getKey();
            // String port = entry.getValue();

            System.out.println("Sending dput1 command to " + ipAddress + ":" + port);

            Future<Boolean> future = dput1CommandExecutor.submit(new Dput1Handler(key, value, ipAddress, port));

            try {
                isAborted = future.get();
                if (isAborted) {
                    dput1CommandExecutor.shutdown();
                    return isAborted;
                }
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Error when getting response from " + ipAddress + ":" + port);
                // e.printStackTrace();
            }
        }

        dput1CommandExecutor.shutdown();

        return isAborted;
    }

    private static Boolean sendCommandDdel1(String key, Map<String, String> ipAddressMap)
    throws IOException {
        Boolean isAborted = false;

        ExecutorService ddel1CommandExecutor = Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : ipAddressMap.entrySet()) {

            // For local testing

            String ipAddressPost = entry.getValue();
            String ipAddress = ipAddressPost.split(":")[0];
            String port = ipAddressPost.split(":")[1];

            // For testing in docker
            
            // String ipAddress = entry.getKey();
            // String port = entry.getValue();

            System.out.println("Sending ddel1 command to " + ipAddress + ":" + port);

            Future<Boolean> future = ddel1CommandExecutor.submit(new Ddel1Handler(key, ipAddress, port));

            try {
                isAborted = future.get();
                if (isAborted) {
                    ddel1CommandExecutor.shutdown();
                    return isAborted;
                }
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Error when getting response from " + ipAddress + ":" + port);
                // e.printStackTrace();
            }
        }

        ddel1CommandExecutor.shutdown();

        return isAborted;
    }

    private static void sendCommandDputAbort(String key, String value, Map<String, String> ipAddressMap) {

        ExecutorService dputAbortCommandExecutor = Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : ipAddressMap.entrySet()) {

            // For local testing

            String ipAddressPost = entry.getValue();
            String ipAddress = ipAddressPost.split(":")[0];
            String port = ipAddressPost.split(":")[1];

            // For testing in docker

            // String ipAddress = entry.getKey();
            // String port = entry.getValue();

            System.out.println("Sending dputAbort command to " + ipAddress + ":" + port);

            dputAbortCommandExecutor.execute( () -> {
                try (
                        Socket socket = new Socket(ipAddress, Integer.parseInt(port));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    out.println("dputabort " + key + " " + value);
                    out.flush();

                    String response = in.readLine();
                    
                    System.out.println(ipAddress + ":" + port+" responsed with: " + response + " for dputabort command");

                } catch (IOException e) {
                    System.out.println("Error when sending dputabort command to " + entry.getKey());
                }
            });
            
        }

        dputAbortCommandExecutor.shutdown();
    }

    private static void sendCommandDdelAbort(String key, Map<String, String> ipAddressMap) {

        ExecutorService ddelAbortCommandExecutor = Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : ipAddressMap.entrySet()) {

            // For local testing

            String ipAddressPost = entry.getValue();
            String ipAddress = ipAddressPost.split(":")[0];
            String port = ipAddressPost.split(":")[1];

            // For testing in docker

            // String ipAddress = entry.getKey();
            // String port = entry.getValue();

            System.out.println("Sending ddelabort command to " + ipAddress + ":" + port);

            ddelAbortCommandExecutor.execute( () -> {
                try (
                        Socket socket = new Socket(ipAddress, Integer.parseInt(port));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    out.println("ddelabort " + key);
                    out.flush();

                    String response = in.readLine();
                    
                    System.out.println(ipAddress + ":" + port+" responsed with: " + response + " for ddelabort command");

                } catch (IOException e) {
                    System.out.println("Error when sending ddelabort command to " + entry.getKey());
                }
            });
            
        }

        ddelAbortCommandExecutor.shutdown();
    }

    private static void sendCommandDput2(String key, String value, Map<String, String> ipAddressMap) {

        ExecutorService dput2CommandExecutor = Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : ipAddressMap.entrySet()) {

            // For local testing

            String ipAddressPost = entry.getValue();
            String ipAddress = ipAddressPost.split(":")[0];
            String port = ipAddressPost.split(":")[1];

            // For testing in docker

            // String ipAddress = entry.getKey();
            // String port = entry.getValue();

            System.out.println("Sending dput2 command to " + ipAddress + ":" + port);

            dput2CommandExecutor.execute( () -> {
                try (
                        Socket socket = new Socket(ipAddress, Integer.parseInt(port));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    out.println("dput2 " + key + " " + value);
                    out.flush();

                    String response = in.readLine();
                    
                    System.out.println(ipAddress + ":" + port+" responsed with: " + response + " for dput2 command");

                } catch (IOException e) {
                    System.out.println("Error when sending dput2 command to " + entry.getKey());
                }
            });
        }

        dput2CommandExecutor.shutdown();
    }

    private static void sendCommandDdel2(String key, Map<String, String> ipAddressMap) {

        ExecutorService ddel2CommandExecutor = Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : ipAddressMap.entrySet()) {

            // For local testing

            String ipAddressPost = entry.getValue();
            String ipAddress = ipAddressPost.split(":")[0];
            String port = ipAddressPost.split(":")[1];

            // For testing in docker

            // String ipAddress = entry.getKey();
            // String port = entry.getValue();

            System.out.println("Sending ddel2 command to " + ipAddress + ":" + port);

            ddel2CommandExecutor.execute( () -> {
                try (
                        Socket socket = new Socket(ipAddress, Integer.parseInt(port));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    out.println("ddel2 " + key);
                    out.flush();

                    String response = in.readLine();
                    
                    System.out.println(ipAddress + ":" + port+" responsed with: " + response + " for ddel2 command");

                } catch (IOException e) {
                    System.out.println("Error when sending ddel2 command to " + entry.getKey());
                }
            });
        }

        ddel2CommandExecutor.shutdown();
    }

    private static void loadNodeAddresses() {
        try {
            List<String> lines = Files.readAllLines(Paths.get(Path.of("").toAbsolutePath().toString()+"/tmp/nodes.cfg"));
            ipAddressMap.clear(); // Clear previous entries

            // For testing in docker

            // for (String line : lines) {
            //     String[] parts = line.split(":");
            //     if (parts.length == 2) {
            //         ipAddressMap.put(parts[0], parts[1]); // IP as key, PORT as value
            //     }

            // For local testing

            for (int i = 0; i < lines.size(); i++) {
                    ipAddressMap.put("node"+i, lines.get(i));
                }
                    
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void startConfigurationReloading() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(GenericNode::loadNodeAddresses, 0, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {
        
        if (args.length > 0) {
            if (args[0].equals("tc")) {
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
                    if (trimmed_command.equals("store")) {
                        System.out.print("server response:");
                        while ((response = in.readLine()) != null) {
                            System.out.println(response);
                        }

                    } else {
                        response = in.readLine();
                        System.out.println("server response:" + response);

                    }

                } catch (UnknownHostException e) {
                    System.err.println("Host not found " + addr);
                    System.exit(1);
                } catch (IOException e) {
                    System.err.println("Couldn't get I/O for the connection to " + addr);
                    System.exit(1);
                }
            }
            if (args[0].equals("ts")) {
                startConfigurationReloading();
                System.out.println("TCP SERVER");
                int port = Integer.parseInt(args[1]);

                ExecutorService clientHandlingExecutor = Executors.newCachedThreadPool();
                try (ServerSocket serverSocket = new ServerSocket(port)) {
                    System.out.println("TCP Server started on port " + port);
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        clientHandlingExecutor.execute(() -> {
                            try (
                                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(
                                            new InputStreamReader(clientSocket.getInputStream()));) {
                                String inputLine = in.readLine();
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
                                            Boolean isAborted = sendCommandDput1(key, value, ipAddressMap);
                                            // if any member aborts, then do the following
                                            if (isAborted) {
                                                sendCommandDputAbort(key, value, ipAddressMap);
                                                response = "transaction aborted";
                                                out.println(response);
                                            } else {
                                                sendCommandDput2(key, value, ipAddressMap);
                                                dataMap.put(key, value);
                                                response = "put key=" + key;
                                                out.println(response);
                                            }

                                        } else {
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
                                                Boolean isAborted = sendCommandDdel1(key, ipAddressMap);

                                                if (isAborted) {
                                                    sendCommandDdelAbort(key, ipAddressMap);
                                                    response = "transaction aborted";
                                                    out.println(response);
                                                } else {
                                                    sendCommandDdel2(key, ipAddressMap);
                                                    dataMap.remove(key);
                                                    response = "delete key=" + key;
                                                    out.println(response);
                                                }
                                                
                                            } else {
                                                // If the key does not exist, return an error message.
                                                response = "Key does not exist";
                                            }
                                        }
                                        out.println(response);
                                        break;
                                    case "store":
                                        // System.out.println("Gets into store");
                                        StringBuilder sb = new StringBuilder();
                                        dataMap.forEach((k, v) -> sb.append("\nkey:").append(k)
                                                .append(":value:").append(v).append(":"));
                                        response = sb.toString();
                                        // System.out.println("Store result: " + response);
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

                                        key = tokens[1];
                                        value = tokens[2];

                                        // Step 1: Check if key is locked
                                        // Step 2: If key is locked, then send abort transaction message to leader
                                        // Step 3: If key is not locked, then send key availability message to leader

                                        if (lockedKeys.contains(key)) {
                                            response = "Abort";
                                            out.println(response);
                                        } else {
                                            lockedKeys.add(key);
                                            response = "OK";
                                            out.println(response);
                                        }

                                        break;

                                    case "dputabort":
                                        System.out.println("dputabort command received");
                                        key = tokens[1];
                                        value = tokens[2];

                                        lockedKeys.remove(key);
                                        response = "Aborted";
                                        out.println(response);
                                        break;

                                    case "dput2":
                                        System.out.println("dput2 command received");
                                        key = tokens[1];
                                        value = tokens[2];

                                        dataMap.put(key, value);
                                        lockedKeys.remove(key);
                                        response = "Value stored";
                                        out.println(response);
                                        break;
                                    case "ddel1":
                                        System.out.println("ddel1 command received");
                                        System.out.println("Key: " + tokens[1]);

                                        key = tokens[1];

                                        // Step 1: Check if key is locked
                                        // Step 2: If key is locked, then send abort transaction message to leader
                                        // Step 3: If key is not locked, then send key availability message to leader

                                        if (lockedKeys.contains(key)) {
                                            response = "Abort";
                                            out.println(response);
                                        } else {
                                            lockedKeys.remove(key);
                                            response = "OK";
                                            out.println(response);
                                        }

                                        break;

                                    case "ddelabort":
                                        System.out.println("ddelabort command received");
                                        key = tokens[1];

                                        lockedKeys.remove(key);
                                        response = "Aborted";
                                        out.println(response);
                                        break;

                                    case "ddel2":
                                        System.out.println("ddel2 command received");
                                        key = tokens[1];

                                        dataMap.remove(key);
                                        lockedKeys.remove(key);
                                        response = "Value deleted";
                                        out.println(response);
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

                            } catch (IOException e) {
                                System.out.println("Error when receiving from client!");
                            } finally {
                                try {
                                    clientSocket.close();
                                } catch (IOException e) {
                                    System.out.println("could not close client socket");
                                    e.printStackTrace();
                                }
                            }
                        });
                    }

                } catch (IOException e) {

                    System.out.println(port + " is already in use");

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

class Dput1Handler implements Callable<Boolean> {
    private String key;
    private String value;
    private String ipAddress;
    private String port;
    Boolean isAborted = false;

    public Dput1Handler(String key, String value, String ipAddress, String port) {
        this.key = key;
        this.value = value;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    @Override
    public Boolean call() {
        try (
                Socket socket = new Socket(ipAddress, Integer.parseInt(port));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("dput1 " + key + " " + value);
            out.flush();

            String response = in.readLine();

            if (response.equals("OK")) {
                System.out.println("dput1 command OK recieved from " + ipAddress + ":" + port);
            } else {
                System.out.println("Aborting dput1 message from " + ipAddress + ":" + port);
                isAborted = true;
            }
        } catch (IOException e) {
            System.out.println("Error when sending dput1 command to " + ipAddress + ":" + port);
        }

        return isAborted;
    }
}

class Ddel1Handler implements Callable<Boolean> {
    private String key;
    private String value;
    private String ipAddress;
    private String port;
    Boolean isAborted = false;

    public Ddel1Handler(String key, String ipAddress, String port) {
        this.key = key;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    @Override
    public Boolean call() {
        try (
                Socket socket = new Socket(ipAddress, Integer.parseInt(port));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("ddel1 " + key + " " + value);
            out.flush();

            String response = in.readLine();

            if (response.equals("OK")) {
                System.out.println("ddel1 command OK recieved from " + ipAddress + ":" + port);
            } else {
                System.out.println("Aborting ddel1 message from " + ipAddress + ":" + port);
                isAborted = true;
            }
        } catch (IOException e) {
            System.out.println("Error when sending ddel1 command to " + ipAddress + ":" + port);
        }

        return isAborted;
    }
}
