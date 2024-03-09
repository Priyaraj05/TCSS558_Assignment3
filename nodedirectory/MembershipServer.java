import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;

public class MembershipServer {
    private static final int MEMBERSHIP_SERVER_PORT = 4410;
    private static final ConcurrentHashMap<String, Integer> nodeRegistry = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(MEMBERSHIP_SERVER_PORT)) {
            System.out.println("Membership Server started on port " + MEMBERSHIP_SERVER_PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Error starting Membership Server: " + e.getMessage());
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    String[] tokens = inputLine.split(" ");
                    String command = tokens[0];

                    switch (command) {
                        case "register":
                            handleRegistration(tokens, out);
                            break;
                        default:
                            out.println("Invalid command.");
                            break;
                    }
                }
            } catch (IOException e) {
                System.err.println("Error handling client request: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing client socket: " + e.getMessage());
                }
            }
        }

        private void handleRegistration(String[] tokens, PrintWriter out) {
            if (tokens.length < 3) {
                out.println("Invalid command. Usage: register <IP> <port>");
                return;
            }

            String ip = tokens[1];
            int port = Integer.parseInt(tokens[2]);
            nodeRegistry.put(ip, port);
            out.println("Registration successful for node at " + ip + ":" + port);
        }
    }
}

