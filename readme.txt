1. Programming language and version: Java 11
2. IDE: VS Code
3. Example of how to launch server locally:
        ----- Centralized TCP membership tracking -----
    a. To start centralized membership TCP server, run the following command in terminal:
    
        java -jar GenericNode.jar kvs

    b. To start TCP server in centralized membership tracking mode and if the membership server is running on 172.17.0.3 (By default membership server runs in port 4410):

        java -jar GenericNode.jar ts 8080 172.17.0.3 

    ----- Static config file membership tracking -----

    a. Add all nodes to tmp/nodes.cfg file

    b. To start TCP server in static config file mode, run the following command in terminal:

        java -jar GenericNode.jar ts 8080 

    ----- Client -----
    a. To start client, run the following command in terminal:
        // PUT command
        java -jar GenericNode.jar tc 127.0.0.1 8080 put mD7 booLeejae8ne0lahgoos
        // GET command
        java -jar GenericNode.jar tc 127.0.0.1 8080 get mD7
        // DELETE command
        java -jar GenericNode.jar tc 127.0.0.1 8080 del mD7
        // STORE command
        java -jar GenericNode.jar tc 127.0.0.1 8080 store
        // EXIT command
        java -jar GenericNode.jar tc 127.0.0.1 8080 exit
    

4. Membership tracking methods implemented:
    a. FD
       Implemented static config file reread but nodes are not added and removed from the tmp/nodes.cfg file when a node is added or removed.
       The file is reread every 5 seconds. 

       In docker, when a TCP node is created, execute bash interactively into the container and make changes to the tmp/nodes.cfg file 
       inside the container as needed. (Nano is installed when creating docker_client and docker_server images)

    b. T

       Implemented centralized TCP membership tracking. The membership server is running on port 4410 by default.
       If starting the server in centralized membership tracking mode, add the IP address of the membership server in the command.
       In docker, to create the nodedirectory container, i.e, centralized membership server, use the DockerFile in the centralized_TCP_membership_server directory to create the image.
       After creating nodedirectory container, find the IP address of the container and add it to the runserver.sh script under TCP Server – Centralized TCP Membership Server mode command at the end.
           Example: 
                     #TCP Server – Centralized TCP Membership Server mode
                     java -jar GenericNode.jar ts 8080 172.17.0.3


       The Centralized TCP membership server is dynamic. When a TCP node is created in T mode, the server is added to the membership server.
       When a TCP node shuts down as it receives the exit command, the server is removed from the membership server. So, the membership server is dynamic.