FROM ubuntu
RUN apt-get update
RUN apt-get install -y default-jre
RUN apt-get install -y net-tools
RUN apt install -y nano
COPY GenericNode.jar /
COPY runserver.sh /
COPY nodes.cfg /tmp/
ENTRYPOINT ["/runserver.sh"]