FROM ubuntu
RUN apt-get update
RUN apt-get install -y default-jre 
RUN apt-get install -y net-tools
RUN apt install -y nano
COPY GenericNode.jar /
COPY entrypoint_client.sh /
COPY bigTest_tc.sh /
ENTRYPOINT ["/entrypoint_client.sh"]
CMD ["300"]