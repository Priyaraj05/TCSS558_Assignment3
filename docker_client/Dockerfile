FROM ubuntu
RUN apt-get update
RUN apt-get install -y default-jre 
RUN apt-get install -y net-tools
COPY GenericNode.jar /
COPY entrypoint_client.sh /
COPY bigTest_tc.sh /
ENTRYPOINT ["/entrypoint_client.sh"]
CMD ["300"]