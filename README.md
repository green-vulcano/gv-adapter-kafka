# gv-adapter-kafka
### GV ESB v4 adapter for Apache Kafka

Support Apache Kafka integration providing `kafka-publish-call` to send message on Kafka topic.

#### Required attributes
 - **name**
 - **type** (=call)
 - **topic** 

#### Optional attributes 
 - **config** _path of kafka producer properties file_
 - **sync** _whether call operation must wait for brokers ACK_
 - **key**
 - **partition**

Here a sample flow:

```xml
<GVServices name="SERVICES" type="module">
        <Groups>
            <Description>This section contains all the service groups.</Description>
            <Group group-activation="on" id-group="DEFAULT_GRP"/>
        </Groups>
        <Services>
            <Description>This section contains a list of all services provided by GreenVulcano ESB</Description>
            <Service group-name="DEFAULT_GRP" id-service="stream" service-activation="on"
                     statistics="off">
                <Operation class="it.greenvulcano.gvesb.core.flow.GVFlowWF" name="publish"
                           operation-activation="on" out-check-type="none"
                           type="operation">
                    <Description>Transform a string in lower-case or upper-case depending first case</Description>
                    <Flow first-node="send" point-x="30" point-y="259">
                        <GVOperationNode class="it.greenvulcano.gvesb.core.flow.GVOperationNode"
                                         id="send" id-channel="MessageStream"
                                         id-system="GreenVulcano" input="payload"
                                         next-node-id="done" op-type="call"
                                         operation-name="notify" output="response"
                                         point-x="210" point-y="139" type="flow-node"/>
                        <GVEndNode class="it.greenvulcano.gvesb.core.flow.GVEndNode"
                                   id="done" op-type="end" output="response" point-x="360"
                                   point-y="139" type="flow-node"/>
                    </Flow>
                </Operation>
            </Service>
        </Services>
    </GVServices>
    <GVSystems name="SYSTEMS" type="module">
        <Systems>
            <Description>This section contains a list of all systems connected to GreenVulcano ESB</Description>
            <System id-system="GreenVulcano" system-activation="on">
                <Channel enabled="true"
                         endpoint="localhost:9092,localhost:9093,localhost:9094"
                         id-channel="MessageStream" type="KafkaAdapter">
                    <kafka-publish-call name="notify" type="call" 
                                        config="${{gv.app.home}}/kafka.properties" 
                                        topic="gvevolution" partition="2" key="nodeAlpha" 
                                        sync="true"/>
                    <kafka-subscription-listener name="inbound" group="consumerGroup"
                                                 service="stream" operation="consume" type="listener">
                        <topic>gvevolution</topic>
                    </kafka-subscription-listener>
                </Channel>
            </System>
        </Systems>
    </GVSystems>
```
