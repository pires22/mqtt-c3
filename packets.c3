module mqtt::packets;
import std::io;

struct Packet {
    Mqtt_Packet_Type type;
    DString data;
}

struct PublishPacket {
    bool dup;
    int qos;
    bool retain;
    String topic;
    int packet_id;
    String payload;
}

struct PubrelPacket {
    int packet_id;
}

enum Mqtt_Packet_Type : int (int value) {
    CONNECT = 1,
    CONNACK = 2,
    PUBLISH = 3,
    PUBACK = 4,
    PUBREC = 5,
    PUBREL = 6,
    PUBCOMP = 7,
    SUBSCRIBE = 8,
    SUBACK = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK = 11,
    PINGREQ = 12,
    PINGRESP = 13,
    UNKNOWN = -1
}