module mqtt;
import std::net::tcp;
import std::io;
import std::thread;
import mqtt::packets;
import mqtt::packets::encoder;
import mqtt::packets::decoder;

struct MqttClient {
    bool debug;
    TcpSocket socket;
    bool connected;
    int keep_alive;
    String host;
    uint port;
    MessageCallback callback;
}

struct KeepAliveThreadArgs {
    MqttClient* client;
}

def MessageCallback = fn void(MqttClient* client, String topic, String payload, int qos);

fn MqttClient* new_client(String host, uint port, bool debug = false) {
    MqttClient* client = (MqttClient*)malloc(MqttClient.sizeof);
    client.host = host;
    client.port = port;
    client.debug = debug;
    return client;
}

fn void MqttClient.set_callback(&self, MessageCallback callback) {
    if (callback == null) {
        return;
    }
    self.callback = callback;
}

fn void! MqttClient.connect(&self, String client_id = "", String username = "", String password = "", bool will_retain = false, String will_topic = "", String will_message = "", int will_qos = 0, bool will_flag = false, bool clean_session = false, int keep_alive = 0) {
    TcpSocket! client = std::net::tcp::connect(self.host, self.port, ip_protocol: std::net::IpProtocol.IPV4);
    if (catch reason = client) {
        return reason?;
    }

    DString packet = packets::encoder::encode_connect_packet(client_id, username, password, will_retain, will_topic, will_message, will_qos, will_flag, clean_session, keep_alive);
    // foreach(c: connect_packet.str_view()) {
    //     io::printf("0x%02X ", c);
    // }
    if (catch reason = client.write(packet.str_view())) {
        io::printfn("failed to publish packet: %s", reason);
        return;
    }
    packet.free();

    char[4] buf;
    DString buffer = dstring::new();
    if (try usz read_size = client.read(&buf)) {
        if (try buffer.write(&buf)) {
            packets::decoder::decode_packet(buffer);
        }
    } else {
        io::printn("failed to read from client");
        return;
    }
    self.connected = true;
    self.socket = client;
    self.keep_alive = keep_alive;

    Thread keep_alive_thread;
    KeepAliveThreadArgs* args = (KeepAliveThreadArgs*)mem::new(KeepAliveThreadArgs);
    args.client = self;

    if (catch reason = keep_alive_thread.create(&keep_alive_thread_handle, (void*)args)) {
        io::printfn("thread creation failed: %s", reason);
        mem::free(args);
    }
}

fn int keep_alive_thread_handle(void* args) @local {
    KeepAliveThreadArgs* keep_alive_args = (KeepAliveThreadArgs*)args;
    MqttClient* client = keep_alive_args.client;
    DString packet = packets::encoder::encode_pingreq();

    while (client.connected) {
        std::thread::sleep_ms((ulong)client.keep_alive * 1000);
        if (catch reason = client.send_packet(packet, Mqtt_Packet_Type.PINGREQ)) {
            io::printfn("failed to send PINGREQ: %s", reason);
            client.connected = false;
        }
    }

    packet.free();
    return 0;
}

fn void decode_received_packet(MqttClient* client, DString buffer) @local {
    Packet* packet = packets::decoder::decode_packet(buffer);
    if (packet == null) {
        io::printfn("failed to decode packet");
        return;
    }
    if (client.debug) {
        io::printfn("<-%s", packet.type);
    }
    switch (packet.type) {
        case Mqtt_Packet_Type.CONNACK:
            // TODO: check connection status here
            break;
        case Mqtt_Packet_Type.PUBLISH:
            PublishPacket* publish_packet = packets::decoder::decode_publish(packet.data);
            if (publish_packet != null) {
                if (client.callback != null) {
                    client.callback(client, publish_packet.topic, publish_packet.payload, publish_packet.qos);
                }
                if (publish_packet.qos == 1) {
                    DString puback_packet = packets::encoder::encode_puback(publish_packet.packet_id);
                    client.send_packet(puback_packet, Mqtt_Packet_Type.PUBACK)!!;
                    puback_packet.free();
                }
                if (publish_packet.qos == 2) {
                    DString pubrec_packet = packets::encoder::encode_pubrec(publish_packet.packet_id);
                    client.send_packet(pubrec_packet, Mqtt_Packet_Type.PUBREC)!!;
                    pubrec_packet.free();
                }
                free(publish_packet);
            }
            break;
        case Mqtt_Packet_Type.PUBREL:
            PubrelPacket* pubrel_packet = packets::decoder::decode_pubrel(packet.data);
            if (pubrel_packet != null) {
                DString pubcomp_packet = packets::encoder::encode_pubcomp(pubrel_packet.packet_id);
                client.send_packet(pubcomp_packet, Mqtt_Packet_Type.PUBCOMP)!!;
                free(pubrel_packet);
            }
            break;
        case Mqtt_Packet_Type.SUBACK:
            break;
        case Mqtt_Packet_Type.PINGRESP:
            break;
        case Mqtt_Packet_Type.UNKNOWN:
            break;
        default:
            break;
    }
    free(packet);
    buffer.clear();
}

fn void MqttClient.loop(&self) {
    listen(self);
}

fn int listen(void* args) @local {
    MqttClient* client = (MqttClient*)args;
    char[1024] buffer;
    DString packet = dstring::new();
    for (;;) {
        usz! read_size = client.socket.read(&buffer);
        if (catch error = read_size) {
            io::printfn("failed to read from client: %s", error);
            break;
        }
        if (read_size == 0) {
            io::printn("connection closed");
            break;
        }
        if (try packet.write(&buffer)) {
            decode_received_packet(client, packet);
        }
    }
    packet.free();
    return 0;
}

fn void MqttClient.loop_non_blocking(&self) {
    Thread listen_thread;

    if (catch reason = listen_thread.create(&listen, (void*)self)) {
        io::printfn("thread creation failed: %s", reason);
    }
}

fn void MqttClient.publish(&self, String topic, String payload, int qos = 0) {
    DString packet = packets::encoder::encode_publish_packet(topic, payload, qos);
    self.send_packet(packet, Mqtt_Packet_Type.PUBLISH)!!;
}

fn void MqttClient.subscribe(&self, String topic, int qos = 0) {
    DString packet = packets::encoder::encode_subscribe_packet(topic, qos);
    self.send_packet(packet, Mqtt_Packet_Type.SUBSCRIBE)!!;
}

fn void MqttClient.unsubscribe(&self, String topic) {
    if (topic == "") {
        return;
    }
    DString packet = packets::encoder::encode_unsubscribe_packet(topic);
    self.send_packet(packet, Mqtt_Packet_Type.UNSUBSCRIBE)!!;
}

fn void! MqttClient.send_packet(&self, DString packet, Mqtt_Packet_Type packet_type) @local {
    if (catch reason = self.socket.write(packet.str_view())) {
        return reason?;
    }
    if (self.debug) {
        io::printfn("->%s", packet_type);
    }
}
