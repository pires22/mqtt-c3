module mqtt::packets::decoder;
import std::io;

fn Packet* decode_packet(DString packet) {
    int pos = 0;

    char packet_type = packet.str_view()[pos] >> 4;
    char flags = packet.str_view()[pos] & 0x0F;
    pos += 1;

    int remaining_length = 0;
    int multiplier = 1;
    char encoded_byte;

    do {
        encoded_byte = packet.str_view()[pos++];
        remaining_length += (encoded_byte & 127) * multiplier;
        multiplier *= 128;
    } while ((encoded_byte & 128) != 0);

    Packet* decoded_packet = mem::new(Packet);

    switch (packet_type) {
        case 0x02: // CONNACK
            decoded_packet.type = Mqtt_Packet_Type.CONNACK;
            decoded_packet.data = packet;
            break;
        case 0x03: // PUBLISH
            decoded_packet.type = Mqtt_Packet_Type.PUBLISH;
            decoded_packet.data = packet;
            break;
        case 0x04: // PUBACK
            decoded_packet.type = Mqtt_Packet_Type.PUBACK;
            decoded_packet.data = packet;
            break;
        case 0x06: // PUBREL
            decoded_packet.type = Mqtt_Packet_Type.PUBREL;
            decoded_packet.data = packet;
            break;
        case 0x09: // SUBACK
            decoded_packet.type = Mqtt_Packet_Type.SUBACK;
            decoded_packet.data = packet;
            break;
        case 0x0B: // UNSUBACK
            decoded_packet.type = Mqtt_Packet_Type.UNSUBACK;
            decoded_packet.data = packet;
            break;
        case 0xD: // PINGRESP
            decoded_packet.type = Mqtt_Packet_Type.PINGRESP;
            decoded_packet.data = packet;
            break;
        default:
            decoded_packet.type = Mqtt_Packet_Type.UNKNOWN;
            io::printfn("unknown packet type or unsupported packet type for decoding: 0x%02X", packet_type);
            break;
    }
    return decoded_packet;
}

fn void decode_connack(DString packet) {
    int pos = 2;

    char ack_flags = packet.str_view()[pos++];
    char return_code = packet.str_view()[pos];

    io::printfn("session present: %d", (ack_flags & 0x01));

    if (return_code == 0x00) {
        io::printn("connection accepted");
    } else {
        io::printfn("connection refused, return code: 0x%02X", return_code);
    }
}

fn PublishPacket* decode_publish(DString packet) {
    int pos = 0;

    // 1. Fixed Header
    char fixed_header = packet.str_view()[pos++];
    int packet_type = (fixed_header >> 4) & 0x0F;
    if (packet_type != 0x03) {
        io::printn("error: not a PUBLISH packet");
        return null;
    }

    PublishPacket* decoded_packet = mem::new(PublishPacket);

    bool dup = (fixed_header & 0x08) != 0;
    int qos = (fixed_header >> 1) & 0x03;
    bool retain = (fixed_header & 0x01) != 0;
    decoded_packet.dup = dup;
    decoded_packet.qos = qos;
    decoded_packet.retain = retain;

    // 2. Remaining Length (decode variable-length integer)
    int remaining_length = 0;
    int multiplier = 1;
    char encoded_byte;
    do {
        encoded_byte = packet.str_view()[pos++];
        remaining_length += (encoded_byte & 127) * multiplier;
        multiplier *= 128;
    } while ((encoded_byte & 128) != 0);

    // 3. Variable Header - Topic Name
    char topic_len_msb = packet.str_view()[pos++];
    char topic_len_lsb = packet.str_view()[pos++];
    int topic_length = (topic_len_msb << 8) | topic_len_lsb;

    String topic = packet.str_view()[pos:topic_length];
    pos += topic_length;

    decoded_packet.topic = topic;

    // Packet Identifier (if QoS > 0)
    int packet_id = 0;
    if (qos > 0) {
        char packet_id_msb = packet.str_view()[pos++];
        char packet_id_lsb = packet.str_view()[pos++];
        packet_id = (packet_id_msb << 8) | packet_id_lsb;
        decoded_packet.packet_id = packet_id;
    }

    // 4. Payload
    String payload = packet.str_view()[pos:remaining_length - (topic_length + 2 + (qos > 0 ? 2 : 0))];
    decoded_packet.payload = payload;

    return decoded_packet;
}

fn PubrelPacket* decode_pubrel(DString packet) {
    int pos = 0;

    PubrelPacket* decoded_packet = mem::new(PubrelPacket);

    // 1. Fixed Header
    char fixed_header = packet.str_view()[pos++];
    int packet_type = (fixed_header >> 4) & 0x0F;

    if (packet_type != (char)Mqtt_Packet_Type.PUBREL.value) {
        io::printn("error: not a PUBREL packet");
        return null;
    }

    int remaining_length = packet.str_view()[pos++];
    if (remaining_length != 0x02) {
        io::printn("error: incorrect remaining length for PUBREL packet");
        return null;
    }

    // 2. Variable Header - Packet Identifier
    char packet_id_msb = packet.str_view()[pos++];
    char packet_id_lsb = packet.str_view()[pos++];
    int packet_id = (packet_id_msb << 8) | packet_id_lsb;

    decoded_packet.packet_id = packet_id;

    return decoded_packet;
}