module mqtt::packets::encoder;

fn DString encode_remaining_length(int length) @local {
    DString encoded_buffer;
    do {
        int encodedByte = length % 128;  // X MOD 128
        length /= 128;                   // X DIV 128
        if (length > 0) {
            encodedByte |= 128;          // Set the continuation bit
        }
        encoded_buffer.append((char)encodedByte);
    } while (length > 0);

    return encoded_buffer;
}

fn void calculate_msb_lsb(int value, char* msb, char* lsb) @local {
    *msb = ((char)value >> 8) & 0xFF;
    *lsb = (char)value & 0xFF;
}

fn char set_connect_flags(bool clean_session, bool will_flag, int will_qos, bool will_retain, bool password_flag, bool username_flag) @local {
    char connect_flags = 0;
    
    if (clean_session) {
        connect_flags |= (1 << 1);
    }
    if (will_flag) {
        connect_flags |= (1 << 2);
    }
    connect_flags |= ((char)will_qos & 0x03) << 3;
    if (will_retain) {
        connect_flags |= (1 << 5);
    }
    if (password_flag) {
        connect_flags |= (1 << 6);
    }
    if (username_flag) {
        connect_flags |= (1 << 7);
    }

    return connect_flags;
}

fn void add_length_if_not_empty(int* payload_length, String value) @local {
    if (value.len > 0) {
        *payload_length += 2;
        *payload_length += value.len;
    } 
}

fn void append_if_not_empty(DString* packet, String value) @local {
    if (value.len > 0) {
        char msb, lsb;
        calculate_msb_lsb(value.len, &msb, &lsb);
        packet.append(msb);
        packet.append(lsb);
        packet.append_chars(value);
    }
}

fn void append_int(DString* packet, int value) @local {
    char msb, lsb;
    calculate_msb_lsb(value, &msb, &lsb);
    packet.append(msb);
    packet.append(lsb);
}

fn DString encode_connect_packet(String client_id, String username, String password, bool will_retain, String will_topic, String will_message, int will_qos, bool will_flag, bool clean_session, int keep_alive) {
    // TODO: validate inputs length and value

    if (client_id.len == 0 && !clean_session) {
        client_id = "random_string";
    }
    
    DString packet = dstring::new();
    packet.append(Mqtt_Packet_Type.CONNECT.value << 4);

    int variable_header_length = 10;
    int payload_length = 0;
    add_length_if_not_empty(&payload_length, client_id);
    add_length_if_not_empty(&payload_length, username);
    add_length_if_not_empty(&payload_length, password);
    add_length_if_not_empty(&payload_length, will_topic);
    add_length_if_not_empty(&payload_length, will_message);

    int remaining_length = variable_header_length + payload_length;
    
    DString encoded_remaining_length = encode_remaining_length(remaining_length);
    packet.append_string(encoded_remaining_length);
    encoded_remaining_length.free();

    char[6] protocol_name = {0x00, 0x04, 0x4D, 0x51, 0x54, 0x54};
    foreach(c: protocol_name) {
        packet.append(c);
    }

    packet.append(0x04);
    char flags = set_connect_flags(clean_session, will_message.len > 0, will_qos, will_retain, password.len > 0 , username.len > 0);
    packet.append(flags);

    append_int(&packet, keep_alive);
    append_if_not_empty(&packet, client_id);
    append_if_not_empty(&packet, will_topic);
    append_if_not_empty(&packet, will_message);
    append_if_not_empty(&packet, username);
    append_if_not_empty(&packet, password);

    return packet.copy();
}

fn DString encode_pingreq() {
    DString packet = dstring::new();
    packet.append((char)(Mqtt_Packet_Type.PINGREQ.value << 4));
    packet.append(0x00);
    return packet.copy();
}

fn DString encode_publish_packet(String topic, String payload, int qos, bool retain = false, bool dup = false) {
    DString packet = dstring::new();

    // TODO: validate inputs

    // 1. Fixed Header
    char fixed_header = (char)Mqtt_Packet_Type.PUBLISH.value << 4;
    
    // Set QoS, DUP, and RETAIN flags
    if (dup) {
        fixed_header |= (1 << 3);
    }
    fixed_header |= ((char)qos & 0x03) << 1;
    if (retain) {
        fixed_header |= 1;
    }
    
    packet.append(fixed_header);

    int remaining_length = 2 + topic.len;
    if (qos > 0) {
        remaining_length += 2;
    }
    remaining_length += payload.len;

    DString encoded_remaining_length = encode_remaining_length(remaining_length);
    packet.append_string(encoded_remaining_length);
    encoded_remaining_length.free();

    // 2. Variable Header
    append_if_not_empty(&packet, topic);

    if (qos > 0) {
        append_int(&packet, qos);
    }

    // 3. Payload
    packet.append_chars(payload);

    return packet.copy();
}

fn DString encode_subscribe_packet(String topic, int qos, int packet_id = 1) {
    DString packet = dstring::new();

    // 1. Fixed Header
    char fixed_header = (char)((Mqtt_Packet_Type.SUBSCRIBE.value << 4) | 0x02);
    packet.append(fixed_header);

    int remaining_length = 2 + 2 + topic.len + 1; // Packet Identifier (2 bytes) + Topic length (2 bytes) + Topic + QoS (1 byte)

    DString encoded_remaining_length = encode_remaining_length(remaining_length);
    packet.append_string(encoded_remaining_length);
    encoded_remaining_length.free();

    // 2. Variable Header
    append_int(&packet, packet_id);

    // 3. Payload
    append_if_not_empty(&packet, topic);

    // 4. QoS
    packet.append(qos & 0x03); // Ensure QoS is within the 0-2 range

    return packet.copy();
}

fn DString encode_puback(int packet_id) {
    DString packet = dstring::new();
    packet.append((char)Mqtt_Packet_Type.PUBACK.value << 4);
    packet.append(0x02);

    append_int(&packet, packet_id);

    return packet.copy();
}

fn DString encode_pubrec(int packet_id) {
    DString packet = dstring::new();
    packet.append((char)Mqtt_Packet_Type.PUBREC.value << 4);
    packet.append(0x02);

    // Packet Identifier
    append_int(&packet, packet_id);

    return packet.copy();
}

fn DString encode_pubcomp(int packet_id) {
    DString packet = dstring::new();
    packet.append((char)Mqtt_Packet_Type.PUBCOMP.value << 4);
    packet.append(0x02);

    // Packet Identifier
    append_int(&packet, packet_id);

    return packet.copy();
}

fn DString encode_unsubscribe_packet(String topic, int packet_id = 2) {
    DString packet = dstring::new();

    // 1. Fixed Header with UNSUBSCRIBE type and reserved flags for QoS 1
    char fixed_header = (Mqtt_Packet_Type.UNSUBSCRIBE.value << 4) | 0x02;
    packet.append(fixed_header);

    int remaining_length = 2 + 2 + topic.len; // Packet Identifier (2 bytes) + Topic length (2 bytes) + Topic

    // Encode Remaining Length using MQTT variable-length encoding
    DString encoded_remaining_length = encode_remaining_length(remaining_length);
    packet.append_string(encoded_remaining_length);
    encoded_remaining_length.free();

    // 2. Variable Header - Packet Identifier
    append_int(&packet, packet_id);

    // 3. Payload - Topic Filter (2-byte length prefix + UTF-8 string)
    append_if_not_empty(&packet, topic);

    return packet.copy();
}