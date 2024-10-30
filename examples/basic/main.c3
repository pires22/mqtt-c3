module main;
import mqtt;
import std::io;
import std::thread;

fn void my_callback(MqttClient* client, String topic, String payload, int qos) {
    io::printfn("topic: %s", topic);
    io::printfn("payload: %s", payload);
    io::printfn("qos: %d", qos);
}

fn void main() {
    String client_id = "my_client";
    String host = "localhost";
    uint port = 1883;
    MqttClient* client = mqtt::new_client(host, port, debug: true);
    client.set_callback(&my_callback);
    if (catch reason = client.connect(client_id, keep_alive: 5)) {
        io::printfn("failed to connect to mqtt: %s", reason);
        free(client);
        return;
    }
    io::printn("connected to mqtt");
    client.subscribe("test123", 1);
    // client.loop();
    client.loop_non_blocking();
    bool block = true;
    while (block) {
        std::thread::sleep_ms((ulong)1 * 1000);
        io::printn("i'm on main thread");
    }
    free(client);
}