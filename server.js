const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;

const wss = new WebSocket.Server({
    port: PORT
});

console.log("WebSocket server started");

wss.on("connection", (socket) => {

    const id = Math.random().toString(36).substring(2, 10);

    socket.send(JSON.stringify({
        type: "id",
        id: id
    }));

    socket.on("message", (message) => {

        wss.clients.forEach(client => {

            if (client.readyState === WebSocket.OPEN) {
                client.send(message.toString());
            }

        });

    });

    console.log("Player joined:", id);

});
