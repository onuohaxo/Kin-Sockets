const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;

const wss = new WebSocket.Server({
    port: PORT
});

console.log("WebSocket server started");

wss.on("connection", (socket) => {

    console.log("Player joined");

    socket.send("Welcome to The Beyond");

    socket.on("message", (message) => {

        console.log(message.toString());

        wss.clients.forEach(client => {

            if (client.readyState === WebSocket.OPEN) {
                client.send(message.toString());
            }

        });

    });

});
