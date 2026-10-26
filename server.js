const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;

const wss = new WebSocket.Server({
    port: PORT
});

const players = new Map();

function broadcast(data, exclude = null) {
    const message = JSON.stringify(data);

    for (const client of wss.clients) {
        if (
            client !== exclude &&
            client.readyState === WebSocket.OPEN
        ) {
            client.send(message);
        }
    }
}

function makePlayerId() {
    return Math.random().toString(36).substring(2, 10);
}

wss.on("connection", (ws) => {

    const playerId = makePlayerId();

    players.set(playerId, {
        id: playerId,
        x: 0,
        y: 0
    });

    console.log(`Connected: ${playerId}`);

    ws.send(JSON.stringify({
        type: "welcome",
        id: playerId,
        players: [...players.values()]
    }));

    broadcast({
        type: "player_joined",
        player: players.get(playerId)
    }, ws);

    ws.on("message", (message) => {

        try {

            const data = JSON.parse(message);

            switch (data.type) {

                case "move":

                    const player = players.get(playerId);

                    if (!player) return;

                    player.x = Number(data.x) || 0;
                    player.y = Number(data.y) || 0;

                    broadcast({
                        type: "move",
                        id: playerId,
                        x: player.x,
                        y: player.y
                    }, ws);

                    break;

                case "chat":

                    broadcast({
                        type: "chat",
                        id: playerId,
                        message: String(data.message).slice(0, 200)
                    });

                    break;
            }

        } catch (err) {
            console.error(err);
        }

    });

    ws.on("close", () => {

        players.delete(playerId);

        broadcast({
            type: "player_left",
            id: playerId
        });

        console.log(`Disconnected: ${playerId}`);
    });

});

console.log(`Running on port ${PORT}`);
