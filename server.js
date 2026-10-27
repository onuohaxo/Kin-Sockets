const WebSocket = require("ws");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;

// --- Tune these to your game ---------------------------------------------
// Units are now Godot 3D units (metres), not pixels.
const TICK_MS = 50; // Broadcast moves at most 20x/sec, coalesced per tick.
const MIN_MOVE_INTERVAL_MS = 30; // Drop floods faster than ~33 moves/sec.
const MOVE_EPSILON = 0.01; // Ignore sub-centimetre jitter (metres).
const MAX_SPEED_UNITS_PER_SEC = 20; // Your fastest legit player speed. Tune this.
const MAX_POSITION = 10000; // World bound. Tune this to your map size.
const CHAT_MIN_INTERVAL_MS = 500;
const HEARTBEAT_MS = 30000;
// -------------------------------------------------------------------------

const wss = new WebSocket.Server({
  port: PORT,
  // A move/chat message should never be kilobytes. Reject oversized
  // payloads instead of parsing and broadcasting them.
  maxPayload: 4096,
});

const players = new Map(); // id -> { id, x, y, z, vx, vy, vz, rotY, lastMoveAt, lastChatAt, hasMoved }
const sockets = new Map(); // id -> ws (kept separate so player objects stay JSON-safe)
const dirtyPlayers = new Set(); // ids whose move still needs broadcasting

function publicPlayer(player) {
  return {
    id: player.id,
    x: player.x,
    y: player.y,
    z: player.z,
    vx: player.vx,
    vy: player.vy,
    vz: player.vz,
    rot_y: player.rotY,
  };
}

function broadcast(data, exclude = null) {
  // Stringify once per message, not once per recipient (the original
  // already did this well — keep it).
  const message = JSON.stringify(data);

  for (const client of wss.clients) {
    if (client !== exclude && client.readyState === WebSocket.OPEN) {
      try {
        client.send(message);
      } catch (err) {
        console.error("Send failed:", err.message);
      }
    }
  }
}

function makePlayerId() {
  // crypto randomness, and loop until unique instead of assuming
  // Math.random() never collides.
  let id;
  do {
    id = crypto.randomBytes(4).toString("hex"); // 8 hex chars, like before
  } while (players.has(id));
  return id;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

// Batched move broadcast: if a client sends 3 moves in one tick, other
// players now get 1 message (its latest position) instead of 3.
const moveTick = setInterval(() => {
  if (dirtyPlayers.size === 0) return;

  for (const playerId of dirtyPlayers) {
    const player = players.get(playerId);
    if (!player) continue;
    broadcast(
      {
        type: "move",
        id: player.id,
        x: player.x,
        y: player.y,
        z: player.z,
        vx: player.vx,
        vy: player.vy,
        vz: player.vz,
        rot_y: player.rotY,
      },
      sockets.get(playerId)
    );
  }
  dirtyPlayers.clear();
}, TICK_MS);
moveTick.unref();

// Dead-connection detection: without this, half-open clients (crashed
// app, lost Wi-Fi, Render proxy timeouts) can linger in `players`
// until TCP finally gives up.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {
      // terminate() on the next heartbeat pass will clean this up.
    }
  }
}, HEARTBEAT_MS);
heartbeat.unref();

wss.on("connection", (ws) => {
  const playerId = makePlayerId();
  ws.isAlive = true;

  const player = {
    id: playerId,
    x: 0,
    y: 0,
    z: 0,
    vx: 0,
    vy: 0,
    vz: 0,
    rotY: 0,
    lastMoveAt: Date.now(),
    lastChatAt: 0,
    hasMoved: false,
  };
  players.set(playerId, player);
  sockets.set(playerId, ws);

  console.log(`Connected: ${playerId}`);

  ws.send(
    JSON.stringify({
      type: "welcome",
      id: playerId,
      players: [...players.values()].map(publicPlayer),
    })
  );

  broadcast(
    {
      type: "player_joined",
      player: publicPlayer(player),
    },
    ws
  );

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("error", (err) => {
    console.error(`Socket error (${playerId}):`, err.message);
  });

  ws.on("message", (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch (err) {
      console.error("Bad JSON:", err.message);
      return;
    }
    if (!data || typeof data.type !== "string") return;

    const current = players.get(playerId);
    if (!current) return;

    switch (data.type) {
      case "move": {
        const now = Date.now();

        // Rate limit: a flooding client gets dropped messages, not
        // a broadcast storm for everyone else.
        if (now - current.lastMoveAt < MIN_MOVE_INTERVAL_MS) return;

        const x = Number(data.x);
        const y = Number(data.y);
        // z defaults to 0, so a 2D client sending only x/y still works.
        const z = data.z === undefined ? 0 : Number(data.z);
        // Number("abc") is NaN and the old `|| 0` turned that into a
        // teleport to (0, 0). Reject non-finite values outright.
        if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) return;

        const targetX = clamp(x, -MAX_POSITION, MAX_POSITION);
        const targetY = clamp(y, -MAX_POSITION, MAX_POSITION);
        const targetZ = clamp(z, -MAX_POSITION, MAX_POSITION);

        let finalX = targetX;
        let finalY = targetY;
        let finalZ = targetZ;

        if (current.hasMoved) {
          // Speed check in 3D: clamp impossible movement instead of
          // trusting the client. The +0.5 m slack absorbs lag/rounding.
          const dtSec = Math.max((now - current.lastMoveAt) / 1000, 0.001);
          const dx = targetX - current.x;
          const dy = targetY - current.y;
          const dz = targetZ - current.z;
          const dist = Math.hypot(dx, dy, dz);
          const maxDist = MAX_SPEED_UNITS_PER_SEC * dtSec + 0.5;
          if (dist > maxDist) {
            finalX = current.x + (dx / dist) * maxDist;
            finalY = current.y + (dy / dist) * maxDist;
            finalZ = current.z + (dz / dist) * maxDist;
          }
          // Sub-centimetre jitter: don't mark the player dirty, so the
          // tick has nothing to broadcast.
          if (Math.hypot(finalX - current.x, finalY - current.y, finalZ - current.z) < MOVE_EPSILON) {
            return;
          }
        }

        current.x = finalX;
        current.y = finalY;
        current.z = finalZ;
        // Forward velocity + yaw so Godot clients can interpolate and
        // face the right way. Extra fields are ignored by old clients.
        // Never trust data.id: the id always comes from this
        // connection (playerId), not from the client.
        current.vx = clamp(Number(data.vx) || 0, -MAX_SPEED_UNITS_PER_SEC, MAX_SPEED_UNITS_PER_SEC);
        current.vy = clamp(Number(data.vy) || 0, -MAX_SPEED_UNITS_PER_SEC, MAX_SPEED_UNITS_PER_SEC);
        current.vz = clamp(Number(data.vz) || 0, -MAX_SPEED_UNITS_PER_SEC, MAX_SPEED_UNITS_PER_SEC);
        const rotY = Number(data.rot_y);
        current.rotY = Number.isFinite(rotY) ? rotY : current.rotY;
        current.lastMoveAt = now;
        current.hasMoved = true;
        dirtyPlayers.add(playerId);
        break;
      }

      case "chat": {
        const now = Date.now();
        if (now - current.lastChatAt < CHAT_MIN_INTERVAL_MS) return;
        const messageText = String(data.message || "").trim();
        if (!messageText) return;
        current.lastChatAt = now;

        broadcast({
          type: "chat",
          id: playerId,
          message: messageText.slice(0, 200),
        });
        break;
      }

      default:
        // Unknown types are ignored, not broadcast.
        break;
    }
  });

  ws.on("close", () => {
    players.delete(playerId);
    sockets.delete(playerId);
    dirtyPlayers.delete(playerId);

    broadcast({
      type: "player_left",
      id: playerId,
    });

    console.log(`Disconnected: ${playerId}`);
  });
});

wss.on("error", (err) => {
  console.error("Server error:", err);
});

function shutdown() {
  clearInterval(moveTick);
  clearInterval(heartbeat);
  wss.close(() => process.exit(0));
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

console.log(`Running on port ${PORT}`);
