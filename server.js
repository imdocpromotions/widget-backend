const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const tmi = require('tmi.js');
const { KickConnection, Events } = require('kick-live-connector');
const cron = require('node-cron');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

let trackingData = {}; 
const defaultIgnored = ['imdoclive', 'botrix', 'kickbot', 'hugomcnut', 'missxss'];

// Twitch Setup
const twitchClient = new tmi.Client({ identity: { username: 'justinfan12345' }, channels: ['imdoclive'] });
twitchClient.connect().catch(console.error);

// Kick Setup
const kickClient = new KickConnection('imdoclive');
kickClient.connect().catch(console.error);

// Merged logic: Everyone goes into the same global pool
function processMessage(user) {
    user = user.toLowerCase();
    if (defaultIgnored.includes(user)) return;
    
    // Add to global count
    trackingData[user] = (trackingData[user] || 0) + 1;
    
    // Sort the entire global object
    const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
    
    // Emit to all connected widgets
    io.emit('updateLeaderboard', top3);
}

// Twitch Listener
twitchClient.on('message', (channel, tags, message, self) => {
    if (self) return;
    processMessage(tags.username);
});

// Kick Listener
kickClient.on(Events.ChatMessage, (data) => {
    processMessage(data.sender.username);
});

// Socket connection
io.on('connection', (socket) => {
    // Send current leaderboard immediately when widget connects
    const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
    socket.emit('updateLeaderboard', top3);
});

// Weekly Reset
cron.schedule('0 0 * * 1', () => { 
    trackingData = {}; 
    io.emit('updateLeaderboard', []); 
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
