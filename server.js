const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const tmi = require('tmi.js');
const { KickConnection, Events } = require('kick-live-connector'); // New import
const cron = require('node-cron');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

let trackingData = {}; 
const defaultIgnored = ['imdoclive', 'botrix', 'kickbot', 'hugomcnut', 'missxss'];

// Twitch Setup
const twitchClient = new tmi.Client({ identity: { username: 'justinfan12345' }, channels: [] });
twitchClient.connect().catch(console.error);

// Kick Setup (Replace 'imdoclive' with your exact Kick username)
const kickClient = new KickConnection('imdoclive');
kickClient.connect().catch(console.error);

// Shared logic to update leaderboard
function processMessage(user, chan) {
    user = user.toLowerCase();
    chan = chan.toLowerCase();
    if (defaultIgnored.includes(user)) return;
    
    if (!trackingData[chan]) trackingData[chan] = {};
    trackingData[chan][user] = (trackingData[chan][user] || 0) + 1;
    
    const top3 = Object.entries(trackingData[chan]).sort(([, a], [, b]) => b - a).slice(0, 3);
    io.to(chan).emit('updateLeaderboard', top3);
}

// Twitch Listener
twitchClient.on('message', (channel, tags, message, self) => {
    if (self) return;
    processMessage(tags.username, channel.replace('#', ''));
});

// Kick Listener
kickClient.on(Events.ChatMessage, (data) => {
    processMessage(data.sender.username, 'imdoclive'); // Assumes your Kick channel is imdoclive
});

io.on('connection', (socket) => {
    socket.on('joinChannel', (channel) => {
        socket.join(channel.toLowerCase());
        const top3 = Object.entries(trackingData[channel.toLowerCase()] || {}).sort(([, a], [, b]) => b - a).slice(0, 3);
        socket.emit('updateLeaderboard', top3);
    });
});

cron.schedule('0 0 * * 1', () => { trackingData = {}; io.emit('updateLeaderboard', []); });

const PORT = process.env.PORT || 3000;
server.listen(PORT);