const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const tmi = require('tmi.js');
const cron = require('node-cron');
const cors = require('cors');
const mongoose = require('mongoose');
const WebSocket = require('ws'); 

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

let trackingData = {}; 
const defaultIgnored = ['imdoclive', 'botrix', 'botrixoficial', '@botrixoficial', 'kickbot', 'hugomcnut', 'missxss'];

// --- MongoDB Database Setup ---
const MONGO_URI = process.env.MONGO_URI; 

if (MONGO_URI) {
    mongoose.connect(MONGO_URI)
        .then(() => console.log('Successfully connected to MongoDB Cloud Database.'))
        .catch(err => console.error('MongoDB connection error:', err));
} else {
    console.warn("WARNING: No MONGO_URI found. Data will not be saved permanently.");
}

const UserScore = mongoose.model('UserScore', new mongoose.Schema({
    username: String,
    score: Number
}));

UserScore.find().then(users => {
    users.forEach(u => {
        trackingData[u.username] = u.score;
    });
    console.log("Loaded saved leaderboard data from the cloud.");
}).catch(console.error);

setInterval(async () => {
    if (!MONGO_URI || Object.keys(trackingData).length === 0) return;
    
    const bulkOps = Object.entries(trackingData).map(([username, score]) => ({
        updateOne: { filter: { username }, update: { username, score }, upsert: true }
    }));
    
    try { await UserScore.bulkWrite(bulkOps); } 
    catch (error) { console.error("Error saving to database:", error); }
}, 60000);

// --- DUAL-PROCESSING LOGIC (Leaderboard + TTS) ---
function processMessage(user, messageContent) {
    const cleanUser = user.toLowerCase().trim();
    if (defaultIgnored.includes(cleanUser)) return;
    
    // 1. Update Leaderboard
    trackingData[cleanUser] = (trackingData[cleanUser] || 0) + 1;
    const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
    io.emit('updateLeaderboard', top3);

    // 2. Check for TTS Commands
    if (messageContent) {
        const text = messageContent.trim();
        if (text.startsWith('!')) {
            const parts = text.split(' ');
            const command = parts[0].toLowerCase(); 
            const spokenText = parts.slice(1).join(' '); 

            const validVoices = ['!speed', '!trump', '!kanye'];
            
            // If it's a valid command and they actually typed a message
            if (validVoices.includes(command) && spokenText.length > 0) {
                const voiceName = command.replace('!', ''); 
                io.emit('triggerTTS', { user: cleanUser, voice: voiceName, text: spokenText });
                console.log(`TTS Triggered: ${cleanUser} as ${voiceName} -> ${spokenText}`);
            }
        }
    }
}

// TWITCH CLIENT SETUP
const twitchClient = new tmi.Client({ identity: { username: 'justinfan12345' }, channels: ['imdoclive'] });
twitchClient.connect().catch(console.error);

twitchClient.on('message', (channel, tags, message, self) => {
    if (self) return;
    processMessage(tags.username, message); 
});

// NATIVE KICK CHAT (Pure WebSocket)
const KICK_CHATROOM_ID = '386930'; 
const kickWs = new WebSocket('wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0&flash=false');

kickWs.on('open', () => {
    kickWs.send(JSON.stringify({ event: "pusher:subscribe", data: { auth: "", channel: `chatrooms.${KICK_CHATROOM_ID}.v2` } }));
    console.log("Successfully connected to Kick Chat!");
});

kickWs.on('message', (raw) => {
    try {
        const msg = JSON.parse(raw);
        if (msg.event === 'pusher:ping') {
            kickWs.send(JSON.stringify({ event: 'pusher:pong', data: {} }));
            return;
        }
        if (msg.event === 'App\\Events\\ChatMessageEvent') {
            const payload = typeof msg.data === 'string' ? JSON.parse(msg.data) : msg.data;
            if (payload.sender && payload.sender.username && payload.content) {
                processMessage(payload.sender.username, payload.content); 
            }
        }
    } catch (err) {
        console.error("Error parsing Kick websocket message:", err);
    }
});

io.on('connection', (socket) => {
    const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
    socket.emit('updateLeaderboard', top3);
});

cron.schedule('0 0 * * 1', async () => { 
    trackingData = {}; 
    if (MONGO_URI) { try { await UserScore.deleteMany({}); } catch (err) {} }
    io.emit('updateLeaderboard', []); 
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
