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
const defaultIgnored = ['imdoclive', 'botrix', 'botrixoficial', '@botrixoficial', 'kickbot', 'hugomcnut', '@missxss'];

// --- MongoDB Database Setup ---
const MONGO_URI = process.env.MONGO_URI; 

if (MONGO_URI) {
    mongoose.connect(MONGO_URI)
        .then(() => console.log('Successfully connected to MongoDB Cloud Database.'))
        .catch(err => console.error('MongoDB connection error:', err));
} else {
    mongoose.warn("WARNING: No MONGO_URI found. Data will not be saved permanently.");
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
function processMessage(user, messageContent, tags = null) {
    const cleanUser = user.toLowerCase().trim();
    if (!messageContent) return;

    let rawText = messageContent.trim();
    
    // If it's not a TTS command, handle normal leaderboard points and exit
    if (!rawText.startsWith('!')) {
        if (defaultIgnored.includes(cleanUser)) return;
        trackingData[cleanUser] = (trackingData[cleanUser] || 0) + 1;
        const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
        io.emit('updateLeaderboard', top3);
        return;
    }

    // Process commands
    const parts = rawText.split(' ');
    const command = parts[0].toLowerCase();
    const validVoices = ['!tts'];

    if (validVoices.includes(command)) {
        let spokenText = parts.slice(1).join(' ');

        // 1. Strip Twitch Native Emotes using exact index positions
        if (tags && tags.emotes) {
            let positions = [];
            Object.values(tags.emotes).forEach(ranges => {
                ranges.forEach(range => {
                    const [start, end] = range.split('-').map(Number);
                    positions.push({ start, end });
                });
            });
            // Sort descending to cut from back to front without breaking index orders
            positions.sort((a, b) => b.start - a.start);
            let msgArr = messageContent.split('');
            positions.forEach(pos => {
                msgArr.splice(pos.start, pos.end - pos.start + 1);
            });
            let clearedMsg = msgArr.join('');
            let clearedParts = clearedMsg.trim().split(' ');
            spokenText = clearedParts.slice(1).join(' ');
        }

        // 2. Strip Kick Native Emotes: [emote:id:name]
        spokenText = spokenText.replace(/\[emote:\d+:[^\]]+\]/gi, '');

        // 3. Strip Graphic Unicode Emojis
        spokenText = spokenText.replace(/[\u{1F300}-\u{1FAFF}\u{1F600}-\u{1F64F}\u{1F680}-\u{1F6FF}\u{2600}-\u{27BF}]/gu, '');

        // 4. Collapse 3+ repeated characters/numbers inside words (aaaaa -> a, 11111 -> 1)
        spokenText = spokenText.replace(/(.)\1{2,}/gu, '$1');

        // 5. Collapse consecutive duplicated words (hello hello hello -> hello)
        let words = spokenText.split(/\s+/);
        let cleanedWords = [];
        for (let i = 0; i < words.length; i++) {
            if (i > 0 && words[i].toLowerCase() === words[i-1].toLowerCase() && words[i].trim() !== '') {
                continue;
            }
            cleanedWords.push(words[i]);
        }
        spokenText = cleanedWords.join(' ');
        spokenText = spokenText.trim();

        // Only trigger the alert if there's real speakable text remaining
        if (spokenText.length > 0) {
            const voiceName = command.replace('!', ''); 
            io.emit('triggerTTS', { user: cleanUser, voice: voiceName, text: spokenText });
            console.log(`TTS Triggered: ${cleanUser} as ${voiceName} -> ${spokenText}`);
        }
    }

    // Track points for valid command triggers if they aren't on the ignore list
    if (defaultIgnored.includes(cleanUser)) return;
    trackingData[cleanUser] = (trackingData[cleanUser] || 0) + 1;
    const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
    io.emit('updateLeaderboard', top3);
}

// TWITCH CLIENT SETUP
const twitchClient = new tmi.Client({ identity: { username: 'justinfan12345' }, channels: ['imdoclive'] });
twitchClient.connect().catch(console.error);

twitchClient.on('message', (channel, tags, message, self) => {
    if (self) return;
    processMessage(tags.username, message, tags); 
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
