const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const tmi = require('tmi.js');
const { KickConnection, Events } = require('kick-live-connector');
const cron = require('node-cron');
const cors = require('cors');
const mongoose = require('mongoose');

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

// Load saved scores on startup
UserScore.find().then(users => {
    users.forEach(u => {
        trackingData[u.username] = u.score;
    });
    console.log("Loaded saved leaderboard data from the cloud.");
}).catch(console.error);

// Sync memory to database every 60 seconds
setInterval(async () => {
    if (!MONGO_URI || Object.keys(trackingData).length === 0) return;
    
    const bulkOps = Object.entries(trackingData).map(([username, score]) => ({
        updateOne: {
            filter: { username },
            update: { username, score },
            upsert: true
        }
    }));
    
    try {
        await UserScore.bulkWrite(bulkOps);
    } catch (error) {
        console.error("Error saving to database:", error);
    }
}, 60000);
// ------------------------------

const twitchClient = new tmi.Client({ identity: { username: 'justinfan12345' }, channels: ['imdoclive'] });
twitchClient.connect().catch(console.error);

const kickClient = new KickConnection('imdoclive');
kickClient.connect().catch(console.error);

function processMessage(user) {
    const cleanUser = user.toLowerCase().trim();
    if (defaultIgnored.includes(cleanUser)) return;
    
    trackingData[cleanUser] = (trackingData[cleanUser] || 0) + 1;
    
    const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
    io.emit('updateLeaderboard', top3);
}

twitchClient.on('message', (channel, tags, message, self) => {
    if (self) return;
    processMessage(tags.username);
});

kickClient.on(Events.ChatMessage, (data) => {
    processMessage(data.sender.username);
});

io.on('connection', (socket) => {
    const top3 = Object.entries(trackingData).sort(([, a], [, b]) => b - a).slice(0, 3);
    socket.emit('updateLeaderboard', top3);
});

// Weekly Reset (Monday at Midnight)
cron.schedule('0 0 * * 1', async () => { 
    trackingData = {}; 
    if (MONGO_URI) {
        try {
            await UserScore.deleteMany({});
            console.log("Weekly reset: Cloud database wiped.");
        } catch (err) {
            console.error("Error wiping database:", err);
        }
    }
    io.emit('updateLeaderboard', []); 
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));