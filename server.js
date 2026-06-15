const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const tmi = require('tmi.js');
const cron = require('node-cron');
const cors = require('cors');
const mongoose = require('mongoose');
const WebSocket = require('ws'); 
const https = require('https'); // ADDED: Required for FakeYou API

const app = express();
app.use(cors());
app.use(express.static(__dirname)); // ADDED: Allows Render to show your tts.html file
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

let trackingData = {}; 
const defaultIgnored = ['imdoclive', 'botrix', 'botrixoficial', '@botrixoficial', 'kickbot', 'hugomcnut', 'missxss'];

// --- FAKEYOU API VOICE TOKENS ---
const FAKEYOU_MODELS = {
    'speed': 'weight_msq6440ch8hj862nz5y255n8j', 
    'trump': 'weight_ppqs5038bvkm6wc29w0xfebzy', 
    'riley': 'weight_6kgfe08hzee1x3gfh5dpcehvh'  
};

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

// --- HTTPS HELPER FOR FAKEYOU ---
function fakeYouFetch(method, path, body = null) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'api.fakeyou.com',
            path: path,
            method: method,
            headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' }
        };
        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); } catch (e) { resolve(null); }
            });
        });
        req.on('error', reject);
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

// --- FAKEYOU AUDIO GENERATOR ---
async function generateFakeYouAudio(voiceName, text, username) {
    const model = FAKEYOU_MODELS[voiceName];
    if (!model) return;
    
    const uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });

    console.log(`[FakeYou] Sending "${text}" to ${voiceName}... (Waiting in queue)`);
    try {
        const postRes = await fakeYouFetch('POST', '/tts/inference', {
            tts_model_token: model,
            uuid_idempotency_token: uuid,
            inference_text: text
        });
        
        if (!postRes || !postRes.success) return console.log("[FakeYou] Error starting job.");
        
        const jobToken = postRes.inference_job_token;
        let attempts = 0;
        
        while (attempts < 30) { 
            await new Promise(r => setTimeout(r, 3000)); 
            const pollRes = await fakeYouFetch('GET', `/tts/job/${jobToken}`);
            
            if (pollRes && pollRes.state) {
                const status = pollRes.state.status;
                if (status === 'complete_success') {
                    const path = pollRes.state.maybe_public_bucket_wav_audio_path || pollRes.state.maybe_public_bucket_media_path;
                    const audioUrl = path.startsWith('http') ? path : 'https://storage.fakeyou.com' + path;
                    
                    io.emit('triggerTTS', { user: username, voice: voiceName, text: text, audioUrl: audioUrl });
                    console.log(`[FakeYou] Success! Sent audio link to OBS.`);
                    return;
                } else if (status === 'dead' || status === 'canceled') {
                    return console.log("[FakeYou] Job failed or was canceled by server.");
                }
            }
            attempts++;
        }
        console.log("[FakeYou] Timed out waiting for audio.");
    } catch (err) {
        console.error("[FakeYou] API Error:", err);
    }
}

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
    
    // UPDATED: Swapped kanye for riley based on your tokens
    const validVoices = ['!speed', '!trump', '!riley'];

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

        // 4. Collapse 3+ repeated characters/numbers inside words
        spokenText = spokenText.replace(/(.)\1{2,}/gu, '$1');

        // 5. Collapse consecutive duplicated words
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

        // REPLACED: Normal TTS trigger is now the FakeYou AI trigger
        if (spokenText.length > 0) {
            const voiceName = command.replace('!', ''); 
            generateFakeYouAudio(voiceName, spokenText, cleanUser);
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
