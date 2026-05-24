import dotenv from '@dotenvx/dotenvx';
import mqtt from 'mqtt';
import { createClient } from '@supabase/supabase-js';
import bcrypt from 'bcryptjs';

console.log("Node version:", process.version);
console.log("ENV keys present:", Object.keys(process.env).filter(k => k.includes("SUPABASE") || k.includes("MQTT")));
console.log("▶️ Initializing Locker Backend Bridge...");

// Catch unexpected crashes or syntax bugs instantly
process.on('uncaughtException', (err) => {
    console.error('💥 CRITICAL RUNTIME ERROR:', err.stack);
    process.exit(1);
});

dotenv.config(); // Loads a local .env if present

// Clean validation: Check process.env directly, which works on both local and Render!
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const mqttBrokerUrl = process.env.MQTT_BROKER_URL;

if (!supabaseUrl || !supabaseKey) {
    console.error("❌ Configuration Error: Missing SUPABASE variables in system environment!");
    process.exit(1);
}
if (!mqttBrokerUrl) {
    console.error("❌ Configuration Error: Missing MQTT broker variable in system environment!");
    process.exit(1);
}

console.log("✅ Environment validation passed successfully.");

console.log("🔗 Connecting to Supabase at:", supabaseUrl);
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

console.log("📡 Attempting connection to MQTT Broker:", process.env.MQTT_BROKER_URL);
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    rejectUnauthorized: true,
    connectTimeout: 5000 // Give up after 5 seconds instead of hanging
});

mqttClient.on('connect', () => {
    console.log('🚀 SUCCESS: Connected to Secure MQTT Broker! Listening for locker payloads...');
    mqttClient.subscribe('lockers/+/deposit');
    mqttClient.subscribe('lockers/+/claim');
});

mqttClient.on('error', (err) => {
    console.error('❌ MQTT Driver Error:', err.message);
});

mqttClient.on('close', () => {
    console.log('⚠️ MQTT connection closed by broker.');
});

mqttClient.on('message', async (topic, message) => {
    const lockerId = parseInt(topic.split('/')[1]);
    try {
        const payload = JSON.parse(message.toString());
        console.log(`📥 Ingested data on topic [${topic}]:`, payload);
        if (topic.endsWith('/deposit')) {
            await handleDeposit(lockerId, payload);
        } else if (topic.endsWith('/claim')) {
            await handleClaim(lockerId, payload);
        }
    } catch (error) {
        console.error(`❌ Error parsing structural payload:`, error.message);
    }
});

async function handleDeposit(lockerId, data) {
    const { studentName, studentNumber, courseSection, pin } = data;
    const pinHash = await bcrypt.hash(pin, 10);

    const { error: txError } = await supabase
        .from('transactions')
        .insert([{
            student_name: studentName,
            student_number: studentNumber,
            course_section: courseSection,
            locker_id: lockerId,
            pin_hash: pinHash,
            status: 'active'
        }]);

    if (txError) {
        console.error("❌ Supabase DB Insert Failed:", txError.message);
        return;
    }

    await supabase.from('lockers').update({ status: 'occupied' }).eq('id', lockerId);
    console.log(`🔒 Locker ${lockerId} registered to ${studentNumber}`);
    mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "SUCCESS", msg: "Locked" }));
}

async function handleClaim(lockerId, data) {
    const { pin } = data;

    const { data: tx, error: txError } = await supabase
        .from('transactions')
        .select('*')
        .eq('locker_id', lockerId)
        .eq('status', 'active')
        .maybeSingle();

    if (txError || !tx) {
        console.log(`⚠️ Claim attempt on empty or error locker slot ${lockerId}`);
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "ERROR", msg: "No Active Session" }));
        return;
    }

    const pinMatch = await bcrypt.compare(pin, tx.pin_hash);

    if (pinMatch) {
        await supabase.from('transactions').update({ status: 'completed', released_at: new Date() }).eq('id', tx.id);
        await supabase.from('lockers').update({ status: 'vacant' }).eq('id', lockerId);
        
        mqttClient.publish(`lockers/${lockerId}/control`, JSON.stringify({ command: "UNLOCK" }));
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "SUCCESS", msg: "Unlocked" }));
        console.log(`🔓 Locker ${lockerId} successfully claimed.`);
    } else {
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "ERROR", msg: "Invalid PIN" }));
        console.log(`⚠️ Wrong PIN entered for Locker ${lockerId}`);
    }
}

const http = require('http');

// 1. Create a minimal web server to satisfy Render's health checks
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('iKaha Backend Bridge is Active and Running!\n');
});

// 2. Render automatically injects a PORT variable into system environment memory
const PORT = process.env.PORT || 3000;

// 3. Listen on the designated port
server.listen(PORT, () => {
  console.log(`🌍 Health check web server is listening on port ${PORT}`);
});