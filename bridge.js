import dotenv from '@dotenvx/dotenvx';
import mqtt from 'mqtt';
import { createClient } from '@supabase/supabase-js';
import bcrypt from 'bcryptjs';
import http from 'http';

console.log("Node version:", process.version);
console.log("ENV keys present:", Object.keys(process.env).filter(k => k.includes("SUPABASE") || k.includes("MQTT")));
console.log("▶️ Initializing Locker Backend Bridge...");

// Catch unexpected crashes or syntax bugs instantly
process.on('uncaughtException', (err) => {
    console.error('💥 CRITICAL RUNTIME ERROR:', err.stack);
    process.exit(1);
});

dotenv.config(); // Loads a local .env if present

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
const supabase = createClient(supabaseUrl, supabaseKey);

console.log("📡 Attempting connection to MQTT Broker:", mqttBrokerUrl);
const mqttClient = mqtt.connect(mqttBrokerUrl, {
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    rejectUnauthorized: true,
    connectTimeout: 5000
});

mqttClient.on('connect', () => {
    console.log('🚀 SUCCESS: Connected to Secure MQTT Broker! Listening for locker payloads...');
    mqttClient.subscribe('lockers/+/deposit', { qos: 2 });
    mqttClient.subscribe('lockers/+/claim', { qos: 2 });
});

mqttClient.on('error', (err) => {
    console.error('❌ MQTT Driver Error:', err.message);
});

mqttClient.on('close', () => {
    console.log('⚠️ MQTT connection closed by broker.');
});

mqttClient.on('message', async (topic, message) => {
  try {
    const payload = JSON.parse(message.toString());
    
    // Parse lockerId as a clean integer base-10 to prevent data-type query mismatches
    const lockerId = parseInt(topic.split('/')[1], 10);
    if (isNaN(lockerId)) {
        console.error(`❌ Invalid locker topic path structure: ${topic}`);
        return;
    }

    // ==========================================
    // 🔒 DEPOSIT FLOW (With Vacancy Guard Check)
    // ==========================================
    if (topic.endsWith('/deposit')) {
      console.log(`📥 Ingested deposit payload for locker ${lockerId}`);

      // 1. Check if the locker is already occupied in the system
      const { data: lockerStatus, error: lockerCheckError } = await supabase
        .from('lockers')
        .select('status')
        .eq('id', lockerId)
        .maybeSingle();

      if (lockerCheckError) {
        console.error("❌ Database verification failed:", lockerCheckError.message);
        return;
      }

      // Safeguard: Block if status column is explicitly set to 'occupied'
      if (lockerStatus && lockerStatus.status === 'occupied') {
        console.log(`🚫 REJECTED: Deposit hit Locker ${lockerId}, but it is ALREADY OCCUPIED!`);
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "ERROR", msg: "Locker Occupied" }));
        return; 
      }

      // 2. Validate input PIN
      const rawPin = payload.pin;
      if (!rawPin) {
        console.error("❌ Drop canceled: No PIN provided in payload.");
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "ERROR", msg: "No PIN Given" }));
        return;
      }

      const studentName = payload.studentName || 'Unknown Admin/User';
      const studentNumber = payload.studentNumber || 'N/A';
      const courseSection = payload.courseSection || 'N/A';

      // 3. Hash the PIN
      const salt = await bcrypt.genSalt(10);
      const hashedPin = await bcrypt.hash(rawPin, salt);

      // 4. Create the active transaction row
      const { error: txError } = await supabase
        .from('transactions')
        .insert([
          { 
            locker_id: lockerId, 
            student_name: studentName, 
            student_number: studentNumber, 
            course_section: courseSection,
            pin_hash: hashedPin, 
            status: 'active',
            created_at: new Date()
          }
        ]);

      if (txError) {
        console.error("❌ Supabase DB Insert Failed:", txError.message);
        return;
      }

      // 5. Explicitly flag the physical locker slot status as occupied
      await supabase
        .from('lockers')
        .update({ status: 'occupied' })
        .eq('id', lockerId);

      console.log(`🔒 Locker ${lockerId} successfully occupied by ${studentNumber}`);
      mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "SUCCESS", msg: "Locked" }));
    }

    // ==========================================
    // 🔓 CLAIM (VACANTING) FLOW
    // ==========================================
    if (topic.endsWith('/claim')) {
      console.log(`📥 Ingested claim payload for locker ${lockerId}`);
      const typedPin = payload.pin;

      if (!typedPin) {
         console.error("❌ Claim canceled: No PIN typed.");
         mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "ERROR", msg: "Enter PIN" }));
         return;
      }

      // 1. Search for the single active transaction row matching this numerical locker ID
      const { data: transaction, error: dbError } = await supabase
        .from('transactions')
        .select('*')
        .eq('locker_id', lockerId)
        .eq('status', 'active')
        .maybeSingle();

      if (dbError) {
        console.error("❌ Supabase Query Error during claim lookup:", dbError.message);
        return;
      }

      if (!transaction) {
        console.log(`⚠️ Claim attempt on empty or error locker slot ${lockerId}`);
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "ERROR", msg: "No Active Session" }));
        return;
      }

      // 2. Validate user's typed PIN against the stored hash
      const isMatch = await bcrypt.compare(typedPin, transaction.pin_hash);

      if (isMatch) {
        // 3. Close the history row item
        await supabase
          .from('transactions')
          .update({ status: 'completed', claimed_at: new Date() })
          .eq('id', transaction.id);

        // 4. Reset the locker status flag back to 'vacant'
        await supabase
          .from('lockers')
          .update({ status: 'vacant' })
          .eq('id', lockerId);

        // 5. Dispatched outputs over MQTT to hardware relays and LCD displays
        mqttClient.publish(`lockers/${lockerId}/control`, JSON.stringify({ command: "UNLOCK" }));
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "SUCCESS", msg: "Unlocked" }));
        console.log(`🔓 SUCCESS: Locker ${lockerId} successfully claimed and is now VACANT.`);
      } else {
        console.log(`❌ Invalid PIN attempt typed for Locker ${lockerId}`);
        mqttClient.publish(`lockers/${lockerId}/display`, JSON.stringify({ status: "ERROR", msg: "Invalid PIN" }));
      }
    }

  } catch (err) {
    console.error("💥 General Parsing Error:", err);
  }
});

// ==========================================
// 🌍 HEALTH CHECK WEB SERVER (For Render Free Tier)
// ==========================================
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('iKaha Backend Bridge is Active and Running!\n');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🌍 Health check web server is listening on port ${PORT}`);
});