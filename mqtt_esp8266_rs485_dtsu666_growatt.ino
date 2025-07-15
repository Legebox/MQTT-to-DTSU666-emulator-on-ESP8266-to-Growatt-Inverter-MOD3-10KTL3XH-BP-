/*
 * Integrated MQTT to RS485 DTSU666 Emulator for ESP8266 with SoftwareSerial
 * 
 * This code integrates:
 * 1. MQTT client functionality
 * 2. RS485 communication using SoftwareSerial
 * 3. DTSU666 meter emulation logic with correct registers
 * 
 * The ESP8266 connects to an MQTT broker, subscribes to topics,
 * receives data, and emulates a DTSU666 meter over RS485.
 */

#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <SoftwareSerial.h>

// WiFi credentials
const char* ssid = "Your Access Point Name";
const char* password = "password";

// MQTT Broker settings
const char* mqtt_server = "your ip";
const int mqtt_port = 1883;
const char* mqtt_user = "";  // Leave empty if no authentication
const char* mqtt_password = "";  // Leave empty if no authentication
const char* client_id = "ESP8266_DTSU666_Emulator";

// MQTT Topics
const char* mqtt_topic_subscribe = "dtsu666/data";  // Topic to receive data for emulation
const char* mqtt_topic_status = "dtsu666/status";   // Topic to publish status updates

// RS485 settings
#define RS485_DIR_PIN D5  // GPIO4 on NodeMCU for direction control
#define RS485_RX_PIN D6   // GPIO14 on NodeMCU for RX
#define RS485_TX_PIN D7   // GPIO12 on NodeMCU for TX
#define RS485_TRANSMIT HIGH
#define RS485_RECEIVE LOW
const int RS485_BUFFER_SIZE = 64;
byte rs485Buffer[RS485_BUFFER_SIZE];

// DTSU666 Modbus slave address
#define DTSU666_SLAVE_ADDRESS 4  // Fixed slave address for DTSU666 emulation

// Initialize SoftwareSerial for RS485
SoftwareSerial rs485Serial(RS485_RX_PIN, RS485_TX_PIN); // RX, TX

// Initialize WiFi and MQTT clients
WiFiClient espClient;
PubSubClient mqtt_client(espClient);

// Last connection attempt timestamp
unsigned long lastReconnectAttempt = 0;
unsigned long lastStatusUpdate = 0;
const unsigned long STATUS_UPDATE_INTERVAL = 60000;  // 1 minute

// DTSU666 Modbus RTU Configuration Registers
#define REG_CT_RATIO          0x0006  // Current Transformer Ratio (IrAt)
#define REG_PT_RATIO          0x0007  // Voltage Transformer Ratio (UrAt)

// Line Voltages
#define REG_VOLTAGE_AB        0x2000  // Line voltage Uab (V)
#define REG_VOLTAGE_BC        0x2002  // Line voltage Ubc (V)
#define REG_VOLTAGE_CA        0x2004  // Line voltage Uca (V)

// Phase Voltages
#define REG_VOLTAGE_A         0x2006  // Phase voltage Ua (V)
#define REG_VOLTAGE_B         0x2008  // Phase voltage Ub (V)
#define REG_VOLTAGE_C         0x200A  // Phase voltage Uc (V)

// Phase Currents
#define REG_CURRENT_A         0x200C  // Phase current Ia (A)
#define REG_CURRENT_B         0x200E  // Phase current Ib (A)
#define REG_CURRENT_C         0x2010  // Phase current Ic (A)

// Active Power
#define REG_ACTIVE_POWER_TOTAL 0x2012  // Combined active power Pt (W)
#define REG_ACTIVE_POWER_A     0x2014  // A phase active power Pa (W)
#define REG_ACTIVE_POWER_B     0x2016  // B phase active power Pb (W)
#define REG_ACTIVE_POWER_C     0x2018  // C phase active power Pc (W)

// Reactive Power
#define REG_REACTIVE_POWER_TOTAL 0x201A  // Combined reactive power Qt (var)
#define REG_REACTIVE_POWER_A     0x201C  // A phase reactive power Qa (var)
#define REG_REACTIVE_POWER_B     0x201E  // B phase reactive power Qb (var)
#define REG_REACTIVE_POWER_C     0x2020  // C phase reactive power Qc (var)

// Power Factor
#define REG_POWER_FACTOR_TOTAL 0x202A  // Combined power factor PFt
#define REG_POWER_FACTOR_A     0x202C  // A phase power factor PFa
#define REG_POWER_FACTOR_B     0x202E  // B phase power factor PFb
#define REG_POWER_FACTOR_C     0x2030  // C phase power factor PFc

// Frequency
#define REG_FREQUENCY          0x2044  // Frequency (Hz)

// Energy
#define REG_FORWARD_ACTIVE_ENERGY_TOTAL 0x101E  // Total Forward active energy (kWh)
#define REG_FORWARD_ACTIVE_ENERGY_A     0x1020  // A Forward active energy (kWh)
#define REG_FORWARD_ACTIVE_ENERGY_B     0x1022  // B Forward active energy (kWh)
#define REG_FORWARD_ACTIVE_ENERGY_C     0x1024  // C Forward active energy (kWh)
#define REG_NET_FORWARD_ACTIVE_ENERGY   0x1026  // Net Forward active energy (kWh)
#define REG_REVERSE_ACTIVE_ENERGY_TOTAL 0x1028  // Total Reverse active energy (kWh)
#define REG_REVERSE_ACTIVE_ENERGY_A     0x102A  // A Reverse active energy (kWh)
#define REG_REVERSE_ACTIVE_ENERGY_B     0x102C  // B Reverse active energy (kWh)
#define REG_REVERSE_ACTIVE_ENERGY_C     0x102E  // C Reverse active energy (kWh)
#define REG_NET_REVERSE_ACTIVE_ENERGY   0x1030  // Net Reverse active energy (kWh)

// Scaling factors for register values
#define SCALE_VOLTAGE      10.0    // Value = register / 10 (V)
#define SCALE_CURRENT      1000.0  // Value = register / 1000 (A)
#define SCALE_POWER        10.0    // Value = register / 10 (W or var)
#define SCALE_POWER_FACTOR 1000.0  // Value = register / 1000
#define SCALE_FREQUENCY    100.0   // Value = register / 100 (Hz)
#define SCALE_ENERGY       1.0     // Value = register (kWh)

// Structure to hold meter values
struct MeterValues {
  // Line Voltages
  float voltageAB;
  float voltageBC;
  float voltageCA;
  
  // Phase Voltages
  float voltageA;
  float voltageB;
  float voltageC;
  
  // Phase Currents
  float currentA;
  float currentB;
  float currentC;
  
  // Active Power
  float activePowerTotal;
  float activePowerA;
  float activePowerB;
  float activePowerC;
  
  // Reactive Power
  float reactivePowerTotal;
  float reactivePowerA;
  float reactivePowerB;
  float reactivePowerC;
  
  // Power Factor
  float powerFactorTotal;
  float powerFactorA;
  float powerFactorB;
  float powerFactorC;
  
  // Frequency
  float frequency;
  
  // Energy
  float forwardActiveEnergyTotal;
  float forwardActiveEnergyA;
  float forwardActiveEnergyB;
  float forwardActiveEnergyC;
  float netForwardActiveEnergy;
  float reverseActiveEnergyTotal;
  float reverseActiveEnergyA;
  float reverseActiveEnergyB;
  float reverseActiveEnergyC;
  float netReverseActiveEnergy;
};

// Global meter values
MeterValues meterValues;

// Function prototypes
void setup_wifi();
void callback(char* topic, byte* payload, unsigned int length);
boolean reconnect();
void publishStatus(const char* status);
void setupRS485();
void rs485Send(byte* data, int length);
int rs485Receive(byte* buffer, int maxLength, unsigned long timeout);
void printHex(byte* data, int length);
uint16_t calculateModbusCRC(byte* data, int length);
int processDTSU666ModbusRequest(byte* request, int requestLength, byte* response);
void initializeMeterValues();
bool updateMeterValuesFromJSON(const char* jsonString);
bool getRegisterValue(uint16_t address, uint32_t* value);

// Helper function to convert float to IEEE754 bytes in ABCD order
void floatToBytes(float val, byte* bytes) {
  union {
    float f;
    uint32_t i;
  } u;
  
  u.f = val;
  bytes[0] = (u.i >> 24) & 0xFF;  // MSB with sign bit
  bytes[1] = (u.i >> 16) & 0xFF;
  bytes[2] = (u.i >> 8) & 0xFF;
  bytes[3] = u.i & 0xFF;          // LSB
}

void setup() {
  // Initialize hardware Serial for debugging
  Serial.begin(115200);
  Serial.println();
  Serial.println("ESP8266 MQTT to RS485 DTSU666 Emulator");
  Serial.print("DTSU666 Slave Address: ");
  Serial.println(DTSU666_SLAVE_ADDRESS);
    
  // Initialize SoftwareSerial for RS485 communication
  rs485Serial.begin(9600);
  
  // Initialize RS485 direction control pin
  pinMode(RS485_DIR_PIN, OUTPUT);
  digitalWrite(RS485_DIR_PIN, RS485_RECEIVE);  // Default to receive mode
  
  // Initialize meter values with defaults
  initializeMeterValues();
  
  // Connect to WiFi
  setup_wifi();
  
  // Set MQTT server and callback function
  mqtt_client.setServer(mqtt_server, mqtt_port);
  mqtt_client.setCallback(callback);
  mqtt_client.setBufferSize(768);
  
  // Initial connection attempt
  reconnect();
  
  Serial.println("ESP8266 MQTT to RS485 DTSU666 Emulator initialized");
}

void loop() {
  // Check if MQTT client is connected
  if (!mqtt_client.connected()) {
    // Try to reconnect every 5 seconds
    unsigned long now = millis();
    if (now - lastReconnectAttempt > 5000) {
      lastReconnectAttempt = now;
      if (reconnect()) {
        lastReconnectAttempt = 0;
      }
    }
  } else {
    // MQTT client loop processing
    mqtt_client.loop();
    
    // Publish status update periodically
    unsigned long now = millis();
    if (now - lastStatusUpdate > STATUS_UPDATE_INTERVAL) {
      lastStatusUpdate = now;
      publishStatus("Running");
    }
  }
  
  // Check for incoming Modbus requests
  int bytesRead = rs485Receive(rs485Buffer, RS485_BUFFER_SIZE, 100);
  
  if (bytesRead > 0) {
    // Process the request and generate a response
    byte responseBuffer[RS485_BUFFER_SIZE];
    int responseLength = processDTSU666ModbusRequest(rs485Buffer, bytesRead, responseBuffer);
    
    if (responseLength > 0) {
      // Send the response
      rs485Send(responseBuffer, responseLength);
    }
  }
  
  // Allow ESP8266 to handle background tasks
  yield();
}

// Setup WiFi connection
void setup_wifi() {
  delay(10);
  Serial.println();
  Serial.print("Connecting to ");
  Serial.println(ssid);

  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println("");
  Serial.println("WiFi connected");
  Serial.print("IP address: ");
  Serial.println(WiFi.localIP());
}

// MQTT message callback function
void callback(char* topic, byte* payload, unsigned int length) {
  Serial.print("Message arrived [");
  Serial.print(topic);
  Serial.print("] ");
  
  // Create a buffer for the payload
  char message[length + 1];
  for (unsigned int i = 0; i < length; i++) {
    message[i] = (char)payload[i];
    Serial.print((char)payload[i]);
  }
  message[length] = '\0';
  Serial.println();
  
  // Process the received message
  if (strcmp(topic, mqtt_topic_subscribe) == 0) {
    // Update meter values from the received JSON
    if (updateMeterValuesFromJSON(message)) {
      publishStatus("Data updated");
    } else {
      publishStatus("Invalid data format");
    }
  }
}

// Reconnect to MQTT broker
boolean reconnect() {
  Serial.print("Attempting MQTT connection...");
  
  // Attempt to connect with authentication if credentials are provided
  boolean connected;
  if (strlen(mqtt_user) > 0) {
    connected = mqtt_client.connect(client_id, mqtt_user, mqtt_password);
  } else {
    connected = mqtt_client.connect(client_id);
  }
  
  if (connected) {
    Serial.println("connected");
    
    // Subscribe to topic
    mqtt_client.subscribe(mqtt_topic_subscribe);
    
    // Publish connection status
    publishStatus("Connected");
    
    return true;
  } else {
    Serial.print("failed, rc=");
    Serial.print(mqtt_client.state());
    Serial.println(" try again in 5 seconds");
    return false;
  }
}

// Publish status message
void publishStatus(const char* status) {
  char status_message[100];
  snprintf(status_message, sizeof(status_message), 
           "{\"status\":\"%s\",\"ip\":\"%s\"}", 
           status, WiFi.localIP().toString().c_str());
  
  mqtt_client.publish(mqtt_topic_status, status_message);
}

// Send data over RS485 using SoftwareSerial
void rs485Send(byte* data, int length) {
  // Set RS485 to transmit mode
  digitalWrite(RS485_DIR_PIN, RS485_TRANSMIT);
  delay(10);  // Small delay to ensure the direction switch is complete
  
  // Send data using SoftwareSerial
  rs485Serial.write(data, length);
  
  // Wait for transmission to complete
  rs485Serial.flush();
  
  // Set RS485 back to receive mode
  digitalWrite(RS485_DIR_PIN, RS485_RECEIVE);
  
  // Debug output
  Serial.print("RS485 TX: ");
  printHex(data, length);
}

// Receive data over RS485 with timeout using SoftwareSerial
int rs485Receive(byte* buffer, int maxLength, unsigned long timeout) {
  // Ensure RS485 is in receive mode
  digitalWrite(RS485_DIR_PIN, RS485_RECEIVE);
  
  // Wait for data
  unsigned long startTime = millis();
  int bytesRead = 0;
  
  while ((millis() - startTime) < timeout && bytesRead < maxLength) {
    if (rs485Serial.available()) {
      buffer[bytesRead] = rs485Serial.read();
      bytesRead++;
    }
    yield();  // Allow ESP8266 to handle background tasks
  }
  
  // Debug output if data received
  if (bytesRead > 0) {
    Serial.print("RS485 RX: ");
    printHex(buffer, bytesRead);
  }
  
  return bytesRead;
}

// Print data in hexadecimal format for debugging
void printHex(byte* data, int length) {
  for (int i = 0; i < length; i++) {
    if (data[i] < 0x10) {
      Serial.print("0");
    }
    Serial.print(data[i], HEX);
    Serial.print(" ");
  }
  Serial.println();
}

// Modbus CRC calculation
uint16_t calculateModbusCRC(byte* data, int length) {
  uint16_t crc = 0xFFFF;
  
  for (int i = 0; i < length; i++) {
    crc ^= data[i];
    
    for (int j = 0; j < 8; j++) {
      if (crc & 0x0001) {
        crc >>= 1;
        crc ^= 0xA001;
      } else {
        crc >>= 1;
      }
    }
  }
  
  return crc;
}

// Initialize meter values with defaults
void initializeMeterValues() {
  // Line Voltages
  meterValues.voltageAB = 400.0;
  meterValues.voltageBC = 400.0;
  meterValues.voltageCA = 400.0;
  
  // Phase Voltages
  meterValues.voltageA = 230.0;
  meterValues.voltageB = 230.0;
  meterValues.voltageC = 230.0;
  
  // Phase Currents
  meterValues.currentA = 0.0;
  meterValues.currentB = 0.0;
  meterValues.currentC = 0.0;
  
  // Active Power
  meterValues.activePowerTotal = 0.0;
  meterValues.activePowerA = 0.0;
  meterValues.activePowerB = 0.0;
  meterValues.activePowerC = 0.0;
  
  // Reactive Power
  meterValues.reactivePowerTotal = 0.0;
  meterValues.reactivePowerA = 0.0;
  meterValues.reactivePowerB = 0.0;
  meterValues.reactivePowerC = 0.0;
  
  // Power Factor
  meterValues.powerFactorTotal = 1.0;
  meterValues.powerFactorA = 1.0;
  meterValues.powerFactorB = 1.0;
  meterValues.powerFactorC = 1.0;
  
  // Frequency
  meterValues.frequency = 50.0;
  
  // Energy
  meterValues.forwardActiveEnergyTotal = 0.0;
  meterValues.forwardActiveEnergyA = 0.0;
  meterValues.forwardActiveEnergyB = 0.0;
  meterValues.forwardActiveEnergyC = 0.0;
  meterValues.netForwardActiveEnergy = 0.0;
  meterValues.reverseActiveEnergyTotal = 0.0;
  meterValues.reverseActiveEnergyA = 0.0;
  meterValues.reverseActiveEnergyB = 0.0;
  meterValues.reverseActiveEnergyC = 0.0;
  meterValues.netReverseActiveEnergy = 0.0;
}

// Update meter values from MQTT message
// This function will be called from the MQTT callback
bool updateMeterValuesFromJSON(const char* jsonString) {
  // Parse the JSON
  StaticJsonDocument<768> doc;
  DeserializationError error = deserializeJson(doc, jsonString);
  if (error) {
    Serial.print("JSON parsing failed: ");
    Serial.println(error.c_str());
    return false;
  }
  
  // Update meter values if present in JSON
  // Line Voltages
  if (doc.containsKey("voltage_ab")) meterValues.voltageAB = doc["voltage_ab"];
  if (doc.containsKey("voltage_bc")) meterValues.voltageBC = doc["voltage_bc"];
  if (doc.containsKey("voltage_ca")) meterValues.voltageCA = doc["voltage_ca"];
  
  // Phase Voltages
  if (doc.containsKey("voltage_a")) meterValues.voltageA = doc["voltage_a"];
  if (doc.containsKey("voltage_b")) meterValues.voltageB = doc["voltage_b"];
  if (doc.containsKey("voltage_c")) meterValues.voltageC = doc["voltage_c"];
  
  // Phase Currents
  if (doc.containsKey("current_a")) meterValues.currentA = doc["current_a"];
  if (doc.containsKey("current_b")) meterValues.currentB = doc["current_b"];
  if (doc.containsKey("current_c")) meterValues.currentC = doc["current_c"];
  
  // Active Power
  if (doc.containsKey("active_power_total")) meterValues.activePowerTotal = doc["active_power_total"];
  if (doc.containsKey("active_power_a")) meterValues.activePowerA = doc["active_power_a"];
  if (doc.containsKey("active_power_b")) meterValues.activePowerB = doc["active_power_b"];
  if (doc.containsKey("active_power_c")) meterValues.activePowerC = doc["active_power_c"];
  
  // Reactive Power
  if (doc.containsKey("reactive_power_total")) meterValues.reactivePowerTotal = doc["reactive_power_total"];
  if (doc.containsKey("reactive_power_a")) meterValues.reactivePowerA = doc["reactive_power_a"];
  if (doc.containsKey("reactive_power_b")) meterValues.reactivePowerB = doc["reactive_power_b"];
  if (doc.containsKey("reactive_power_c")) meterValues.reactivePowerC = doc["reactive_power_c"];
  
  // Power Factor
  if (doc.containsKey("power_factor_total")) meterValues.powerFactorTotal = doc["power_factor_total"];
  if (doc.containsKey("power_factor_a")) meterValues.powerFactorA = doc["power_factor_a"];
  if (doc.containsKey("power_factor_b")) meterValues.powerFactorB = doc["power_factor_b"];
  if (doc.containsKey("power_factor_c")) meterValues.powerFactorC = doc["power_factor_c"];
  
  // Frequency
  if (doc.containsKey("frequency")) meterValues.frequency = doc["frequency"];
  
  // Energy
  if (doc.containsKey("forward_active_energy_total")) meterValues.forwardActiveEnergyTotal = doc["forward_active_energy_total"];
  if (doc.containsKey("forward_active_energy_a")) meterValues.forwardActiveEnergyA = doc["forward_active_energy_a"];
  if (doc.containsKey("forward_active_energy_b")) meterValues.forwardActiveEnergyB = doc["forward_active_energy_b"];
  if (doc.containsKey("forward_active_energy_c")) meterValues.forwardActiveEnergyC = doc["forward_active_energy_c"];
  if (doc.containsKey("net_forward_active_energy")) meterValues.netForwardActiveEnergy = doc["net_forward_active_energy"];
  if (doc.containsKey("reverse_active_energy_total")) meterValues.reverseActiveEnergyTotal = doc["reverse_active_energy_total"];
  if (doc.containsKey("reverse_active_energy_a")) meterValues.reverseActiveEnergyA = doc["reverse_active_energy_a"];
  if (doc.containsKey("reverse_active_energy_b")) meterValues.reverseActiveEnergyB = doc["reverse_active_energy_b"];
  if (doc.containsKey("reverse_active_energy_c")) meterValues.reverseActiveEnergyC = doc["reverse_active_energy_c"];
  if (doc.containsKey("net_reverse_active_energy")) meterValues.netReverseActiveEnergy = doc["net_reverse_active_energy"];
  
  Serial.println("Meter values updated from MQTT");
  return true;
}

// Convert float value to register value based on scaling factor
uint32_t floatToRegister(float value, float scale) {
  return (uint32_t)(value * scale);
}

// Get register value for a specific address
// Returns true if address is valid and value is set
bool getRegisterValue(uint16_t address, byte* response, int* bytesWritten) {
  *bytesWritten = 0;
  
  switch (address) {
    // ===== DTSU666 Configuration Registers =====
    case REG_CT_RATIO: {  // 0x0006 - CT Ratio (IrAt)
        uint16_t ctRatio = 10;
        response[0] = (ctRatio >> 8) & 0xFF;  // High byte
        response[1] = ctRatio & 0xFF;         // Low byte
        *bytesWritten = 2;
        return true;
    }      
    case REG_PT_RATIO: {  // 0x0007 - PT Ratio (UrAt)
        uint16_t ptRatio = 1.0;
        response[0] = (ptRatio >> 8) & 0xFF;  // High byte
        response[1] = ptRatio & 0xFF;         // Low byte
        *bytesWritten = 2;
        return true;
    }
         
    // Line Voltages
    case REG_VOLTAGE_AB:
      floatToBytes(meterValues.voltageAB, response);
      *bytesWritten = 4;
      return true;
    case REG_VOLTAGE_BC:
      floatToBytes(meterValues.voltageBC, response);
      *bytesWritten = 4;
      return true;
    case REG_VOLTAGE_CA:
      floatToBytes(meterValues.voltageCA, response);
      *bytesWritten = 4;
      return true;
    
    // Phase Voltages
    case REG_VOLTAGE_A:
      floatToBytes(meterValues.voltageA, response);
      *bytesWritten = 4;
      return true;
    case REG_VOLTAGE_B:
      floatToBytes(meterValues.voltageB, response);
      *bytesWritten = 4;
      return true;
    case REG_VOLTAGE_C:
      floatToBytes(meterValues.voltageC, response);
      *bytesWritten = 4;
      return true;
    
    // Phase Currents
    case REG_CURRENT_A:
      floatToBytes(meterValues.currentA, response);
      *bytesWritten = 4;
      return true;
    case REG_CURRENT_B:
      floatToBytes(meterValues.currentB, response);
      *bytesWritten = 4;
      return true;
    case REG_CURRENT_C:
      floatToBytes(meterValues.currentC, response);
      *bytesWritten = 4;
      return true;
    
    // Active Power
    case REG_ACTIVE_POWER_TOTAL:
      floatToBytes(meterValues.activePowerTotal, response);
      *bytesWritten = 4;
      return true;
    case REG_ACTIVE_POWER_A:
      floatToBytes(meterValues.activePowerA, response);
      *bytesWritten = 4;
      return true;
    case REG_ACTIVE_POWER_B:
      floatToBytes(meterValues.activePowerB, response);
      *bytesWritten = 4;
      return true;
    case REG_ACTIVE_POWER_C:
      floatToBytes(meterValues.activePowerC, response);
      *bytesWritten = 4;
      return true;
    
    // Reactive Power
    case REG_REACTIVE_POWER_TOTAL:
      floatToBytes(meterValues.reactivePowerTotal, response);
      *bytesWritten = 4;
      return true;
    case REG_REACTIVE_POWER_A:
      floatToBytes(meterValues.reactivePowerA, response);
      *bytesWritten = 4;
      return true;
    case REG_REACTIVE_POWER_B:
      floatToBytes(meterValues.reactivePowerB, response);
      *bytesWritten = 4;
      return true;
    case REG_REACTIVE_POWER_C:
      floatToBytes(meterValues.reactivePowerC, response);
      *bytesWritten = 4;
      return true;
    
    // Power Factor
    case REG_POWER_FACTOR_TOTAL: {
      float scaledValue = meterValues.powerFactorTotal / SCALE_POWER_FACTOR;
      floatToBytes(scaledValue, response);
      *bytesWritten = 4;
      return true;
    }
    case REG_POWER_FACTOR_A: {
      float scaledValue = meterValues.powerFactorA / SCALE_POWER_FACTOR;
      floatToBytes(scaledValue, response);
      *bytesWritten = 4;
      return true;
    }
    case REG_POWER_FACTOR_B: {
      float scaledValue = meterValues.powerFactorB / SCALE_POWER_FACTOR;
      floatToBytes(scaledValue, response);
      *bytesWritten = 4;
      return true;
    }
    case REG_POWER_FACTOR_C: {
      float scaledValue = meterValues.powerFactorC / SCALE_POWER_FACTOR;
      floatToBytes(scaledValue, response);
      *bytesWritten = 4;
      return true;
    }
    
    // Frequency
    case REG_FREQUENCY:
      floatToBytes(meterValues.frequency, response);
      *bytesWritten = 4;
      return true;
    
    // Energy
    case REG_FORWARD_ACTIVE_ENERGY_TOTAL: {
      uint32_t wh = meterValues.forwardActiveEnergyTotal * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_FORWARD_ACTIVE_ENERGY_A: {
      uint32_t wh = meterValues.forwardActiveEnergyA * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_FORWARD_ACTIVE_ENERGY_B: {
      uint32_t wh = meterValues.forwardActiveEnergyB * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_FORWARD_ACTIVE_ENERGY_C: {
      uint32_t wh = meterValues.forwardActiveEnergyC * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_NET_FORWARD_ACTIVE_ENERGY: {
      uint32_t wh = meterValues.netForwardActiveEnergy * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_REVERSE_ACTIVE_ENERGY_TOTAL: {
      uint32_t wh = meterValues.reverseActiveEnergyTotal * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_REVERSE_ACTIVE_ENERGY_A: {
      uint32_t wh = meterValues.reverseActiveEnergyA * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_REVERSE_ACTIVE_ENERGY_B: {
      uint32_t wh = meterValues.reverseActiveEnergyB * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_REVERSE_ACTIVE_ENERGY_C: {
      uint32_t wh = meterValues.reverseActiveEnergyC * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    case REG_NET_REVERSE_ACTIVE_ENERGY: {
      uint32_t wh = meterValues.netReverseActiveEnergy * 1000;
      response[0] = (wh >> 24) & 0xFF;
      response[1] = (wh >> 16) & 0xFF; 
      response[2] = (wh >> 8) & 0xFF;
      response[3] = wh & 0xFF;
      *bytesWritten = 4;
      return true;
    }
    
    default:
      return false; 
  }      
}

// Process a Modbus request for DTSU666 emulation
int processDTSU666ModbusRequest(byte* request, int requestLength, byte* response) {
  // Check if request is valid (minimum length and CRC)
  if (requestLength < 8) {
    return 0;  // Invalid request, too short
  }
    
  // Extract slave address from request
  byte slaveAddress = request[0];
  
  // Only respond to our specific slave address (4)
  if (slaveAddress != DTSU666_SLAVE_ADDRESS) {
    return 0;  // Not for us, ignore
  }
  
  // Calculate and verify CRC
  uint16_t receivedCRC = (request[requestLength-1] << 8) | request[requestLength-2];
  uint16_t calculatedCRC = calculateModbusCRC(request, requestLength-2);
  
  if (receivedCRC != calculatedCRC) {
    return 0;  // Invalid CRC
  }
  
  // Extract request parameters
  byte functionCode = request[1];
  uint16_t startAddress = (request[2] << 8) | request[3];
  uint16_t quantity = (request[4] << 8) | request[5];
  
  // Function code 0x03 (Read Holding Registers)
  if (functionCode == 0x03) {
    response[0] = slaveAddress;
    response[1] = functionCode;
    
    int byteCountPos = 2;
    int dataPos = 3;
    int bytesWritten = 0;
    
    for (int i = 0; i < quantity; ) {
      uint16_t address = startAddress + i;
      byte regData[4];
      int regBytes;
      
      if (getRegisterValue(address, regData, &regBytes)) {
        // Copy register data to response
        memcpy(response + dataPos, regData, regBytes);
        dataPos += regBytes;
        bytesWritten += regBytes;
        
        // Skip next register if we handled a 32-bit float
        i += (regBytes == 4) ? 2 : 1;
      } else {
        // Unknown register - write zeros
        response[dataPos++] = 0;
        response[dataPos++] = 0;
        bytesWritten += 2;
        i++;
      }
    }
    
    // Set byte count
    response[byteCountPos] = bytesWritten;
    
    // Calculate CRC
    uint16_t crc = calculateModbusCRC(response, dataPos);
    response[dataPos++] = crc & 0xFF;
    response[dataPos++] = (crc >> 8) & 0xFF;
    
    return dataPos;
  }
  
  return 0;
}
