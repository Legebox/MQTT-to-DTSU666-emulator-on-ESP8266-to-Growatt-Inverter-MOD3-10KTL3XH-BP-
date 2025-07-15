The goal was to send the values read from a physical DTSU666 meter and stored on an MQTT server to an ESP8266.

On the ESP8266 a DTSU666 is emulated from which the Growatt inverter connected to the MAX485 can read its required registers by Modbus to meter port.

In this way the DTSU666 values can be used in a home automation system but are also available for the inverter to regulate the generated energy flow.

Connecting an DTSU666, ESP8266 and inverter to the same Modbus RTU line didn't work out because the MAX485 use 3.3 V and the inverter 4.8 V.

Set Growatt holding register 122 from 0 to 1 to enable polling of the emulator.
