############################################################
#################### IMPORT LIBRARIES ######################
############################################################
from time import sleep                     # <<< DO NOT REMOVE >>>
import onewire
import ds18x20
from machine import Pin, I2C, ADC
from mfrc522 import MFRC522
from picozero import Speaker
from ssd1306 import SSD1306_I2C


# Imports for MQTT communication           # <<< DO NOT REMOVE >>>
import network                             # <<< DO NOT REMOVE >>>
import json                                # <<< DO NOT REMOVE >>>
from umqtt.robust import MQTTClient        # <<< DO NOT REMOVE >>>

# Imports the library to make a random
#    number. This is used to create a
#    psuedo temperature value to transmit for demo
#    purposes. You don't need this library for the
#    project.
import random

############################################################
################# SPECIFY PINS AND OBJECTS #################
############################################################
#Speaker
speaker = Speaker(26)


#Temprature Sensor
tempSensorPin = machine.Pin(14)

tempSensor = ds18x20.DS18X20(onewire.OneWire(tempSensorPin))

tsList = tempSensor.scan()
print(' Found temp Sensors: ', tsList)


# OLED object
display_width = 128 # pixel x values = 0 to 127
display_height = 64 # pixel y values = 0 to 63
i2c = I2C(0, sda=Pin(0), scl=Pin(1), freq=400000) # TX pin is Pin 0, RX pin is Pin 1

display = SSD1306_I2C(display_width, display_height, i2c)


#RFID
reader = MFRC522(spi_id=0,sck=6,miso=4,mosi=7,cs=5,rst=8)





############################################################
##################### OTHER SETUP STUFF ####################
############################################################

# Wi-Fi and MQTT settings
SSID = "WilfongEngr301" # Raspberry Pi 4 Wi-Fi name                               # <<< DO NOT REMOVE >>>
PASSWORD = "BoilerUp" # Raspberry Pi 4 Wi-Fi password, WPA/WPA2 security          # <<< DO NOT REMOVE >>>
MQTT_BROKER = "10.42.0.1"  # aspberry Pi 4's IP                                   # <<< DO NOT REMOVE >>>
TOPIC = "pico/data" # "pico/data" is just a label                                 # <<< DO NOT REMOVE >>>
                    # It helps organize messages, like folders in a file system.  # <<< DO NOT REMOVE >>>
                    # The TOPIC could be any string, but leave it as "pico/data"  # <<< DO NOT REMOVE >>>

SENSOR_ID = "Team06"  # !!!-- CHANGE THIS AS DIRECTED BY DR. WILFONG --!!!


# Connect to Wi-Fi                                      # <<< DO NOT REMOVE >>>
wlan = network.WLAN(network.STA_IF)                     # <<< DO NOT REMOVE >>>
wlan.active(True)                                       # <<< DO NOT REMOVE >>>
wlan.connect(SSID, PASSWORD)                            # <<< DO NOT REMOVE >>>

print("Attempting to connect to Wi-Fi")
while not wlan.isconnected():                           # <<< DO NOT REMOVE >>>
    pass                                                # <<< DO NOT REMOVE >>>

sleep(2)  # Extra delay for stability                   # <<< DO NOT REMOVE >>>
print("Connected to Wi-Fi!")

# Optional: disable Wi-Fi power save                    # <<< DO NOT REMOVE >>>
wlan.config(pm = 0xa11140)                              # <<< DO NOT REMOVE >>>

# Connect to MQTT broker with reconnect support         # <<< DO NOT REMOVE >>>
client = MQTTClient(f"client_{SENSOR_ID}", MQTT_BROKER) # <<< DO NOT REMOVE >>>
client.DEBUG = True                                     # <<< DO NOT REMOVE >>>

# Try to connect to MQTT broker                         # <<< DO NOT REMOVE >>>
try:                                                    # <<< DO NOT REMOVE >>>
    client.connect()                                    # <<< DO NOT REMOVE >>>
    print("Connected to MQTT broker!")
except Exception as e:                                  # <<< DO NOT REMOVE >>>
    print("Failed to connect to MQTT broker:", e)




display.fill(0) # clears display
display.text("Starting Up", 0, 0) # write text starting at x=0 and y=0
display.show() # make the changes take effect



# Buzzer


BEAT = 0.5 # You can tweak this for faster/slower playback


# Formatted melody
melody2 = [
    ['g5', BEAT / 2]
    ]

melody = ['g5', BEAT / 2], ['g5', BEAT / 2], ['a5', BEAT / 2], ['g5', BEAT / 2], ['b5', BEAT / 4], ['a5', BEAT / 4], ['g5', BEAT /2]
  
    

for note in melody:
    speaker.play(note)
    sleep(0.1)
    
locked = 1
############################################################
####################### INFINITE LOOP ######################
############################################################
while True:
    
    reader.init()
    (stat, tag_type) = reader.request(reader.REQIDL)

    if stat == reader.OK:
        (stat, uid) = reader.SelectTagSN()
        card = int.from_bytes(bytes(uid), 'little', False)
        print(card)
        if (card == 3623865223):
            if locked == 1:
                locked = 0
                print("Unlocked")
                for note in melody2:
                    speaker.play(note)
            elif locked == 0:
                locked = 1
                print("Locked")
        else:
            display.fill(0) 
            display.text("Incorrect RFID", 0, 0) 
            display.text("Please try", 0, 10) 
            display.text("Again", 0, 20) 
            display.show() 
         
    if locked == False:   
        # !!!-- Psuedo temperature sensor reading between 60 and 80 --!!!
        # !!!-- You must use this variable name: temperature_sensor_reading --!!!
        # !!!-- Currently, the temperature reading is just a random number for demo purposes --!!!
        tempSensor.convert_temp()
        for tsListItem in tsList:
            temperature_sensor_reading = tempSensor.read_temp(tsListItem)
        
        
            
        
        
        # Create and send MQTT payload                               # <<< DO NOT REMOVE >>>
        message_data = {                                             # <<< DO NOT REMOVE >>>
            "sensorID": SENSOR_ID,                                   # <<< DO NOT REMOVE >>>
            "temperatureReading": temperature_sensor_reading         # <<< DO NOT REMOVE >>>
        }                                                            # <<< DO NOT REMOVE >>>
        message_json = json.dumps(message_data)  # Convert to JSON   # <<< DO NOT REMOVE >>>
        
        # Try to publish message to MQTT broker                                   # <<< DO NOT REMOVE >>>
        try:                                                                      # <<< DO NOT REMOVE >>>
            client.publish(TOPIC, message_json, retain=True) # Send MQTT payload  # <<< DO NOT REMOVE >>>
        
            # Print MQTT payload to Thonny's terminal/shell/console
            print(f"Published: {message_json}")
        except Exception as e:                                                    # <<< DO NOT REMOVE >>>
            print("Publish failed:",e)
        
        display.fill(0) # clears display
        display.text("Temprature: " + str(temperature_sensor_reading), 0, 0) # write text starting at x=0 and y=0
        display.show() # make the changes take effect
            
    else:
        display.fill(0) # clears display
        display.text("Please Scan RFID", 0, 0) # write text starting at x=0 and y=0
        display.show() # make the changes take effect

        
    
    # Determines how often the MQTT payload (i.e., sensorID and temperature reading) is sent
    sleep(0.5)  
