import asyncio
import json
import struct
import traceback
from datetime import time

import mqttapi as mqtt
import requests
import serial

PACKET_LEN = 23
SYNC_BYTE = 0x61
HEADER_BYTES = 0x159202120a10
STATUS_BIT_MASK = 0x04
TIMEOUT_SERIAL_READ = 5
TIMEOUT_HTTP_REQUEST = 2
PRIVATE_CONFIG_PATH = '/config/appdaemon/apps/private_config.json'

PRIVATE_CONF = {}


class mqtt_homewizard(mqtt.Mqtt):

    async def mqtt_discovery(self):
        try:
            for device_id in PRIVATE_CONF['DEVICE_CODES']:
                device = PRIVATE_CONF['DEVICE_CODES'][device_id]
                topic = 'sensor/'
                sub_id = 0
                match device['type']:
                    case 'hw_energy_switch':
                        config = [
                            {"name": device['name'] + '_V',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.VOLT }}',
                             "device_class": 'voltage', "unit_of_measurement": 'V',
                             "expire_after": 600},
                            {"name": device['name'] + '_A',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.AMP }}',
                             "device_class": 'current', "unit_of_measurement": 'A',
                             "expire_after": 600},
                            {"name": device['name'] + '_W',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.WATT }}',
                             "device_class": 'power', "unit_of_measurement": 'W',
                             "expire_after": 600}]
                    case 'hw_thermometer':
                        config = [
                            {"name": device['name'] + '_T',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.TEMP }}',
                             "device_class": 'temperature', "unit_of_measurement": '°C',
                             "expire_after": 600},
                            {"name": device['name'] + '_H',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.HUMID }}',
                             "device_class": 'humidity', "unit_of_measurement": '%',
                             "expire_after": 600}, {}]
                    case 'sw_leak_detector':
                        config = [
                            {"name": device['name'],
                             "state_topic": 'homeassistant/binary_sensor/' + device['name'] + '/state',
                             "device_class": 'moisture',
                             "expire_after": 600}, {}, {}]
                        topic = 'binary_' + topic
                    case 'sw_smoke_detector':
                        config = [
                            {"name": device['name'],
                             "state_topic": 'homeassistant/binary_sensor/' + device['name'] + '/state',
                             "device_class": 'smoke',
                             "expire_after": 600}, {}, {}]
                        topic = 'binary_' + topic
                    case _:
                        continue
                for cfg in config:
                    if bool(cfg):
                        cfg['unique_id'] = device['code'] + str(sub_id)
                        sub_id += 1
                        self.mqtt_publish('homeassistant/' + topic + cfg['name'] + '/config',
                                          payload=json.dumps(cfg), retain=True)
            self.log('MQTT_DISCOVERY OK')
        except Exception:
            self.log(traceback.format_exc())

    def cloud_connect(self):
        try:
            cloud_auth = (PRIVATE_CONF['CLOUD_AUTH']['USERNAME'], PRIVATE_CONF['CLOUD_AUTH']['PASSWORD'])
            res = requests.get('https://api.homewizardeasyonline.com/v1/auth/devices', auth=cloud_auth,
                               timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code: raise Exception('HTTP result not 200')
            hw_id = json.loads(res.content)['devices'][0]['identifier'].split('HW_LINK', 1)[1]
            res = requests.post('https://api.homewizardeasyonline.com/v1/auth/token', auth=cloud_auth,
                                json={'device': 'HW_LINK' + hw_id}, timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code: raise Exception('HTTP result not 200')
            bearer_auth = {'Authorization': 'Bearer %s' % json.loads(res.content)['token']}
            device_url = 'https://' + hw_id + '.homewizard.link'
            res = requests.get(device_url + '/handshake', headers=bearer_auth, timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code: raise Exception('HTTP result not 200')
            res = requests.get(device_url + '/v24/home', headers=bearer_auth, timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code: raise Exception('HTTP result not 200')
            devices = json.loads(res.content)['devices']
            self.log('CLOUD_CONNECT OK')
        except Exception:
            devices = {}
            self.log(traceback.format_exc())
        return devices

    async def cloud_poll(self, kwargs):
        try:
            devices = self.cloud_connect()
            if bool(devices):
                for device in devices:
                    topic = 'sensor/'
                    match device['type']:
                        case 'hw_energy_switch':
                            value = json.dumps({'VOLT': device['state']['energy']['voltage'],
                                                'AMP': round(device['state']['energy']['amperage'] / 1000.0, 3),
                                                'WATT': device['state']['energy']['wattage']})
                        case 'hw_thermometer':
                            value = json.dumps({'TEMP': device['state']['temperature'],
                                                'HUMID': device['state']['humidity']})
                        case 'sw_leak_detector':
                            if 'ok' != device['state']['status']:
                                value = 'ON'
                            else:
                                value = 'OFF'
                            topic = 'binary_' + topic
                        case 'sw_smoke_detector':
                            if 'ok' != device['state']['status']:
                                value = 'ON'
                            else:
                                value = 'OFF'
                            topic = 'binary_' + topic
                        case _:
                            continue
                    self.mqtt_publish('homeassistant/' + topic + device['name'] + '/state', payload=value)
                self.log('CLOUD_POLL OK')
        except Exception:
            self.log(traceback.format_exc())

    async def cloud_sync(self, kwargs):
        try:
            global PRIVATE_CONF
            devices = self.cloud_connect()
            if bool(devices):
                dev_codes = {}
                for device in devices:
                    dev_codes[device['listen_code']] = {'name': device['name'], 'type': device['type'],
                                                        'code': device['code']}
                PRIVATE_CONF['DEVICE_CODES'] = dev_codes
                with open(PRIVATE_CONFIG_PATH, "r") as private_conf_file:
                    private_conf_dump = json.load(private_conf_file)
                private_conf_dump['HOMEWIZARD']['DEVICE_CODES'] = dev_codes
                with open(PRIVATE_CONFIG_PATH, "w") as private_conf_file:
                    json.dump(private_conf_dump, private_conf_file, ensure_ascii=False, indent=4)
                self.log('CLOUD_SYNC OK')
        except Exception:
            self.log(traceback.format_exc())
        await self.mqtt_discovery()
        await self.cloud_poll(kwargs)

    async def serial_sampling(self, kwargs):
        ser = serial.Serial()
        try:
            rx_data = []
            ser = serial.Serial(PRIVATE_CONF['SERIAL_PORT'], 115200, timeout=TIMEOUT_SERIAL_READ)
            ser.flushInput()
            self.log('LOCAL_SAMPLING START')
            while ser.is_open:
                await asyncio.sleep(0.05)
                if ser.inWaiting() > 0:
                    rx_data += ser.read(ser.inWaiting())
                    if SYNC_BYTE == rx_data[0] or len(rx_data) >= PACKET_LEN:
                        if len(rx_data) == PACKET_LEN:
                            try:
                                # [0:1] = HEADER, [2] = CODE [3] = DATA, [4] = CRC
                                data_elems = struct.unpack('>HLL12sB', bytearray(rx_data))
                                crc = 0
                                for data in rx_data[:-1]:
                                    crc = (crc + data) & 0xFF
                                crc = (0x100 - crc) & 0xFF
                                if crc == data_elems[4] and HEADER_BYTES == (
                                        (data_elems[0] << 32) | data_elems[1]):
                                    sensor_info = PRIVATE_CONF['DEVICE_CODES'][format(data_elems[2], 'X')]
                                    sensor_data = bytearray(data_elems[3])
                                    topic = 'sensor/'
                                    match sensor_info['type']:
                                        case 'hw_energy_switch':
                                            # [0] = UNDEFINED, [1] = VOLT, [2] = AMP, [3] = WATT
                                            buff = struct.unpack('<6sBHHx', sensor_data)
                                            value = json.dumps({'VOLT': buff[1], 'AMP': round(buff[2] / 1000.0, 3),
                                                                'WATT': buff[3]})
                                        case 'hw_thermometer':
                                            # [0] = UNDEFINED, [1] = TEMP, [2] = HUMID
                                            buff = struct.unpack('<IhB5x', sensor_data)
                                            value = json.dumps({'TEMP': round(buff[1] / 10.0, 1), 'HUMID': buff[2]})
                                        case 'sw_leak_detector':
                                            # [0] = UNDEFINED, [1] = LEAK
                                            buff = struct.unpack('<HH8x', sensor_data)
                                            if buff[1] & STATUS_BIT_MASK:
                                                value = 'ON'
                                            else:
                                                value = 'OFF'
                                            topic = 'binary_' + topic
                                        case 'sw_smoke_detector':
                                            # [0] = UNDEFINED, [1] = SMOKE
                                            buff = struct.unpack('<HB9x', sensor_data)
                                            if buff[1] & STATUS_BIT_MASK:
                                                value = 'ON'
                                            else:
                                                value = 'OFF'
                                            topic = 'binary_' + topic
                                        case _:
                                            value = {'UNDEFINED': sensor_data.hex()}
                                    self.mqtt_publish('homeassistant/' + topic + sensor_info['name'] + '/state',
                                                      payload=value)
                            except Exception:
                                self.log(traceback.format_exc())
                        rx_data = []
                        ser.flushInput()
        except Exception:
            self.log(traceback.format_exc())
        try:
            ser.close()
        except Exception:
            pass

    async def initialize(self):
        try:
            global PRIVATE_CONF
            with open(PRIVATE_CONFIG_PATH, "r") as private_conf_file:
                PRIVATE_CONF = json.load(private_conf_file)['HOMEWIZARD']
            cloud_polling_interval = PRIVATE_CONF['CLOUD_POLLING_INTERVAL']
            self.log('CLOUD_POLLING_INTERVAL=' + str(PRIVATE_CONF['CLOUD_POLLING_INTERVAL']))
            await self.run_in(self.cloud_sync, 1)
            await self.run_daily(self.cloud_sync, time(1, 0, 0))
            if cloud_polling_interval > 0:
                self.log('CLOUD_POLLING START')
                await self.run_every(self.cloud_poll, 'now+2', cloud_polling_interval)
            else:
                await self.run_in(self.serial_sampling, 2)
        except Exception:
            self.log(traceback.format_exc())
