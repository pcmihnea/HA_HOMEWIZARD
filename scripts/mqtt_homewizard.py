import asyncio
import json
import struct
import traceback
from datetime import time as datetime
import time

import mqttapi as mqtt  # AppDaemon-specific API
import requests
import serial

DEBUG_PRINT = 0

PACKET_LEN = 23
SYNC_BYTE = 0x61
HEADER_BYTES = 0x159202120a10
STATUS_BIT_MASK = 0x04
HIGH_BATT_MASK = 0x01
TIMEOUT_SERIAL_READ = 5
TIMEOUT_HTTP_REQUEST = 1
KEEPALIVE_ENERGY_SWITCH = 60 * 5
KEEPALIVE_THERMOMETER = 60 * 10
KEEPALIVE_LEAK_DETECTOR = 60 * (60 * 3 + 10)
KEEPALIVE_SMOKE_DETECTOR = 60 * (60 * 25 + 10)

PRIVATE_CONFIG_PATH = '/config/apps'

PRIVATE_CONF = {}


class mqtt_homewizard(mqtt.Mqtt):

    async def mqtt_discovery(self):
        for device_id in PRIVATE_CONF['DEVICE_CODES']:
            device = PRIVATE_CONF['DEVICE_CODES'][device_id]
            sub_id = 0
            if device['type'] == 'hw_energy_switch':
                config = [
                    {"name": device['name'] + '_V',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.VOLT }}',
                     "device_class": 'voltage',
                     "state_class": 'measurement',
                     "unit_of_measurement": 'V',
                     "expire_after": KEEPALIVE_ENERGY_SWITCH},
                    {"name": device['name'] + '_A',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.AMP }}',
                     "device_class": 'current',
                     "state_class": 'measurement',
                     "unit_of_measurement": 'A',
                     "expire_after": KEEPALIVE_ENERGY_SWITCH},
                    {"name": device['name'] + '_W',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.WATT }}',
                     "device_class": 'power',
                     "state_class": 'measurement',
                     "unit_of_measurement": 'W',
                     "expire_after": KEEPALIVE_ENERGY_SWITCH}
                ]
            elif device['type'] == 'hw_thermometer':
                config = [
                    {"name": device['name'] + '_T',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.TEMP }}',
                     "device_class": 'temperature',
                     "state_class": 'measurement',
                     "unit_of_measurement": '°C',
                     "expire_after": KEEPALIVE_THERMOMETER},
                    {"name": device['name'] + '_H',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.HUMID }}',
                     "device_class": 'humidity',
                     "state_class": 'measurement',
                     "unit_of_measurement": '%',
                     "expire_after": KEEPALIVE_THERMOMETER},
                    {"name": device['name'] + '_B',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.BATT }}',
                     "device_class": 'battery',
                     "state_class": 'measurement',
                     "unit_of_measurement": '%',
                     "expire_after": KEEPALIVE_THERMOMETER}
                ]
            elif device['type'] == 'sw_leak_detector':
                config = [
                    {"name": device['name'] + '_S',
                     "state_topic": 'homeassistant/binary_sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.SENS }}',
                     "device_class": 'moisture',
                     "state_class": 'measurement',
                     "expire_after": KEEPALIVE_LEAK_DETECTOR},
                    {"name": device['name'] + '_B',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.BATT }}',
                     "device_class": 'battery',
                     "state_class": 'measurement',
                     "unit_of_measurement": '%',
                     "expire_after": KEEPALIVE_LEAK_DETECTOR},
                    {}
                ]
            elif device['type'] == 'sw_smoke_detector':
                config = [
                    {"name": device['name'] + '_S',
                     "state_topic": 'homeassistant/binary_sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.SENS }}',
                     "device_class": 'smoke',
                     "state_class": 'measurement',
                     "expire_after": KEEPALIVE_SMOKE_DETECTOR},
                    {"name": device['name'] + '_B',
                     "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                     "value_template": '{{ value_json.BATT }}',
                     "device_class": 'battery',
                     "state_class": 'measurement',
                     "unit_of_measurement": '%',
                     "expire_after": KEEPALIVE_SMOKE_DETECTOR},
                    {}
                ]
            else:
                continue
            for cfg in config:
                if bool(cfg):
                    cfg['unique_id'] = device['code'] + str(sub_id)
                    sub_id += 1
                    if 'binary' in cfg['state_topic']:
                        topic = 'binary_sensor/'
                    else:
                        topic = 'sensor/'
                    self.mqtt_publish('homeassistant/' + topic + cfg['name'] + '/config',
                                      payload=json.dumps(cfg), retain=True)
                    time.sleep(0.1)
        self.log('MQTT_DISCOVERY OK')

    def cloud_connect(self):
        try:
            cloud_auth = (PRIVATE_CONF['CLOUD_AUTH']['USERNAME'], PRIVATE_CONF['CLOUD_AUTH']['PASSWORD'])
            res = requests.get('https://api.homewizardeasyonline.com/v1/auth/devices', auth=cloud_auth,
                               timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code:
                raise Exception('HTTP result not 200')
            hw_id = json.loads(res.content)['devices'][0]['identifier'].split('HW_LINK', 1)[1]
            res = requests.post('https://api.homewizardeasyonline.com/v1/auth/token', auth=cloud_auth,
                                json={'device': 'HW_LINK' + hw_id}, timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code:
                raise Exception('HTTP result not 200')
            bearer_auth = {'Authorization': 'Bearer %s' % json.loads(res.content)['token']}
            device_url = 'https://' + hw_id + '.homewizard.link'
            res = requests.get(device_url + '/handshake', headers=bearer_auth, timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code:
                raise Exception('HTTP result not 200')
            res = requests.get(device_url + '/v24/home', headers=bearer_auth, timeout=TIMEOUT_HTTP_REQUEST)
            if 200 != res.status_code:
                raise Exception('HTTP result not 200')
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
                    if 'ok' == device['status']:
                        if device['type'] == 'hw_energy_switch':
                            value = json.dumps({'VOLT': device['state']['energy']['voltage'],
                                                'AMP': round(device['state']['energy']['amperage'] / 1000.0, 3),
                                                'WATT': device['state']['energy']['wattage']})
                        elif device['type'] == 'hw_thermometer':
                            if device['state']['low_battery']:
                                batt = 0
                            else:
                                batt = 100
                            value = json.dumps({'TEMP': device['state']['temperature'],
                                                'HUMID': device['state']['humidity'],
                                                'BATT': batt})
                        elif (device['type'] == 'sw_leak_detector') or (device['type'] == 'sw_smoke_detector'):
                            if 'ok' != device['state']['status']:
                                sense = 'ON'
                            else:
                                sense = 'OFF'
                            if device['state']['low_battery']:
                                batt = 0
                            else:
                                batt = 100
                            value = json.dumps({'BATT': batt})
                            try:
                                self.mqtt_publish('homeassistant/binary_sensor/' + device['name'] + '/state',
                                                  payload=json.dumps({'SENS': sense}))
                            except Exception:
                                self.log(traceback.format_exc())
                        else:
                            continue
                        try:
                            self.mqtt_publish('homeassistant/sensor/' + device['name'] + '/state', payload=value)
                        except Exception:
                            self.log(traceback.format_exc())
                self.log('CLOUD_POLL OK')
        except Exception:
            self.log(traceback.format_exc())

    async def cloud_sync(self, kwargs):
        try:
            global PRIVATE_CONF
            devices = self.cloud_connect()
            if DEBUG_PRINT:
                self.log(json.dumps(devices, indent=4))
            if bool(devices):
                dev_codes = {}
                for device in devices:
                    dev_codes[device['listen_code']] = {'name': device['name'], 'type': device['type'],
                                                        'code': device['code']}
                PRIVATE_CONF['DEVICE_CODES'] = dev_codes
                with open(PRIVATE_CONFIG_PATH + '/private_config.json', "r") as private_conf_file:
                    private_conf_dump = json.load(private_conf_file)
                private_conf_dump['HOMEWIZARD']['DEVICE_CODES'] = dev_codes
                with open(PRIVATE_CONFIG_PATH + '/private_config.json', "w") as private_conf_file:
                    json.dump(private_conf_dump, private_conf_file, ensure_ascii=False, indent=4)
                self.log('CLOUD_SYNC OK')
        except Exception:
            self.log(traceback.format_exc())
        await self.mqtt_discovery()
        await self.cloud_poll(kwargs)

    async def local_sampling(self, kwargs):
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
                                    if sensor_info['type'] == 'hw_energy_switch':
                                        buff = struct.unpack('<6xBHHx', sensor_data)
                                        value = json.dumps({'VOLT': buff[0], 'AMP': round(buff[1] / 1000.0, 3),
                                                            'WATT': buff[2]})
                                    elif sensor_info['type'] == 'hw_thermometer':
                                        buff = struct.unpack('<2xBxhB5x', sensor_data)
                                        if buff[0] & HIGH_BATT_MASK:
                                            batt = 100
                                        else:
                                            batt = 0
                                        value = json.dumps(
                                            {'TEMP': round(buff[1] / 10.0, 1), 'HUMID': buff[2], 'BATT': batt})
                                    elif (sensor_info['type'] == 'sw_leak_detector') or (
                                            sensor_info['type'] == 'sw_smoke_detector'):
                                        buff = struct.unpack('<2xB9x', sensor_data)
                                        if buff[0] & HIGH_BATT_MASK:
                                            low_batt = 'OFF'
                                        else:
                                            low_batt = 'ON'
                                        if buff[0] & STATUS_BIT_MASK:
                                            sense = 'ON'
                                        else:
                                            sense = 'OFF'
                                        value = json.dumps({'BATT': low_batt})
                                        try:
                                            self.mqtt_publish(
                                                'homeassistant/binary_sensor/' + sensor_info['name'] + '/state',
                                                payload=json.dumps({'SENS': sense}))
                                        except Exception:
                                            self.log(traceback.format_exc())
                                    else:
                                        value = {'UNDEFINED': str(sensor_data.hex())}
                                    try:
                                        self.mqtt_publish('homeassistant/sensor/' + sensor_info['name'] + '/state',
                                                          payload=value)
                                    except Exception:
                                        self.log(traceback.format_exc())
                            except KeyError:
                                pass  # sensor most likely offline
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
            with open(PRIVATE_CONFIG_PATH + '/private_config.json', "r") as private_conf_file:
                PRIVATE_CONF = json.load(private_conf_file)['HOMEWIZARD']
            cloud_polling_interval = PRIVATE_CONF['CLOUD_POLLING_INTERVAL']
            self.log('CLOUD_POLLING_INTERVAL=' + str(PRIVATE_CONF['CLOUD_POLLING_INTERVAL']))
            await self.run_in(self.cloud_sync, 1)
            await self.run_daily(self.cloud_sync, datetime(2, 0, 0))
            if cloud_polling_interval > 0:
                self.log('CLOUD_POLLING START')
                await self.run_every(self.cloud_poll, 'now+1', cloud_polling_interval)
            else:
                await self.run_in(self.local_sampling, 2)
        except Exception:
            self.log(traceback.format_exc())
