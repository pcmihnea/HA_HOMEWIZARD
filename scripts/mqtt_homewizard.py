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


class mqtt_homewizard(mqtt.Mqtt):

    def cloud_connect(self, cloud_auth):
        try:
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
            dev_config = json.loads(res.content)
        except Exception:
            dev_config = {}
            self.log(traceback.format_exc())
        return dev_config

    async def cloud_polling(self, kwargs):
        try:
            for device in self.cloud_connect(cloud_auth=kwargs['cloud_auth'])['devices']:
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
                        if 'ok' == device['state']['status']:
                            value = 'OFF'
                        else:
                            value = 'ON'
                        topic = 'binary_' + topic
                    case 'sw_smoke_detector':
                        if 'ok' == device['state']['status']:
                            value = 'OFF'
                        else:
                            value = 'ON'
                        topic = 'binary_' + topic
                    case _:
                        continue
                self.mqtt_publish('homeassistant/' + topic + device['name'] + '/state', payload=value)
            self.log('CLOUD_POLL OK')
        except Exception:
            self.log(traceback.format_exc())

    async def local_sampling(self, kwargs):
        ser = serial.Serial()
        try:
            rx_data = []
            ser = serial.Serial(kwargs['dev_config']['serial_port'], 115200, timeout=TIMEOUT_SERIAL_READ)
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
                                    sensor_info = kwargs['dev_config']['dev_codes'][format(data_elems[2], 'X')]
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
                                                value = 'OFF'
                                            else:
                                                value = 'ON'
                                            topic = 'binary_' + topic
                                        case 'sw_smoke_detector':
                                            # [0] = UNDEFINED, [1] = SMOKE
                                            buff = struct.unpack('<HB9x', sensor_data)
                                            if buff[1] & STATUS_BIT_MASK:
                                                value = 'OFF'
                                            else:
                                                value = 'ON'
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

    async def mqtt_discovery(self, kwargs):
        try:
            for dev_id in kwargs['dev_codes']:
                device = kwargs['dev_codes'][dev_id]
                topic = 'sensor/'
                match device['type']:
                    case 'hw_energy_switch':
                        config = [
                            {"name": device['name'] + '_V',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.VOLT }}',
                             "device_class": 'voltage', "unit_of_measurement": 'V'},
                            {"name": device['name'] + '_A',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.AMP }}',
                             "device_class": 'current', "unit_of_measurement": 'A'},
                            {"name": device['name'] + '_W',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.WATT }}',
                             "device_class": 'power', "unit_of_measurement": 'W'}
                        ]
                    case 'hw_thermometer':
                        config = [
                            {"name": device['name'] + '_T',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.TEMP }}',
                             "device_class": 'temperature', "unit_of_measurement": '°C'},
                            {"name": device['name'] + '_H',
                             "state_topic": 'homeassistant/sensor/' + device['name'] + '/state',
                             "value_template": '{{ value_json.HUMID }}',
                             "device_class": 'humidity', "unit_of_measurement": '%'},
                            {}
                        ]
                    case 'sw_leak_detector':
                        config = [
                            {"name": device['name'],
                             "state_topic": 'homeassistant/binary_sensor/' + device['name'] + '/state',
                             "device_class": 'moisture'},
                            {},
                            {}
                        ]
                        topic = 'binary_' + topic
                    case 'sw_smoke_detector':
                        config = [
                            {"name": device['name'],
                             "state_topic": 'homeassistant/binary_sensor/' + device['name'] + '/state',
                             "device_class": 'smoke'},
                            {},
                            {}
                        ]
                        topic = 'binary_' + topic
                    case _:
                        continue
                unique_id = 0
                for cfg in config:
                    if bool(cfg):
                        cfg['unique_id'] = device['code'] + str(unique_id)
                        unique_id += 1
                        self.mqtt_publish('homeassistant/' + topic + cfg['name'] + '/config',
                                          payload=json.dumps(cfg), retain=True)
            self.log('MQTT_DISCOVERY OK')
        except Exception:
            self.log(traceback.format_exc())

    async def initialize(self):
        try:
            f = open('/config/appdaemon/apps/private_config.json')
            private_conf = json.load(f)
            f.close()
            cloud_polling_interval = private_conf['HOMEWIZARD']['CLOUD_POLLING_INTERVAL']
            cloud_auth = (
                private_conf['HOMEWIZARD']['CLOUD_AUTH']['USERNAME'],
                private_conf['HOMEWIZARD']['CLOUD_AUTH']['PASSWORD'])
            self.log('CLOUD_POLLING_PERIOD ' + str(cloud_polling_interval))
            try:
                dev_codes = {}
                dev_cfg = self.cloud_connect(cloud_auth=cloud_auth)
                for device in dev_cfg['devices']:
                    dev_codes[device['listen_code']] = {'name': device['name'], 'type': device['type'],
                                                        'code': device['code']}
                if private_conf['HOMEWIZARD']['CLOUD_DEVICE_CODES_PRINT']:
                    self.log(json.dumps(dev_cfg, indent=2))
                    self.log(json.dumps(dev_codes, indent=2))
                await self.run_in(self.cloud_polling, 1, cloud_auth=cloud_auth)
                self.log('DEV_CFG OK')
            except Exception:
                dev_codes = private_conf['HOMEWIZARD']['BACKUP_DEVICE_CODES']
                self.log(traceback.format_exc())

            await self.run_in(self.mqtt_discovery, 1, dev_codes=dev_codes)
            if cloud_polling_interval > 0:
                self.log('CLOUD_POLLING START')
                await self.run_every(self.cloud_polling, 'now', cloud_polling_interval, cloud_auth=cloud_auth)
            elif bool(dev_codes):
                await self.run_daily(self.cloud_polling, time(0, 0, 0), cloud_auth=cloud_auth)
                await self.run_in(self.local_sampling, 2,
                                  dev_config={"serial_port": private_conf['HOMEWIZARD']['LOCAL_SERIAL_PORT'],
                                              "dev_codes": dev_codes})
            else:
                raise Exception('DEV_CODES')
        except Exception:
            self.log(traceback.format_exc())
