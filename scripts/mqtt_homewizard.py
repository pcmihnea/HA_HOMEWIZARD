import asyncio
import json
import struct
import traceback

import mqttapi as mqtt
import requests
import serial

PACKET_LEN = 23
SYNC_BYTE = 0x61
HEADER_BYTES = 0x159202120a10
STATUS_BIT_MASK = 0x04
TIMEOUT_SERIAL_READ = 5
TIMEOUT_HTTP_REQUEST = 2

PRIVATE_CONFIG = {}


class mqtt_homewizard(mqtt.Mqtt):

    def cloud_connect(self):
        try:
            cloud_auth = (
                PRIVATE_CONFIG['HOMEWIZARD']['CLOUD_AUTH']['USERNAME'],
                PRIVATE_CONFIG['HOMEWIZARD']['CLOUD_AUTH']['PASSWORD'])
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
            for device in self.cloud_connect()['devices']:
                match device['type']:
                    case 'hw_energy_switch':
                        value = {'VOLT': device['state']['energy']['voltage'],
                                 'AMP': round(device['state']['energy']['amperage'] / 1000.0, 3),
                                 'WATT': device['state']['energy']['wattage']}
                    case 'hw_thermometer':
                        value = {'TEMP': device['state']['temperature'], 'HUMID': device['state']['humidity']}
                    case 'sw_leak_detector':
                        value = {'LEAK': device['state']['status']}
                    case 'sw_smoke_detector':
                        value = {'SMOKE': device['state']['status']}
                    case _:
                        continue
                self.mqtt_publish('homewizard/sensors/' + device['name'], payload=json.dumps(value))
            self.log('CLOUD_POLL OK')
        except Exception:
            self.log(traceback.format_exc())

    async def local_sampling(self, dev_codes):
        ser = serial.Serial()
        try:
            rx_data = []
            ser = serial.Serial(PRIVATE_CONFIG['HOMEWIZARD']['LOCAL_SERIAL_PORT'], 115200, timeout=TIMEOUT_SERIAL_READ)
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
                                    sensor_info = dev_codes['dev_codes'][format(data_elems[2], 'X')]
                                    sensor_data = bytearray(data_elems[3])
                                    match sensor_info['type']:
                                        case 'hw_energy_switch':
                                            # [0] = UNDEFINED, [1] = VOLT, [2] = AMP, [3] = WATT
                                            buff = struct.unpack('<6sBHHx', sensor_data)
                                            value = {'VOLT': buff[1], 'AMP': round(buff[2] / 1000.0, 3),
                                                     'WATT': buff[3]}
                                        case 'hw_thermometer':
                                            # [0] = UNDEFINED, [1] = TEMP, [2] = HUMID
                                            buff = struct.unpack('<IhB5x', sensor_data)
                                            value = {'TEMP': round(buff[1] / 10.0, 1), 'HUMID': buff[2]}
                                        case 'sw_leak_detector':
                                            # [0] = UNDEFINED, [1] = LEAK
                                            buff = struct.unpack('<HH8x', sensor_data)
                                            if buff[1] & STATUS_BIT_MASK:
                                                val = 'ok'
                                            else:
                                                val = 'nok'
                                            value = {'LEAK': val}
                                        case 'sw_smoke_detector':
                                            # [0] = UNDEFINED, [1] = SMOKE
                                            buff = struct.unpack('<HB9x', sensor_data)
                                            if buff[1] & STATUS_BIT_MASK:
                                                val = 'ok'
                                            else:
                                                val = 'nok'
                                            value = {'SMOKE': val}
                                        case _:
                                            value = {'UNDEFINED': sensor_data.hex()}
                                    self.mqtt_publish('homewizard/sensors/' + sensor_info['name'],
                                                      payload=json.dumps(value))
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
            global PRIVATE_CONFIG

            f = open('/config/appdaemon/apps/private_config.json')
            PRIVATE_CONFIG = json.load(f)
            f.close()
            if bool(PRIVATE_CONFIG['HOMEWIZARD']['CLOUD_AUTH']):
                pass
            CLOUD_POLLING_INTERVAL = PRIVATE_CONFIG['HOMEWIZARD']['CLOUD_POLLING_INTERVAL']
            self.log('CLOUD_POLLING_PERIOD ' + str(CLOUD_POLLING_INTERVAL))
            self.log('CONFIG_FILE OK')
            try:
                dev_codes = {}
                dev_cfg = self.cloud_connect()
                for device in dev_cfg['devices']:
                    dev_codes[device['listen_code']] = {'name': device['name'], 'type': device['type'],
                                                        'code': device['code']}
                if PRIVATE_CONFIG['HOMEWIZARD']['CLOUD_DEVICE_CODES_PRINT']:
                    self.log(json.dumps(dev_cfg, indent=2))
                    self.log(json.dumps(dev_codes, indent=2))
                self.log('DEV_CFG OK')
            except Exception:
                dev_codes = PRIVATE_CONFIG['HOMEWIZARD']['BACKUP_DEVICE_CODES']
                self.log(traceback.format_exc())

            if CLOUD_POLLING_INTERVAL > 0:
                self.log('CLOUD_POLLING START')
                await self.run_every(self.cloud_polling, 'now', CLOUD_POLLING_INTERVAL)
            else:
                if bool(dev_codes):
                    await self.run_in(self.local_sampling, 2, dev_codes=dev_codes)
                else:
                    raise Exception('DEV_CODES')
        except Exception:
            self.log(traceback.format_exc())
