import os
import sys
import time
import json
import pymysql
import requests
import datetime
from zabbix.api import ZabbixAPI
from multiprocessing import Pool
from influxdb import InfluxDBClient

home_path = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.path.pardir))
config_file = os.path.join(home_path, 'config.json')
with open(config_file) as f:
    config = json.loads(f.read())

zapi = ZabbixAPI(url=config['zabbix']['url'], user=config['zabbix']['user'], password=config['zabbix']['password'])


class BaseData(object):
    def __init__(self, site, begin_time, end_time, interval):
        self.client = InfluxDBClient(config['influxdb']['url'], config['influxdb']['port'], config['influxdb']['user'],
                                     config['influxdb']['password'], config['influxdb']['db'])
        self.site = site
        self.begin_time = begin_time
        self.end_time = end_time
        self.interval = interval

    def get_influx(self, query):
        result = self.client.query(query)
        raw_data = result.raw.get('series')
        if raw_data:
            return raw_data[0].get('values')

    def process_hour(self, hour):
        hour = int(hour)
        if hour <= 16:
            return str(hour + 8)
        else:
            return str(hour + 8 - 24)

    def get_date(self):
        date_list = []
        if self.interval == '24h':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%dT%H:%M:%S.00Z")
            while begin_time < end_time:
                date_str = begin_time.strftime("%Y-%m-%d")
                date_list.append(date_str)
                begin_time += datetime.timedelta(days=1)
        elif self.interval == '1h':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%dT%H:%M:%S.00Z")
            begin_time += datetime.timedelta(hours=8)
            end_time += datetime.timedelta(hours=8)
            while begin_time < end_time:
                date_str = begin_time.strftime("%Y-%m-%d %H")
                date_list.append(date_str)
                begin_time += datetime.timedelta(hours=1)
        elif self.interval == '5m':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%dT%H:%M:%S.00Z")
            begin_time += datetime.timedelta(hours=8)
            end_time += datetime.timedelta(hours=8)
            while begin_time < end_time:
                date_str = begin_time.strftime("%Y-%m-%d %H:%M")
                date_list.append(date_str)
                begin_time += datetime.timedelta(minutes=5)
        elif self.interval == '1m':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%dT%H:%M:%S.00Z")
            begin_time += datetime.timedelta(hours=8)
            end_time += datetime.timedelta(hours=8)
            while begin_time < end_time:
                date_str = begin_time.strftime("%Y-%m-%d %H:%M")
                date_list.append(date_str)
                begin_time += datetime.timedelta(minutes=1)
        return date_list

    def process_data(self, query):
        data = self.get_influx(query)
        value = []
        if data:
            date = self.get_date()
            for item in data:
                value.append(item[1])
        else:
            date = self.get_date()
            for i in range(len(date)):
                value.append(0)
        return {"date": date, "value": value}


class EventData(BaseData):
    def __init__(self, site, begin_time, end_time, interval):
        super(EventData, self).__init__(site, begin_time, end_time, interval)
        if self.interval in ['1m', '5m', '1h']:
            delta = datetime.timedelta(hours=8)
            self.begin_time = datetime.datetime.strptime(self.begin_time, '%Y-%m-%d %H:%M') - delta
            self.begin_time = self.begin_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
            self.end_time = datetime.datetime.strptime(self.end_time, '%Y-%m-%d %H:%M') - delta
            self.end_time = self.end_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
        elif self.interval in ['24h']:
            self.begin_time = datetime.datetime.strptime(self.begin_time, '%Y-%m-%d %H:%M')
            self.begin_time = self.begin_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
            self.end_time = datetime.datetime.strptime(self.end_time, '%Y-%m-%d %H:%M')
            self.end_time = self.end_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')

    def get_data(self):
        query = '''select count(value) from \"yms.event\" where time>'{}' and time < '{}' \
                and app='{}' group by time({})'''.format(self.begin_time, self.end_time, self.site, self.interval)
        return self.process_data(query)


class LogData(BaseData):
    def __init__(self, site, begin_time, end_time, interval):
        super(LogData, self).__init__(site, begin_time, end_time, interval)
        if self.interval in ['1m', '5m', '1h']:
            delta = datetime.timedelta(hours=8)
            self.begin_time = datetime.datetime.strptime(self.begin_time, '%Y-%m-%d %H:%M') - delta
            self.begin_time = self.begin_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
            self.end_time = datetime.datetime.strptime(self.end_time, '%Y-%m-%d %H:%M') - delta
            self.end_time = self.end_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
        elif self.interval in ['24h']:
            self.begin_time = datetime.datetime.strptime(self.begin_time, '%Y-%m-%d %H:%M')
            self.begin_time = self.begin_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
            self.end_time = datetime.datetime.strptime(self.end_time, '%Y-%m-%d %H:%M')
            self.end_time = self.end_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')

    def get_data(self):
        data = {}
        query = '''select sum(value) from "{}" where time>'{}' and time < '{}' and app='{}' group by time({})''' \
            .format("log.pv", self.begin_time, self.end_time, self.site, self.interval)
        data['pv'] = self.process_data(query).get("value")
        data['date'] = self.process_data(query).get("date")
        query = '''select sum(value) from "{}" where time>'{}' and time < '{}' and app='{}' group by time({})''' \
            .format("log.4XX", self.begin_time, self.end_time, self.site, self.interval)
        data['4xx'] = self.process_data(query).get("value")
        query = '''select sum(value) from "{}" where time>'{}' and time < '{}' and app='{}' group by time({})''' \
            .format("log.5XX", self.begin_time, self.end_time, self.site, self.interval)
        data['5xx'] = self.process_data(query).get("value")
        return data


class AppData(BaseData):
    def __init__(self, site, begin_time, end_time, interval):
        super(AppData, self).__init__(site, begin_time, end_time, interval)
        if self.interval in ['1m', '5m', '1h']:
            delta = datetime.timedelta(hours=8)
            self.begin_time = datetime.datetime.strptime(self.begin_time, '%Y-%m-%d %H:%M') - delta
            self.begin_time = self.begin_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
            self.end_time = datetime.datetime.strptime(self.end_time, '%Y-%m-%d %H:%M') - delta
            self.end_time = self.end_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
        elif self.interval in ['24h']:
            self.begin_time = datetime.datetime.strptime(self.begin_time, '%Y-%m-%d %H:%M')
            self.begin_time = self.begin_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')
            self.end_time = datetime.datetime.strptime(self.end_time, '%Y-%m-%d %H:%M')
            self.end_time = self.end_time.strftime('%Y-%m-%dT%H:%M:%S.00Z')

    def get_influx(self, query):
        result = self.client.query(query)
        raw_data = result.raw.get('series')
        if raw_data:
            return raw_data

    def get_top(self):
        query = '''select sum(pv) from "app.time" where time>'{}' and time < '{}' and app='{}' group by counter ''' \
            .format(self.begin_time, self.end_time, self.site)
        data = self.get_influx(query)
        top_pv = []
        if data:
            for i in data:
                top_pv.append([i.get('tags').get('counter'), i.get('values')[0][-1]])
            return reversed(sorted(top_pv, key=lambda x: x[1], reverse=True)[:10])

    def get_data(self):
        data = {'api': [], '0ms-99ms': [], '100ms-199ms': [], '200ms-299ms': [], '300ms-499ms': [], '500ms-999ms': [],
                '1s+': []}
        if self.get_date() and self.get_top():
            for item in self.get_top():
                query = """select sum(Lt10) as "Lt10",sum(Lt20) as "Lt20",sum(Lt50) as "Lt50",sum(Lt100) as "Lt100",\
                           sum(Lt200) as "Lt200",sum(Lt300) as "Lt300",sum(Lt500) as "Lt500",sum(Lt1000) as "Lt1000",\
                           sum(Lt2000) as "Lt2000",sum(Lt3000) as "Lt3000",sum(Lt5000) as "Lt5000",\
                           sum(Lt10000) as "Lt10000",sum(Gt10000) as "Gt10000" from "app.time" where time>'{}' and\
                           time < '{}' and app='{}' and counter='{}'""".format \
                    (self.begin_time, self.end_time, self.site, item[0])
                result = self.get_influx(query)
                result = result[0].get('values')[0][1:]
                tmp_data = []
                for i in result:
                    if i:
                        tmp_data.append(i)
                    else:
                        tmp_data.append(0)
                data['api'].append(item[0])
                data['0ms-99ms'].append(sum(tmp_data[:4]))
                data['100ms-199ms'].append(tmp_data[4])
                data['200ms-299ms'].append(tmp_data[5])
                data['300ms-499ms'].append(tmp_data[6])
                data['500ms-999ms'].append(tmp_data[7])
                data['1s+'].append(sum(tmp_data[8:]))
        return {'legend': data['api'], 'data': [{'name': '0ms-99ms', 'data': data['0ms-99ms']},
                                                {'name': '100ms-199ms', 'data': data['100ms-199ms']},
                                                {'name': '200ms-299ms', 'data': data['200ms-299ms']},
                                                {'name': '300ms-499ms', 'data': data['300ms-499ms']},
                                                {'name': '500ms-999ms', 'data': data['500ms-999ms']},
                                                {'name': '1s+', 'data': data['1s+']}]}


class ZabbixData(BaseData):
    def get_date(self):
        date_list = []
        if self.interval == '24h':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%d %H:%M")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M")
            while begin_time < end_time:
                date_str = begin_time.strftime("%d")
                date_list.append(date_str)
                begin_time += datetime.timedelta(days=1)
        elif self.interval == '1h':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%d %H:%M")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M")
            while begin_time < end_time:
                date_str = begin_time.strftime("%H")
                date_list.append(date_str)
                begin_time += datetime.timedelta(hours=1)
        elif self.interval == '5m':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%d %H:%M")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M")
            while begin_time < end_time:
                hour = begin_time.strftime("%H")
                minite = begin_time.strftime("%M")
                date_str = hour + ':' + minite
                date_list.append(date_str)
                begin_time += datetime.timedelta(minutes=5)
        elif self.interval == '1m':
            begin_time = datetime.datetime.strptime(self.begin_time, "%Y-%m-%d %H:%M")
            end_time = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M")
            while begin_time < end_time:
                hour = begin_time.strftime("%H")
                minite = begin_time.strftime("%M")
                date_str = hour + ':' + minite
                date_list.append(date_str)
                begin_time += datetime.timedelta(minutes=1)
        return date_list

    def get_ipaddresses(self):
        cmdb_headers = {'Authorization': config['cmdb']['Authorization']}
        r = requests.get(
            '{}?application__name={}&environment__name=Production'.format(config['cmdb']['url'], self.site),
            headers=cmdb_headers)
        ipaddresses = []
        for i in r.json().get('results'):
            ipaddresses.extend(i.get('ipaddresses', []))
        return ipaddresses

    def get_hosts(self):
        hosts = {}
        ipaddresses = self.get_ipaddresses()
        if ipaddresses:
            for item in zapi.host.get(output=["host", "hostid"], filter={"host": ipaddresses}):
                hosts[item.get('hostid')] = item.get('host')
        return hosts

    def process_values(self, key, values):
        value_list = []
        if key in ['netin', 'netout']:
            if self.interval == '24h':
                value_list = [round(sum(values[i:i + 1440]) / 1440 / 1024, 1) for i in range(0, len(values), 1440)]
            elif self.interval == '1h':
                value_list = [round(sum(values[i:i + 60]) / 60 / 1024, 1) for i in range(0, len(values), 60)]
            elif self.interval == '5m':
                value_list = [round(sum(values[i:i + 5]) / 5 / 1024, 1) for i in range(0, len(values), 5)]
            elif self.interval == '1m':
                value_list = [round(values[i] / 1024, 1) for i in range(0, len(values))]
        elif key in ['cpuload']:
            if self.interval == '24h':
                value_list = [round(sum(values[i:i + 1440]) / 1440, 5) for i in range(0, len(values), 1440)]
            elif self.interval == '1h':
                value_list = [round(sum(values[i:i + 60]) / 60, 5) for i in range(0, len(values), 60)]
            elif self.interval == '5m':
                value_list = [round(sum(values[i:i + 5]) / 5, 5) for i in range(0, len(values), 5)]
            elif self.interval == '1m':
                value_list = [round(values[i], 5) for i in range(0, len(values))]
        elif key in ['cpupro', 'iis']:
            if self.interval == '24h':
                value_list = [round(sum(values[i:i + 1440]) / 1440, 2) for i in range(0, len(values), 1440)]
            elif self.interval == '1h':
                value_list = [round(sum(values[i:i + 60]) / 60, 2) for i in range(0, len(values), 60)]
            elif self.interval == '5m':
                value_list = [round(sum(values[i:i + 5]) / 5, 2) for i in range(0, len(values), 5)]
            elif self.interval == '1m':
                value_list = [round(values[i], 2) for i in range(0, len(values))]
        return value_list

    def get_zabbix(self, key):
        result = {}
        hosts = self.get_hosts()
        if hosts:
            items = {}
            if key in ['net.if.in[Intel(R) PRO/1000 MT Network Connection]',
                       'net.if.out[Intel(R) PRO/1000 MT Network Connection]']:
                for item in zapi.item.get(output=["hostid", "itemids"], hostids=list(hosts.keys()),
                                          search={"key_": key}):
                    items[item.get('itemid')] = item.get('hostid')
                data = zapi.history.get(history=3, itemids=list(items.keys()),
                                        time_from=time.mktime(time.strptime(self.begin_time, "%Y-%m-%d %H:%M")),
                                        time_till=time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M")))
            else:
                for item in zapi.item.get(output=["hostid", "itemids"], hostids=list(hosts.keys()),
                                          search={"key_": key}):
                    items[item.get('itemid')] = item.get('hostid')
                data = zapi.history.get(history=0, itemids=list(items.keys()),
                                        time_from=time.mktime(time.strptime(self.begin_time, "%Y-%m-%d %H:%M")),
                                        time_till=time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M")))
            for i in data:
                if result.get(hosts.get(items.get(i.get('itemid')))):
                    result[hosts.get(items.get(i.get('itemid')))].append(float(i.get('value')))
                else:
                    result[hosts.get(items.get(i.get('itemid')))] = [float(i.get('value'))]
        return result

    def get_data(self):
        data = {}
        keys = {"cpuload": "system.cpu.load[percpu,avg5",
                "cpupro": "perf_counter[\Processor(_Total)\% Processor Time]",
                "iis": 'perf_counter["\Web Service(_Total)\Current Connections",300]',
                "netin": 'net.if.in[Intel(R) PRO/1000 MT Network Connection]',
                "netout": 'net.if.out[Intel(R) PRO/1000 MT Network Connection]'}

        p = Pool(5)
        for k, v in keys.items():
            data[k] = p.apply_async(self.get_zabbix, args=(v,))
        p.close()
        p.join()
        for k, v in keys.items():
            data[k] = data[k].get()

        for k, v in data.items():
            for name in v.keys():
                data[k][name] = self.process_values(k, data[k][name])
        data['data'] = [data['cpuload'], data['cpupro'], data['iis'], data['netin'], data['netout']]
        return {'legend': self.get_date(),
                'data': [{'name': 'cpuload', 'alias': 'CPU load(5m)', 'data': data['cpuload']},
                         {'name': 'cpupro', 'alias': 'CPU 占比', 'data': data['cpupro']},
                         {'name': 'iis', 'alias': 'IIS连接数', 'data': data['iis']},
                         {'name': 'netin', 'alias': '网络流入(KB)', 'data': data['netin']},
                         {'name': 'netout', 'alias': '网络流出(KB)', 'data': data['netout']}]}


class ReleaseData(object):
    def __init__(self, site, begin_time, end_time):
        self.site = site
        self.begin_time = begin_time
        self.end_time = end_time
        delta = datetime.timedelta(hours=8)
        self.begin_time = datetime.datetime.strptime(self.begin_time, '%Y-%m-%d %H:%M') - delta
        self.begin_time = self.begin_time.strftime('%Y-%m-%dT%H:%M:%S.00')
        self.end_time = datetime.datetime.strptime(self.end_time, '%Y-%m-%d %H:%M') - delta
        self.end_time = self.end_time.strftime('%Y-%m-%dT%H:%M:%S.00')
        self.conn = pymysql.connect(host=config['release']['host'], user=config['release']['user'],
                                    passwd=config['release']['passwd'], db=config['release']['db'],
                                    port=config['release']['port'], charset=config['release']['charset'])

    def get_data(self):
        cur = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = "select * from api_publishrecord where app_name='{}' and publish_time>'{}' and publish_time<'{}';". \
            format(self.site, self.begin_time, self.end_time)
        cur.execute(sql)
        data = {'prod': {'total': 0, 'success': 0, 'failed': 0}, 'stag': {'total': 0, 'success': 0, 'failed': 0},
                'roll': {'total': 0, 'success': 0, 'failed': 0}}
        for item in cur.fetchall():
            if item.get('app_evn') == 'prod':
                if item.get('app_result') == '完成':
                    data['prod']['success'] += 1
                elif item.get('app_result') == '失败':
                    data['prod']['failed'] += 1
            elif item.get('app_evn') == 'stag':
                if item.get('app_result') == '完成':
                    data['stag']['success'] += 1
                elif item.get('app_result') == '失败':
                    data['stag']['failed'] += 1
            elif item.get('app_evn') == 'roll':
                if item.get('app_result') == '完成':
                    data['roll']['success'] += 1
                elif item.get('app_result') == '失败':
                    data['roll']['failed'] += 1
        data['prod']['total'] = data['prod']['success'] + data['prod']['failed']
        data['stag']['total'] = data['stag']['success'] + data['stag']['failed']
        data['roll']['total'] = data['roll']['success'] + data['roll']['failed']
        return data


class AlarmData(object):
    def __init__(self, site, begin_time, end_time):
        self.site = site
        self.begin_time = begin_time
        self.end_time = end_time
        self.url = config['alarm']['url']
        self.search_url = config['alarm']['search_url']
        self.cookies = config['alarm']['cookies']
        requests.post(config['alarm']['login_url'], data={'UserName': config['alarm']['username'],
                                                          'Password': config['alarm']['password']},
                      cookies=self.cookies)

    def process_data(self, tmp_data):
        data = []
        for i in tmp_data['Result']:
            data.append(i.get('Value'))
        return data

    def get_message(self, level):
        url = '{}?BeginTime={}&EndTime={}&ErrorLevel={}&ExceptionName=&MessageKey=&IP=&AppId={}&xPagePos=0&xPageSize=10\
        &items_per_page=20'.format(self.search_url, self.begin_time, self.end_time, level, self.site)
        r = requests.get(url, cookies=self.cookies)
        result = r.json().get('data')
        data = []
        for item in result:
            data.append("Title:{}\nMessage:{}\nType:{}\n".format \
                            (item.get('Title'), item.get('Message'), item.get('ExceptionName')))
        return data

    def get_data(self):
        data = {'data': [{'name': 'critical', 'alias': '严重级错误', 'level': 1, 'data': [], 'message': []},
                         {'name': 'normal', 'alias': '普通级错误', 'level': 2, 'data': [], 'message': []},
                         {'name': 'alarm', 'alias': '警告级错误', 'level': 3, 'data': [], 'message': []}],
                'legend': []}
        for item in data['data']:
            url = "{}?BeginTime={}&EndTime={}&AppId={}&ErrorLevel={}".format(self.url, self.begin_time, self.end_time,
                                                                             self.site, item.get('level'))
            r = requests.get(url, cookies=self.cookies)
            if r.json().get('Result'):
                if not data.get('legend'):
                    for i in r.json().get('Result'):
                        data['legend'].append(i.get('Key'))
                item['data'] = self.process_data(r.json())
        return data


class MessageData(BaseData):
    def __init__(self, site, begin_time, end_time, interval=None):
        super(MessageData, self).__init__(site, begin_time, end_time, interval)
        self.site = site
        self.begin_time = begin_time
        self.end_time = end_time

    def get_data(self):
        query = '''select message from "yms.event" where time>\'{}\' and time < \'{}\' and app=\'{}\'''' \
            .format(self.begin_time,
                    self.end_time, self.site)
        data = self.get_influx(query)
        if data:
            return [i[1] for i in data]
        else:
            return []


if __name__ == '__main__':
    event = AlarmData('app.ymatou.com', begin_time='2016-12-08 0:00', end_time='2016-12-08 02:00')
    print(event.get_message(3))
