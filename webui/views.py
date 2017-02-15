import json
import datetime
from django.shortcuts import HttpResponse
from webui.service import EventData, LogData, AppData, ZabbixData, ReleaseData, AlarmData, MessageData


def data(req):
    site = req.GET.get("site")
    begin_time = req.GET.get("begin_time")
    end_time = req.GET.get("end_time")
    interval = req.GET.get("interval")
    type = req.GET.get("type")
    if type == 'event_data':
        event = EventData(site, begin_time, end_time, interval)
        event_data = event.get_data()
        data = {'event_data': event_data}
    elif type == 'log_data':
        log = LogData(site, begin_time, end_time, interval)
        log_data = log.get_data()
        data = {'log_data': log_data}
    elif type == 'app_data':
        app = AppData(site, begin_time, end_time, interval)
        app_data = app.get_data()
        data = {'app_data': app_data}
    elif type == 'zabbix_data':
        zabbix = ZabbixData(site, begin_time, end_time, interval)
        zabbix_data = zabbix.get_data()
        data = {'zabbix_data': zabbix_data}
    elif type == 'release_data':
        release = ReleaseData(site, begin_time, end_time)
        release_data = release.get_data()
        data = {'release_data': release_data}
    elif type == 'alarm_data':
        alarm = AlarmData(site, begin_time, end_time)
        alarm_data = alarm.get_data()
        data = {'alarm_data': alarm_data}
    elif type == 'all':
        event = EventData(site, begin_time, end_time, interval)
        event_data = event.get_data()

        log = LogData(site, begin_time, end_time, interval)
        log_data = log.get_data()

        app = AppData(site, begin_time, end_time, interval)
        app_data = app.get_data()

        zabbix = ZabbixData(site, begin_time, end_time, interval)
        zabbix_data = zabbix.get_data()

        release = ReleaseData(site, begin_time, end_time)
        release_data = release.get_data()

        alarm = AlarmData(site, begin_time, end_time)
        alarm_data = alarm.get_data()

        data = {'event_data': event_data, 'log_data': log_data, 'app_data': app_data, 'zabbix_data': zabbix_data,
                'release_data': release_data, 'alarm_data': alarm_data}

    return HttpResponse(json.dumps(data))

def message(req):
    site = req.GET.get("site")
    begin_time = req.GET.get("begin_time")
    interval = req.GET.get("interval")
    if interval == '24h':
        begin_time = datetime.datetime.strptime(begin_time, "%Y-%m-%d")
        end_time = begin_time + datetime.timedelta(days=1)
        begin_time = datetime.datetime.strftime(begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
        end_time = datetime.datetime.strftime(end_time, "%Y-%m-%dT%H:%M:%S.00Z")
        message = MessageData(site, begin_time, end_time)
        message_data = message.get_data()
        data = {'message_data': message_data}
    elif interval == '1h':
        begin_time = datetime.datetime.strptime(begin_time, "%Y-%m-%d %H")
        begin_time = begin_time - datetime.timedelta(hours=8)
        end_time = begin_time + datetime.timedelta(hours=1)
        begin_time = datetime.datetime.strftime(begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
        end_time = datetime.datetime.strftime(end_time, "%Y-%m-%dT%H:%M:%S.00Z")
        message = MessageData(site, begin_time, end_time)
        message_data = message.get_data()
        data = {'message_data': message_data}
    elif interval == '5m':
        begin_time = datetime.datetime.strptime(begin_time, "%Y-%m-%d %H:%M")
        begin_time = begin_time - datetime.timedelta(hours=8)
        end_time = begin_time + datetime.timedelta(minutes=5)
        begin_time = datetime.datetime.strftime(begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
        end_time = datetime.datetime.strftime(end_time, "%Y-%m-%dT%H:%M:%S.00Z")
        message = MessageData(site, begin_time, end_time)
        message_data = message.get_data()
        data = {'message_data': message_data}
    elif interval == '1m':
        begin_time = datetime.datetime.strptime(begin_time, "%Y-%m-%d %H:%M")
        begin_time = begin_time - datetime.timedelta(hours=8)
        end_time = begin_time + datetime.timedelta(minutes=1)
        begin_time = datetime.datetime.strftime(begin_time, "%Y-%m-%dT%H:%M:%S.00Z")
        end_time = datetime.datetime.strftime(end_time, "%Y-%m-%dT%H:%M:%S.00Z")
        message = MessageData(site, begin_time, end_time)
        message_data = message.get_data()
        data = {'message_data': message_data}
    return HttpResponse(json.dumps(data))

def alarm_message(req):
    site = req.GET.get("site")
    begin_time = req.GET.get("begin_time")
    level = req.GET.get("level")
    begin_time += ':00'
    delta = datetime.timedelta(hours=1)
    end_time = datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M') + delta
    end_time = end_time.strftime('%Y-%m-%d %H:%M')
    message = AlarmData(site, begin_time, end_time)
    message_data = message.get_message(level)
    data = {'message_data': message_data}
    return HttpResponse(json.dumps(data))