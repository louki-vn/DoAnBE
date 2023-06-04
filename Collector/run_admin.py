from configparser import ConfigParser
from time import sleep
import win32evtlog
import sys
from xml.etree import ElementTree as ET
import json
import xmltodict
import msvcrt
import pyuac
from json import dumps
from kafka import KafkaProducer
import os
import win32con


# event_context = {"info": "this object is always passed to your callback"}

def read_config(file_name):
    data = {}
    config = ConfigParser()
    config.read(filenames=file_name)  # reading config from file
    data['topic_name'] = config.get('kafka', 'topic_name')
    data['bootstrap_servers'] = config.get('kafka', 'bootstrap_servers')
    data['group_id'] = config.get('kafka', 'group_id')
    data['auto_offset_reset'] = config.get('kafka', 'auto_offset_reset')
    data['enable_auto_commit'] = config.get('kafka', 'enable_auto_commit')

    return data


def xml_to_json(xml_string):
    xml_dict = xmltodict.parse(xml_string)
    json_data = json.dumps(xml_dict)
    return json_data


evt_dict = {win32con.EVENTLOG_AUDIT_FAILURE: 'EVENTLOG_AUDIT_FAILURE',
            win32con.EVENTLOG_AUDIT_SUCCESS: 'EVENTLOG_AUDIT_SUCCESS',
            win32con.EVENTLOG_INFORMATION_TYPE: 'EVENTLOG_INFORMATION_TYPE',
            win32con.EVENTLOG_WARNING_TYPE: 'EVENTLOG_WARNING_TYPE',
            win32con.EVENTLOG_ERROR_TYPE: 'EVENTLOG_ERROR_TYPE'}


def parse_XML_log(event):
    tree = ET.ElementTree(ET.fromstring(event))
    root = tree.getroot()
    ns = "{http://schemas.microsoft.com/win/2004/08/events/event}"
    data = {}
    for eventID in root.findall(".//"):
        if eventID.tag == f"{ns}System":
            for e_id in eventID.iter():
                if e_id.tag == f"{ns}System":
                    pass
                elif e_id.tag == f"{ns}Provider":
                    data["Provider"] = e_id.attrib.get('Name')
                elif e_id.tag == f"{ns}TimeCreated":
                    data["TimeCreated"] = e_id.attrib.get('SystemTime')
                elif e_id.tag == f"{ns}Correlation":
                    data["ActivityID"] = e_id.attrib.get('ActivityID')
                elif e_id.tag == f"{ns}Execution":
                    data["ProcessID"] = e_id.attrib.get('ProcessID')
                    data["ThreadID"] = e_id.attrib.get('ThreadID')
                elif e_id.tag == f"{ns}EventType":
                    if not e_id.text in evt_dict.keys():
                        data['EventType'] = "unknown"
                    else:
                        data['EventType'] = str(evt_dict[e_id.text])
                else:
                    att = e_id.tag.replace(f"{ns}", "")
                    data[att] = e_id.text

        if eventID.tag == f"{ns}EventData":
            for attr in eventID.iter():
                if attr.tag == f'{ns}Data':
                    if attr.get('Name') is None:
                        data["Data"] = attr.text
                    else:
                        data[attr.get('Name')] = attr.text
                elif attr.tag == f'{ns}Binary':
                    data["Binary"] = attr.text

    return data


def new_logs_event_handler(reason, context, evt):
    """
    Called when new events are logged.

    reason - reason the event was logged?
    context - context the event handler was registered with
    evt - event handle
    """
    # Just print some information about the event
    # print ('reason', reason, 'context', context, 'event handle', evt)

    event = win32evtlog.EvtRender(evt, win32evtlog.EvtRenderEventXml)
    result = " ".join(l.strip() for l in event.splitlines())
    log = parse_XML_log(event=result)
    with open('xml_logs_test.txt', 'a') as file:
        file.write(log)
        file.write('\n')
    # my_producer.send('users', value=result)
    print(' New log record! ')

    # Make sure all printed text is actually printed to the console now
    sys.stdout.flush()
    return 0


def main():
    kafka_config = read_config('../kafka_config.ini')
    producer = KafkaProducer(bootstrap_servers=kafka_config['bootstrap_servers'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    print('Hello - Welcome to my Windows Logs Collector!!!')
    subscription1 = win32evtlog.EvtSubscribe('application', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    subscription2 = win32evtlog.EvtSubscribe('system', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    subscription3 = win32evtlog.EvtSubscribe('Security', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    while True:
        sleep(10)
        os.rename('xml_logs_test.txt', 'xml_logs_done.txt')
        with open('xml_logs_done.txt', 'r') as f:
            data = f.readlines()
            producer.send(topic=kafka_config['topic_name'], value=data)

        if msvcrt.kbhit() and msvcrt.getch() == chr(27).encode():
            break


if __name__ == "__main__":
    if not pyuac.isUserAdmin():
        print("Re-launching as admin!")

        pyuac.runAsAdmin()
        input("Press enter to close the window. >")
    else:
        main()  # Already an admin here.
