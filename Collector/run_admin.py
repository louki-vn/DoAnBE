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
import dateutil.parser
import os


event_context = {"info": "this object is always passed to your callback"}


def read_config(file_name):
    """Read kafka file config 

    Args:
        file_name (string): path to file config

    Returns:
        _type_: _description_
    """
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
    """Convert XML tring to JSON format

    Args:
        xml_string (string): XML string

    Returns:
        _type_: _description_
    """
    xml_dict = xmltodict.parse(xml_string)
    json_data = json.dumps(xml_dict)
    return json_data


evt_level_dict = {0: 'LogAlways',
                  1: 'Critical',
                  2: 'Error',
                  3: 'Warning',
                  4: 'Informational',
                  5: 'Verbose'}

evt_opcode_dict = {0: 'Info',
                   1: 'Start',
                   2: 'Stop',
                   3: 'DataCollectionStart',
                   4: 'DataCollectionStop',
                   5: 'Extension',
                   6: 'Reply',
                   7: 'Resume',
                   8: 'Suspend',
                   9: 'Send',
                   240: 'Receive'}


def parse_XML_log(event):
    """"Parse a Windows event log entry in XML format into a dictionary of properties. 

    Args:
        event (string): The XML string representing the Windows event log entry.

    Returns:
        dict: A dictionary of properties extracted from the XML string.
    """
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
                elif e_id.tag == f"{ns}Level":
                    if not int(e_id.text) in evt_level_dict.keys():
                        data['Level'] = "unknown"
                    else:
                        data['Level'] = evt_level_dict[int(e_id.text)]
                elif e_id.tag == f"{ns}Opcode":
                    if not int(e_id.text) in evt_opcode_dict.keys():
                        data['Opcode'] = "unknown"
                    else:
                        data['Opcode'] = evt_opcode_dict[int(e_id.text)]
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


kafka_config = read_config('../kafka_config.ini')
my_producer = KafkaProducer(bootstrap_servers=kafka_config['bootstrap_servers'],
                            value_serializer=lambda x: dumps(x).encode('utf-8'))


def new_logs_event_handler(reason, context, evt):
    """
    Called when new events are logged. Send processed data to Kafka.

    reason - reason the event was logged?
    context - context the event handler was registered with
    evt - event handle
    """
    # Just print some information about the event
    # print ('reason', reason, 'context', context, 'event handle', evt)

    event = win32evtlog.EvtRender(evt, win32evtlog.EvtRenderEventXml)
    result = " ".join(l.strip() for l in event.splitlines())
    log = parse_XML_log(event=result)
    try:
        my_producer.send('users', value=log)
        print(' New log record! ')
    except:
        print('Can not sent to server!')

    # Make sure all printed text is actually printed to the console now
    sys.stdout.flush()
    return 0


def main():
    """
        Generate Windows event logs subscriber for channels
    """
    print('Hello - Welcome to my Windows Logs Collector!!!')
    subscription1 = win32evtlog.EvtSubscribe('application', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    subscription2 = win32evtlog.EvtSubscribe('system', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    subscription3 = win32evtlog.EvtSubscribe('Security', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    while True:
        try:
            sleep(1)
        except:
            if msvcrt.kbhit() and msvcrt.getch() == chr(27).encode():
                break


if __name__ == "__main__":
    """
        If the program does not run with admin privileges, rerun and prompt for admin privileges.
    """
    if not pyuac.isUserAdmin():
        print("Re-launching as admin!")
        pyuac.runAsAdmin()
        input("Press enter to close the window. >")
    else:
        main()  # Already an admin here.
