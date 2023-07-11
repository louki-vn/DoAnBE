from configparser import ConfigParser
import win32evtlog
import sys
from xml.etree import ElementTree as ET
import json
import xmltodict
import msvcrt
import pyuac
from json import dumps
from kafka import KafkaProducer
import socket
import os
from pathlib import Path

path = Path(__file__)
ROOT_DIR = path.parent.absolute()
config_path = os.path.join(ROOT_DIR, "..\kafka_config.ini")


IP = socket.gethostbyname(socket.gethostname())

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

evt_id_dict = {1000: 'MALWAREPROTECTION_SCAN_STARTED',
               1001: 'MALWAREPROTECTION_SCAN_COMPLETED',
               1002: 'MALWAREPROTECTION_SCAN_CANCELLED',
               1003: 'MALWAREPROTECTION_SCAN_PAUSED',
               1004: 'MALWAREPROTECTION_SCAN_RESUMED',
               1005: 'MALWAREPROTECTION_SCAN_FAILED',
               1006: 'MALWAREPROTECTION_MALWARE_DETECTED',
               1007: 'MALWAREPROTECTION_MALWARE_ACTION_TAKEN',
               1008: 'MALWAREPROTECTION_MALWARE_ACTION_FAILED',
               1009: 'MALWAREPROTECTION_QUARANTINE_RESTORE',
               1010: 'MALWAREPROTECTION_QUARANTINE_RESTORE_FAILED',
               1011: 'MALWAREPROTECTION_QUARANTINE_DELETE',
               1012: 'MALWAREPROTECTION_QUARANTINE_DELETE_FAILED',
               1013: 'MALWAREPROTECTION_MALWARE_HISTORY_DELETE',
               1014: 'MALWAREPROTECTION_MALWARE_HISTORY_DELETE_FAILED',
               1015: 'MALWAREPROTECTION_BEHAVIOR_DETECTED',
               1116: 'MALWAREPROTECTION_STATE_MALWARE_DETECTED',
               1117: 'MALWAREPROTECTION_STATE_MALWARE_ACTION_TAKEN',
               1118: 'MALWAREPROTECTION_STATE_MALWARE_ACTION_FAILED',
               1119: 'MALWAREPROTECTION_STATE_MALWARE_ACTION_CRITICALLY_FAILED',
               1120: 'MALWAREPROTECTION_THREAT_HASH',
               1121: 'MALWAREPROTECTION_FOLDER_GUARD_SECTOR_BLOCK',
               1150: 'MALWAREPROTECTION_SERVICE_HEALTHY',
               1151: 'MALWAREPROTECTION_SERVICE_HEALTH_REPORT',
               2000: 'MALWAREPROTECTION_SIGNATURE_UPDATED',
               2001: 'MALWAREPROTECTION_SIGNATURE_UPDATE_FAILED',
               2002: 'MALWAREPROTECTION_ENGINE_UPDATED',
               2003: 'MALWAREPROTECTION_ENGINE_UPDATE_FAILED',
               2004: 'MALWAREPROTECTION_SIGNATURE_REVERSION',
               2005: 'MALWAREPROTECTION_ENGINE_UPDATE_PLATFORMOUTOFDATE',
               2006: 'MALWAREPROTECTION_PLATFORM_UPDATE_FAILED',
               2007: 'MALWAREPROTECTION_PLATFORM_ALMOSTOUTOFDATE',
               2010: 'MALWAREPROTECTION_SIGNATURE_FASTPATH_UPDATED',
               2011: 'MALWAREPROTECTION_SIGNATURE_FASTPATH_DELETED',
               2012: 'MALWAREPROTECTION_SIGNATURE_FASTPATH_UPDATE_FAILED',
               2013: 'MALWAREPROTECTION_SIGNATURE_FASTPATH_DELETED_ALL',
               2020: 'MALWAREPROTECTION_CLOUD_CLEAN_RESTORE_FILE_DOWNLOADED',
               2021: 'MALWAREPROTECTION_CLOUD_CLEAN_RESTORE_FILE_DOWNLOAD_FAILED',
               2030: 'MALWAREPROTECTION_OFFLINE_SCAN_INSTALLED',
               2031: 'MALWAREPROTECTION_OFFLINE_SCAN_INSTALL_FAILED',
               2040: 'MALWAREPROTECTION_OS_EXPIRING',
               2041: 'MALWAREPROTECTION_OS_EOL',
               2042: 'MALWAREPROTECTION_PROTECTION_EOL',
               3002: 'MALWAREPROTECTION_RTP_FEATURE_FAILURE',
               3007: 'MALWAREPROTECTION_RTP_FEATURE_RECOVERED',
               5000: 'MALWAREPROTECTION_RTP_ENABLED',
               5001: 'MALWAREPROTECTION_RTP_DISABLED',
               5004: 'MALWAREPROTECTION_RTP_FEATURE_CONFIGURED',
               5007: 'MALWAREPROTECTION_CONFIG_CHANGED',
               5008: 'MALWAREPROTECTION_ENGINE_FAILURE',
               5009: 'MALWAREPROTECTION_ANTISPYWARE_ENABLED',
               5010: 'MALWAREPROTECTION_ANTISPYWARE_DISABLED',
               5011: 'MALWAREPROTECTION_ANTIVIRUS_ENABLED',
               5012: 'MALWAREPROTECTION_ANTIVIRUS_DISABLED',
               5013: 'MALWAREPROTECTION_SCAN_CANCELLED',
               5100: 'MALWAREPROTECTION_EXPIRATION_WARNING_STATE',
               5101: 'MALWAREPROTECTION_DISABLED_EXPIRED_STATE', }


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


kafka_config = read_config(config_path)
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
    log['IP'] = IP
    try:
        my_producer.send('windows', value=log)
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
    # subscription1 = win32evtlog.EvtSubscribe('application', win32evtlog.EvtSubscribeToFutureEvents,
    #                                          None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    # subscription2 = win32evtlog.EvtSubscribe('system', win32evtlog.EvtSubscribeToFutureEvents,
    #                                          None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    subscription3 = win32evtlog.EvtSubscribe('Security', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    subscription4 = win32evtlog.EvtSubscribe('Microsoft-Windows-Windows Defender/Operational', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    subscription5 = win32evtlog.EvtSubscribe('Microsoft-Windows-Windows Firewall With Advanced Security/Firewall', win32evtlog.EvtSubscribeToFutureEvents,
                                             None, Callback=new_logs_event_handler, Context=event_context, Query=None)
    while True:
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
