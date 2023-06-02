import win32evtlog
import sys
from xml.etree import ElementTree as ET
import json
import xmltodict
import sys
import msvcrt


# event_context can be `None` if not required, this is just to demonstrate how it works
event_context = {"info": "this object is always passed to your callback"}


def xml_to_json(xml_string):
    xml_dict = xmltodict.parse(xml_string)
    json_data = json.dumps(xml_dict)
    return json_data


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

    with open('xml_logs_test.txt', 'a') as file:
        file.write(result)
        file.write('\n')

    print(' - ')
    sys.stdout.flush()
    return 0


# Subscribe to future events
print('Hello - Welcome to my Windows Logs Collector!!!')
subscription1 = win32evtlog.EvtSubscribe('application', win32evtlog.EvtSubscribeToFutureEvents,
                                         None, Callback=new_logs_event_handler, Context=event_context, Query=None)
subscription2 = win32evtlog.EvtSubscribe('system', win32evtlog.EvtSubscribeToFutureEvents,
                                         None, Callback=new_logs_event_handler, Context=event_context, Query=None)
subscription3 = win32evtlog.EvtSubscribe('Security', win32evtlog.EvtSubscribeToFutureEvents,
                                         None, Callback=new_logs_event_handler, Context=event_context, Query=None)
while True:
    if msvcrt.kbhit() and msvcrt.getch() == chr(27).encode():
        break
