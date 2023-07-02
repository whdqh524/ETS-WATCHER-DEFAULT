from datetime import datetime
import requests
import simplejson


from database.mongo import Conn
from config.settings import SlackMSG, SERVICE


class MessageHandle(Conn):
    """
    slack에 message를 보내거나 log를 저장할때 호출, log저장은 주석처리됨.
    에러발생 시 호출
    """
    def __init__(self):
        Conn.__init__(self)
        self.s = requests.Session()

    def send_slack(self, event_type, title, msg):
        data = {'username': SERVICE, 'title': title, 'text': msg,
                'attachments': [{'text': simplejson.dumps({'error_type': event_type})}],
                'channel': SlackMSG.CHANNEL, 'icon_emoji': 'slack'}
        headers = {'authorization': 'Bearer {}'.format(SlackMSG.TOKEN), 'Content-type': 'application/json'}
        try:
            res = self.s.post(SlackMSG.URI, data=simplejson.dumps(data), headers=headers)
            # self.mongo_ins_error({'event_type': event_type, 'title': title, 'msg': msg})
            return res
        except Exception as e:
            print('send_slack_error: {}'.format(str(e)))
            return False



