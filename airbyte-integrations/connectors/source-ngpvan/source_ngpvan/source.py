from abc import ABC
import base64
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator

ROOT = 'https://api.securevan.com/v4'


class BasicAuthenticator(HttpAuthenticator, HTTPBasicAuth):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def get_tuple(self):
        return (self.username, self.password)

    def get_auth_header(self):
        creds = f'{self.username}:{self.password}'
        encoded = base64.b64encode(creds.encode('utf-8')).decode('ascii')
        return {'Authorization': f'Basic {encoded}'}


class NgpvanStream(HttpStream, ABC):
    url_base = ROOT

    def next_page_token(self, response):
        return response.json()['nextPageLink']

    def parse_response(self, response, **kwargs) -> Iterable[Mapping]:
        return response.json()['items']


class EmailMessages(NgpvanStream):
    primary_key = 'foreignMessageId'
    cursor_field = 'dateModified'
    state_checkpoint_interval = 100

    def __init__(self, authenticator, min_date: str = None):
        super().__init__(authenticator=authenticator)
        self.min_date = min_date

    def get_updated_state(self, stream_state, latest_record):
        return max(stream_state, latest_record['dateModified'])

    def path(self, stream_state, stream_slice, next_page_token):
        if next_page_token:
            return '/email/messages?' +  next_page_token.split('?', 1)[1]
        return '/email/messages'

    def request_params(self, stream_state, stream_slice, next_page_token):
        return {'$top': '10', '$orderby': 'dateModified asc'}

    def parse_response(self, response, **kwargs) -> Iterable[Mapping]:
        for item in response.json()['items']:
            item_request = self._create_prepared_request(
                path='/email/message/' + item['foreignMessageId'],
                headers=self.authenticator.get_auth_header(),
                params={'$expand': 'emailMessageContent,emailMessageContentDistributions'}
            )
            item_kwargs = self.request_kwargs(**kwargs)
            item_response = self._send_request(item_request, item_kwargs)
            yield item_response.json()

    def get_json_schema(self):
        STRING_TYPE = {'type': 'string'}
        INTEGER_TYPE = {'type': 'integer'}
        NUMBER_TYPE = {'type': 'number'}
        DATETIME_TYPE = {'type': 'string', 'format': 'date-time'}
        return {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'foreignMessageId': STRING_TYPE,
                'name': STRING_TYPE,
                'createdBy': STRING_TYPE,
                'dateCreated': DATETIME_TYPE,
                'dateScheduled': DATETIME_TYPE,
                'dateModified': DATETIME_TYPE,
                'campaignID': INTEGER_TYPE,
                'emailMessageContent': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'name': STRING_TYPE,
                            'senderDisplayName': STRING_TYPE,
                            'senderEmailAddress': STRING_TYPE,
                            'createdBy': STRING_TYPE,
                            'subject': STRING_TYPE,
                            'dateCreated': DATETIME_TYPE,
                            'emailMessageContentDistributions': {
                                'type': ['null', 'object'],
                                'properties': {
                                    'dateSent': DATETIME_TYPE,
                                    'recipientCount': INTEGER_TYPE,
                                    'openCount': INTEGER_TYPE,
                                    'linksClickedCount': INTEGER_TYPE,
                                    'unsubscribeCount': INTEGER_TYPE,
                                    'bounceCount': INTEGER_TYPE,
                                    'contributionTotal': NUMBER_TYPE,
                                    'formSubmissionCount': INTEGER_TYPE,
                                    'contributionCount': INTEGER_TYPE
                                }
                            }
                        }
                    }
                }
            }
        }


def get_authenticator(config):
    username = 'apiuser'
    password = f'{config["van_api_key"]}|{config["database_mode"]}'
    return BasicAuthenticator(username, password)


class SourceNgpvan(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = get_authenticator(config)
            response = requests.get(f'{ROOT}/apiKeyProfiles', auth=auth)
            result = response.json()
            if response.status_code != 200:
                return False, result['errors'][0]['text']
            try:
                assert result['items'][0]['username']
            except:
                return False, 'API key not recognized'
        except Exception as e:
            return False, f'Error: {e}'
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = get_authenticator(config)
        return [EmailMessages(authenticator=auth)]
