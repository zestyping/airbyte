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

    def path(self, stream_state, stream_slice, next_page_token):
        if next_page_token:
            return '/email/messages?' +  next_page_token.split('?', 1)[1]
        return '/email/messages'

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
                    'type': ['null', 'object'],
                    'properties': {
                        'name': STRING_TYPE,
                        'senderDisplayName': STRING_TYPE,
                        'senderEmailAddress': STRING_TYPE,
                        'createdBy': STRING_TYPE,
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


class IncrementalNgpvanStream(NgpvanStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class IncrementalEmailMessages(IncrementalNgpvanStream):
    primary_key = 'foreignMessageId'
    cursor_field = 'dateModified'

    def path(self, stream_state, stream_slice, next_page_token):
        return "/email/messages"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


def get_authenticator(config):
    username = 'apiuser'
    password = f'{config["van_api_key"]}|{config["database_mode"]}'
    return BasicAuthenticator(username, password)


# Source
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
