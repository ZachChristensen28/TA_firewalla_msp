
# encoding = utf-8

import os
import sys
import time
import datetime
import json
import firewalla_constants
from zts_helper import *


def validate_input(helper, definition):
    pass

def collect_events(helper, ew):
    log_level = helper.get_log_level()
    helper.set_log_level(log_level)
    helper.log_info(f'log_level="{log_level}"')

    firewalla_account = helper.get_arg('firewalla_account')
    msp_domain = firewalla_account['username']
    msp_token = firewalla_account['password']
    cloud_env = helper.get_arg('cloud_environment')
    stanza = str(helper.get_input_stanza_names())

    proxy = helper.get_proxy()
    event_type = 'proxy_config'
    if proxy:
        if proxy["proxy_username"]:
            event_log = zts_logger(
                msg='Proxy is configured with authentication',
                action='success',
                event_type=event_type,
                stanza=stanza
            )
            helper.log_info(event_log)
            proxy_string = f'{proxy["proxy_type"]}://{proxy["proxy_username"]}:{proxy["proxy_password"]}@{proxy["proxy_url"]}:{proxy["proxy_port"]}'
        else:
            event_log = zts_logger(
                msg='Proxy is configured with no authentication',
                action='success',
                event_type=event_type,
                stanza=stanza
            )
            helper.log_info(event_log)
            proxy_string = f'{proxy["proxy_type"]}://{proxy["proxy_url"]}:{proxy["proxy_port"]}'

        proxy_config = {'http': proxy_string, 'https': proxy_string}
    else:
        event_log = zts_logger(
            msg='Proxy is not configured',
            action='success',
            event_type=event_type,
            stanza=stanza
        )
        helper.log_info(event_log)
        proxy_config = None

    headers = {
        "Authorization": f"Token {msp_token}",
        'Accept': 'application/json',
        'Content-type': 'application/json'
    }

    event_type = "data collection"
    event_log = zts_logger(
        msg='Starting data collection',
        action='starting',
        event_type=event_type,
        stanza=stanza
    )
    helper.log_info(event_log)

    clean_domain = msp_domain.lstrip("https?://").rstrip("/")
    url = f"https://{clean_domain}/{firewalla_constants.FIREWALLA_DEVICES_ENDPOINT}"

    r = helper.send_http_request(url, "GET", headers=headers, verify=True, use_proxy=True)

    if r.status_code != 200:
        event_log = zts_logger(
            msg='Error retrieving data',
            action='failed',
            event_type=event_type,
            stanza=stanza
        )
        helper.log_error(event_log)
        raise SystemExit(r.status_code)

    event_log = zts_logger(
        msg='Data collection retrieved',
        action='success',
        event_type=event_type,
        stanza=stanza
    )
    helper.log_info(event_log)

    device_count = 0
    for device in r.json():
        device_count += 1
        splunk_event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(device), host="ta_firewalla_msp")
        ew.write_event(splunk_event)

    event_log = zts_logger(
        msg=f'Collection complete',
        action='success',
        event_type=event_type,
        stanza=stanza,
        device_count=device_count
    )
    helper.log_info(event_log)

    raise SystemExit()
