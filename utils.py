import os
# noinspection PyUnresolvedReferences
from apiclient.discovery import build
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from csv import DictWriter
from datetime import date, timedelta
import json

from constants import API_ROWS_LIMIT, FIELDNAMES, COUNTRIES


def get_domain(url: str) -> str:
    return '//'.join(url.split('/')[0:3:2])


def get_console(creds: dict):
    scope = 'https://www.googleapis.com/auth/webmasters.readonly'
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scopes=[scope])
    http = credentials.authorize(Http())
    return build('searchconsole', 'v1', http=http)


def refactor_date(dt: str):
    if len(dt) != 8:
        return
    return '-'.join([dt[:4], dt[4:6], dt[6:8]])


def refactor_url(url: str) -> str:
    return url.split('//')[-1].replace('.', '_').replace('/', '~')


def get_inner_dates(st_date: str, end_date: str) -> list:
    st_date, end_date = date.fromisoformat(st_date), date.fromisoformat(end_date)
    output, cur_date = [], st_date
    while cur_date <= end_date:
        output.append(cur_date.isoformat())
        cur_date += timedelta(days=1)
    return output


def retrieve_json(filename: str):
    with open(filename, encoding='utf-8') as f:
        return json.loads(f.read())


def write_headers(filename: str, fieldnames: list, delimiter: str = ';'):
    with open(filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = DictWriter(csv_file, fieldnames, delimiter=delimiter)
        writer.writeheader()


def write_row(row: dict, filename: str, fieldnames: list, delimiter: str = ';'):
    with open(filename, 'a', newline='', encoding='utf-8') as csv_file:
        writer = DictWriter(csv_file, fieldnames, delimiter=delimiter)
        writer.writerow(row)


def _execute_request(service, url: str, request: dict):
    return service.searchanalytics().query(siteUrl=url, body=request).execute()


def get_url_queries(url: str, st_date: str, end_date: str, creds: dict,
                    extra_fields: list = None):
    extra_fields = extra_fields if extra_fields and isinstance(extra_fields, list) else []
    domain = get_domain(url)
    service = get_console(creds)
    headers = ['page', 'query'] + extra_fields
    start_row = 0
    raw_output, current_rows = [], []
    while len(current_rows) == API_ROWS_LIMIT or start_row == 0:
        request = {'startDate': st_date,
                   'endDate': end_date,
                   'dimensions': headers,
                   'rowLimit': API_ROWS_LIMIT,
                   'startRow': start_row}
        current_rows = _execute_request(service, domain, request).get('rows', [])
        current_rows = [row for row in current_rows]
        raw_output.extend(current_rows)
        start_row += API_ROWS_LIMIT
    output = []
    for row in raw_output:
        keys = row.pop('keys')
        data = {headers[i]: keys[i] for i in range(len(keys))}
        output.append({**data, **row})
    return [row for row in output if row['page'].rstrip('/') == url.rstrip('/')]


def process_url(url: str, st_date: str, end_date: str, filename: str, creds: dict, extra_fields: list = None):
    extra_fields = extra_fields if extra_fields and isinstance(extra_fields, list) else []
    data = get_url_queries(url, st_date, end_date, creds, extra_fields=extra_fields)
    fieldnames = FIELDNAMES + [field.capitalize() for field in extra_fields]
    write_headers(filename, fieldnames)
    output = []
    for d in data:
        row = {'URL': url, 'Query': d['query'], 'Impressions': d['impressions'],
               'Clicks': d['clicks'], 'Position': float(d['position'])}
        for i, field in enumerate(extra_fields):
            try:
                val = d[field]
            except KeyError:
                val = d[None][i]
            if field == 'country':
                val = COUNTRIES.get(val, val)
            row[field.capitalize()] = val
        write_row(row, filename, fieldnames)
        output.append(row)
    return output


def process_period(url: str, period: list, folder: str, creds: dict, extra_fields: list = None):
    extra_fields = extra_fields if extra_fields and isinstance(extra_fields, list) else []
    dates = get_inner_dates(*period)
    return [process_url(url, _date, _date, os.path.join(folder,
                                                        f'{refactor_url(url)}_{_date.replace("-", "")}.csv'),
                        creds, extra_fields) for _date in dates], [_date.replace('-', '') for _date in dates]


def process_periods(url: str, periods: list, folder: str, creds: dict, extra_fields: list = None):
    extra_fields = extra_fields if extra_fields and isinstance(extra_fields, list) else []
    output = []
    for period in periods:
        st_date, end_date = period
        output.append(process_url(url, st_date, end_date, os.path.join(
            folder, f'{refactor_url(url)}_{period[0].replace("-", "")}-{period[1].replace("-", "")}'),
                                  creds, extra_fields))
    return output, ['-'.join([_date.replace("-", "") for _date in period]) for period in periods]


def compare_pct_pos(a: int, b: int):
    div = a
    if a == 0:
        a = 1000
        div = 1
    if b == 0:
        b = 1000
    val = (b - a) / div * 100
    return val * -1 if val != 0 else val


def compare_pct(a: int, b: int):
    return (b - a) / a * 100 if a != 0 else b * 100


def compare_diff_pos(a: int, b: int):
    if a == 0:
        a = 1000
    if b == 0:
        b = 1000
    val = b - a
    return val * -1 if val != 0 else val


def compare_diff(a: int, b: int):
    return b - a
