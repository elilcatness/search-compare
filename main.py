import os
from shutil import rmtree

from utils import (retrieve_json, write_headers, refactor_date, process_period,
                   refactor_url, write_row, compare_pct, compare_diff, process_periods, compare_pct_pos,
                   compare_diff_pos)
from constants import DYNAMIC_FIELDNAMES


def main(input_filename: str, output_folder: str = 'output', data_folder: str = 'data',
         creds_filename: str = 'creds.json'):
    for folder in output_folder, data_folder:
        if os.path.exists(folder):
            if input(f'Папка {folder} будет перезаписана. Вы готовы продолжить? (y\\n) ').lower() != 'y':
                return
            rmtree(folder)
        os.mkdir(folder)
    extra_fields = []
    for name, verbose_name in ('country', 'странам'), ('device', 'устройствам'):
        if input(f'Делаем ли разбивку по {verbose_name}? (y\\n) ').lower() == 'y':
            extra_fields.append(name)
    with open(input_filename, encoding='utf-8') as f:
        urls = [line.strip() for line in f.readlines()]
        count = len(urls)
    creds = retrieve_json(creds_filename)
    periods = [[refactor_date(dt) for dt in period.strip().split()] for period in
               input(f'Введите диапазон(ы) дат для выгрузки '
                     f'(диапазоны разделять через #, даты – через пробел):\n').split('#')]
    if not periods:
        return print('Без диапазонов никуда :)')
    for i in range(count):
        if len(periods) == 1:
            multiple = False
            data, dates = process_period(urls[i], periods[0], data_folder, creds, extra_fields)
        else:
            multiple = True
            data, dates = process_periods(urls[i], periods, data_folder, creds, extra_fields)
        primary_keys = ['URL', 'Query'] + [field.capitalize() for field in extra_fields]
        fieldnames = primary_keys[:]
        dynamic = []
        for field in DYNAMIC_FIELDNAMES:
            fields, comparisons = [], []
            for m in range(len(dates)):
                fields.append('_'.join([field, dates[m]]))
                if m > 0:
                    comparisons.extend([f'{field}_{key}_{"_".join([dates[m], dates[m - 1]])}'
                                        for key in ('diff', 'pct')])
            fieldnames += fields + sorted(comparisons)
            dynamic += fields
        if not multiple:
            filename = os.path.join(output_folder,
                                    f'{refactor_url(urls[i])}_{"-".join([dates[0], dates[-1]])}.csv')
        else:
            filename = os.path.join(output_folder, f'{refactor_url(urls[i])}_{"#".join(dates)}.csv')
        write_headers(filename, fieldnames)
        for j0 in range(len(data)):  # Current date iterations
            for k0 in range(len(data[j0])):  # Current date rows iteration
                if not data[j0][k0]:
                    continue
                row = {**{key: data[j0][k0][key] for key in primary_keys},
                       **{key: 0 for key in dynamic}}
                matches = {_date: dict() for _date in dates}
                matches[dates[j0]] = data[j0][k0]
                for j1 in range(j0 + 1, len(data)):  # Next dates iterations
                    for k1 in range(len(data[j1])):  # Next dates rows iterations
                        if (data[j0][k0] and data[j1][k1]
                                and all(data[j0][k0][key] == data[j1][k1][key] for key in primary_keys)):
                            matches[dates[j1]] = data[j1][k1]
                            data[j1][k1] = None
                            break
                for m, item in enumerate(matches.items()):
                    _date, match = item
                    for field in DYNAMIC_FIELDNAMES:
                        row[f'{field}_{_date}'] = match.get(field, 0)
                        if m > 0:
                            prev_val = row[f'{field}_{dates[m - 1]}']
                            cur_val = row[f'{field}_{_date}']
                            if field == 'Position':
                                diff = compare_diff_pos(prev_val, cur_val)
                                pct = compare_pct_pos(prev_val, cur_val)
                            else:
                                diff = compare_diff(prev_val, cur_val)
                                pct = compare_pct(prev_val, cur_val)
                            row[f'{field}_diff_{_date}_{dates[m - 1]}'] = diff
                            row[f'{field}_pct_{_date}_{dates[m - 1]}'] = pct
                write_row(row, filename, fieldnames)
        print('\nOK')


if __name__ == '__main__':
    main('urls.txt')
