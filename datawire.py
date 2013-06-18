from datastringer import DataStringer
from common import table, date


def submit_all():
    stringer = DataStringer(service='foerderkatalog', event='project')
    for row in list(table.find()):
        if 'datawire_submitted' in row:
            if row['datawire_submitted']:
                continue
            del row['datawire_submitted']
        stringer.submit(row, source_url=row.get('url'), action_at=date(row.get('laufzeit_von')))
        table.update({'datawire_submitted': True, 'fkz': row['fkz']}, ['fkz'])


if __name__ == '__main__':
    submit_all()
