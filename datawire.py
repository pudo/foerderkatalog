from datastringer import DataStringer
from common import engine
import urllib

URL_BASE = "http://foerderportal.bund.de/foekat/jsp/SucheAction.do?actionMode=view&fkz=%s"


def submit_all():
    stringer = DataStringer(host='http://localhost:5000', service='foerderkatalog', event='project')
    table = engine['foerderungen']
    for row in list(table):
        if 'datawire_submitted' in row:
            del row['datawire_submitted']
        row['source_url'] = URL_BASE % urllib.quote_plus(row['fkz'])
        stringer.submit(row)
        table.update({'datawire_submitted': True, 'fkz': row['fkz']}, ['fkz'])


if __name__ == '__main__':
    submit_all()
