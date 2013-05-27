from datastringer import DataStringer
from common import table
import urllib

URL_BASE = "http://foerderportal.bund.de/foekat/jsp/SucheAction.do?actionMode=view&fkz=%s"


def submit_all():
    stringer = DataStringer(host='http://localhost:5000', service='foerderkatalog', event='project')
    for row in list(table.find(datawire_submitted=False)):
        if 'datawire_submitted' in row:
            del row['datawire_submitted']
        row['source_url'] = URL_BASE % urllib.quote_plus(row['fkz'])
        stringer.submit(row)
        table.update({'datawire_submitted': True, 'fkz': row['fkz']}, ['fkz'])


if __name__ == '__main__':
    submit_all()
