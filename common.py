import os
import dataset
import logging

database_url = os.environ.get('DATABASE_URL')
engine = dataset.connect(database_url)
table = engine.get_table('projekte')

logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)
