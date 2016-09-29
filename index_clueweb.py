# built-in modules
import re
import os
import gzip
import time
import random
import chardet
import itertools
import html.parser

# installed modules
from bs4 import BeautifulSoup
# from boilerpipe.extract import Extractor

# project modules
from utils import elastic
from utils.multiprocessing import pool_map

DOMAIN_RE = r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/\n]+)'
URL_RE = (
    rb'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|'
    rb'(?:%[0-9a-fA-F][0-9a-fA-F]))+'
)
CLUEWEB_PATH = '/home/ls988/clueweb12-b13/clueweb12-b13/'
DEBUG = False

ES_HOST = 'devram4.cs.georgetown.edu'
ES_PORT = 9200
INDEX_NAME = 'clueweb12b'
PROGRESS_FILE = 'progress.txt'
SKIPPED_FILE = 'skipped.txt'


class ClueWebIndexingError(RuntimeError):
    def __init__(self):
        super(ClueWebIndexingError, self).__init__()


def simplify_html(html_text):
    parser = html.parser.HTMLParser()

    # html_text = html_text.replace('\r\n', '\n')
    html_text = re.sub(r'<br\\?>', '\n', html_text)
    html_text = html_text.replace('><', '> <')

    # try:
    soup = BeautifulSoup(html_text, 'lxml')

    title = soup.find('title')
    title = parser.unescape(title.text) if title else ''

    for script in soup.find_all('script'):
        script.decompose()

    for style in soup.find_all('style'):
        style.decompose()

    for tag in soup.find_all(True):
        tag.attrs.clear()

    body_text = soup.text.replace('[ \t]{2:}', '\n\n')

    return title, body_text


class WarcHeader(dict):
    def __init__(self):
        dict.__init__(self)
        self.__dict__ = self


class WarcRecord(dict):
    def __init__(self, raw_record):
        dict.__init__(self)
        self.__dict__ = self

        warc_attr, html_attr, raw_content = \
            re.split(rb'\n\s+', raw_record, maxsplit=2)

        url_regex = rb'WARC-Target-URI: (' + URL_RE + rb')'
        self.url = re.search(url_regex, warc_attr).group(1).decode('utf-8')

        self.id = re.search(
            rb'WARC-TREC-ID: ([a-zA-Z0-9\-]+)', warc_attr
        ).group(1).decode('utf-8')

        # we try getting the encoding from the file itself
        encoding_match = re.search(rb'charset=([a-zA-Z0-9\-]+)', raw_content)
        encoding = (
            encoding_match.group(1).decode('ascii')
            if encoding_match else 'utf-8'
        )

        try:
            content = raw_content.decode(encoding)
            has_decoded = True
        except (UnicodeDecodeError, LookupError):
            has_decoded = False

        # the encding specified in the document is not correct; so
        # we use chardet to find it; if that still does not help,
        # we default to unicode and ignore all errors.

        if not has_decoded:
            encoding = chardet.detect(raw_content)['encoding']

            if encoding is None:
                encoding = 'utf-8'

            try:
                content = raw_content.decode(encoding, errors='ignore')
            except LookupError:
                content = raw_content.decode('utf-8', errors='ignore')

        self.content = content.strip()


class WarcFile(list):
    def __init__(self, raw_content, version='1.0'):
        warc_split = 'WARC/{}'.format(version).encode('ascii')

        content = [
            (
                warc_split +
                raw_record.replace(b'\r\n', b'\n')
            )
            for raw_record in
            raw_content.split(warc_split)[1:]
        ]

        # skip the header of the warc file
        content.pop(0)

        super(WarcFile, self).__init__()

        for raw_record in content:
            self.append(WarcRecord(raw_record))


class Progress(object):
    def __init__(self, path):
        if not os.path.exists(path):
            raise RuntimeError('"{}" does not exists'.format(path))

        with open(path) as f:
            self._progress = set(f.read().strip().split())

        self.path = path

    def __len__(self):
        return len(self._progress)

    def __contains__(self, _id):
        return self._progress.__contains__(_id)

    def add(self, _id):
        self._progress.add(_id)
        return Progress.append(self.path, _id)

    @staticmethod
    def append(path, _id):
        with open(path, 'a') as f:
            f.write('{}\n'.format(_id))

    @staticmethod
    def make(path, overwrite=False):
        if not os.path.exists(path) or overwrite:
            with open(path, 'w') as f:
                f.write('')

    @staticmethod
    def write_skipped(li, path):
        with open(path, 'w') as f:
            f.write('\n'.join(li))


def warc_filepaths_iterator(basepath, ignore_progress=False):
    progress = Progress(PROGRESS_FILE)

    for dirname in os.listdir(basepath):
        for fn_gz in os.listdir(os.path.join(basepath, dirname)):
            fp_gz = os.path.join(basepath, dirname, fn_gz)

            if not(ignore_progress) and fp_gz in progress:
                continue

            yield fp_gz


# def get_boilerpipe_body(content):
#     extractor = Extractor(extractor='ArticleExtractor', html=content)
#     return extractor.getText()


def extract_from_warc(warc_path):
    start = time.time()

    with gzip.open(warc_path) as gzf:
        warc = WarcFile(gzf.read())

    delta = time.time() - start

    print('[info] warc "{}" extracted in {:.0f} s ({:,} pages)'
          ''.format(warc_path.rsplit('/', 1)[1], delta, len(warc)))

    cnt = 0

    for doc in warc:
        cnt += 1
        if doc.content.strip():
            title, body = simplify_html(doc.content)
        else:
            title = body = ''

        doc = {
            '_id': doc.id,
            'url': doc.url,
            'domain': re.match(DOMAIN_RE, doc.url).group(1),
            '_type': 'document',
            'title': title,
            'body': body
        }

        yield doc

    Progress.append(PROGRESS_FILE, warc_path)

    delta = time.time() - start
    per_doc = delta / cnt if cnt > 0 else 0
    print('[info] "{}": {:,} documents processed in {:.0f} s ({:.1e} s / doc)'
          ''.format(warc_path.rsplit('/', 1)[1], cnt, delta, per_doc))


def index_warc(warc_file):
    es_client = elastic.get_client(
        host=ES_HOST, port=ES_PORT, timeout=120, index_name=INDEX_NAME
    )
    extracted = extract_from_warc(warc_file)
    _, skipped = elastic.index_in_bulk(
        extracted, es_client=es_client, bulk_size_in_bytes=7500000
    )
    return skipped


def main(clueweb_fp=CLUEWEB_PATH):
    Progress.make(PROGRESS_FILE)

    base_paths = [
        os.path.join(clueweb_fp, p) for p in os.listdir(clueweb_fp)
        if os.path.isdir(os.path.join(clueweb_fp, p)) and
        'ClueWeb12_' in p
    ]
    paths = list(
        itertools.chain(*(warc_filepaths_iterator(p) for p in base_paths)))
    print('[info] {:,} files to index'.format(len(paths)))
    random.shuffle(paths)

    es_client = elastic.get_client(
        host=ES_HOST, port=ES_PORT, timeout=120, index_name=INDEX_NAME
    )
    elastic.create_index(
        index_name=INDEX_NAME,
        index_settings='clueweb.json',
        es_client=es_client,
        allow_if_not_deleted=True
    )

    skipped = pool_map(index_warc, [paths], single_thread=DEBUG, cpu_ratio=.96)
    skipped = list(itertools.chain(*skipped))
    Progress.write_skipped(skipped, SKIPPED_FILE)


if __name__ == '__main__':
    main()
