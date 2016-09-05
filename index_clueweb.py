# built-in modules
import re
import os
import gzip
import chardet

# installed modules
# None

# project modules
from utils import elastic
from utils.meta import timer
from utils.multiprocessing import pool_map


CLUEWEB_PATH = '/home/ls988/clueweb12-b13/clueweb12-b13/'
DEBUG = True


class WarcHeader(dict):
    def __init__(self):
        dict.__init__(self)
        self.__dict__ = self


class WarcRecord(dict):
    def __init__(self, raw_record):
        dict.__init__(self)
        self.__dict__ = self

        encoding_match = re.search(rb'charset=([a-zA-Z0-9\-]+)', raw_record)

        encoding = (
            encoding_match.group(1).decode('ascii')
            if encoding_match else 'utf-8'
        )

        try:
            raw_record = raw_record.decode(encoding)
        except UnicodeDecodeError:
            encoding = chardet.detect(raw_record)['encoding']
            raw_record = raw_record.decode(encoding)

        warc_hearder, html_header, content = raw_record.split('\n\n', 2)

        self._parse_header(warc_hearder)
        self._parse_header(html_header)

        self.content = content.strip()

    def _parse_header(self, raw_header):
        header_name = '_meta'

        for i, ln in enumerate(raw_header.strip().split('\n')):
            try:
                key, value = ln.split(': ', 1)
            except ValueError:
                header_name, metadata = ln.split('/')
                header_name = re.sub('\W+', '_', header_name)

                if re.match(r'\d\.\d \d+ \w+', header_name):
                    # HTTP header, first line has response code
                    _, resp_code, _ = metadata.split()
                    print(header_name)
                    self.setdefault(header_name, WarcHeader())['resp_code'] =\
                        resp_code

                continue

            key = re.sub('\W+', '_', key)
            self.setdefault(header_name, WarcHeader())[key] = value


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
        self.info = WarcRecord(content.pop(0))

        super(WarcFile, self).__init__(
            [WarcRecord(raw_record) for raw_record in content]
        )


def files_iterator(basepath):
    for dirname in os.listdir(basepath):
        for fn_gz in os.listdir(os.path.join(basepath, dirname)):
            fp_gz = os.path.join(basepath, dirname, fn_gz)
            with gzip.open(fp_gz) as f:
                content = f.read()
                warc = WarcFile(content)

            import ipdb; ipdb.set_trace()


def main(clueweb_fp=CLUEWEB_PATH):
    paths = [
        os.path.join(clueweb_fp, p) for p in os.listdir(clueweb_fp)
        if os.path.isdir(os.path.join(clueweb_fp, p)) and
        'ClueWeb12_' in p
    ]

    pool_map(files_iterator, [paths], single_thread=DEBUG)


if __name__ == '__main__':
    main()
