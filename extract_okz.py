import camelot
import PyPDF2
import glob
import tqdm
import csv

import time
from multiprocessing import Pool

import re
import typing
import itertools
import contextlib
import collections

PAT_WHITESPACES = re.compile(r'\s+')
PAT_PROFSTANDARDS_HEADER = re.compile('^.*код.*проф.*стандарт.*$', re.IGNORECASE)
PAT_PROFSTANDARD_CODE = re.compile(r'\b\d\d\.\d\d\d\b')
PAT_PROGRAM_PDF = re.compile(r'(\d\d\.\d\d\.\d\d)\.pdf')
PAT_PS_PDF = re.compile(r'(\d\d\.\d\d\d)\.pdf')
PAT_OKZ_LABEL = re.compile(r'\bОКЗ\b')
PAT_OKZ = re.compile(r'\b\d\d\d\d\b')

N_PROCESSES = 4
N_PDF_TRIES = 4

# no time to fix camelot, just set set ulimit -n LARGE_NUMBER


def get_files():
    return glob.glob('ps/*.pdf')


def n_pages_in_file(filename):
    with open(filename, 'rb') as f:
        pdf = PyPDF2.PdfFileReader(f, strict=False)
        return pdf.getNumPages()

def good_pdf(filename):
    for i in range(N_PDF_TRIES):
        try:
            n_pages_in_file(filename)
            return True
        except:
            pass
    return False

def get_dataframes(filename):
    n_pages = n_pages_in_file(filename)
    tables = camelot.read_pdf(
            filename,
            pages='1-{}'.format(n_pages),
            strict=False,
            flavor='lattice')
    tables = [t.df for t in tables]
    return tables

def good_df(df):
    return True

def df_to_okzs(df):
    rows = (row for (i, row) in df.iterrows())
    okz_row = False
    for row in rows:
        cell0 = row[0]
        if cell0 is not None:
            cell0 = str(cell0).strip()
        okz_row = okz_row and (cell0 is None or len(cell0) == 0)
        okz_row = okz_row or bool(PAT_OKZ_LABEL.search(cell0))
        if not okz_row:
            continue
        cells = map(str, row)
        cells = map(PAT_OKZ.finditer, cells)
        cells = itertools.chain.from_iterable(cells)
        cells = (m.group(0) for m in cells)
        for c in cells:
            yield c

def process_file(filename: str) -> typing.Tuple[str, list]:
    ps_code = PAT_PS_PDF.search(filename)
    assert ps_code, filename
    ps_code = ps_code.group(1)
    if not good_pdf(filename):
        return (ps_code, [])
    okzs = get_dataframes(filename)
    okzs = filter(good_df, okzs)
    okzs = map(df_to_okzs, okzs)
    okzs = itertools.chain.from_iterable(okzs)
    okzs = set(okzs)
    okzs = sorted(okzs)
    return (ps_code, okzs)

def retry_until_success(fun):
    """
    I would, of course, rather maintain a queue
    of pending tasks and a loop which feeds chunks
    from that queue into the pool and waits for
    queue to get empty and all of pool to finish its jobs,
    so that a task could be returned into the queue
    in case of failure and that no. of failures be
    accounted for somewhere outside the fmap.
    A saner way to implement this would probably
    be to allow fmap to yield "events" that can be
    either "results" or "failures".
    Those events would then be processed inside GIL in the main loop,
    with no need of explicit synchronization.
    However, I just want to get the fucking graph
    and don't mind spending CPU resources instead of my own cognitive ones"""

    def _fun(*args, **kwargs):
        while True:
            try:
                return fun(*args, **kwargs)
            except OSError:
                # I don't really care now
                pass
    _fun.__name__ = fun.__name__
    return _fun

def tee(writer):
    def _tee(row):
        writer(row)
        return row
    _tee.__name__ = 'tee'
    return _tee

def consume(iterator):
    collections.deque(iterator, maxlen=0)

def log_empty(fobj):
    def _log_empty(filename_and_codes: typing.Tuple[str, list]):
        filename, codes = filename_and_codes
        if len(codes) == 0:
            fobj.write(filename)
            fobj.write('\n')
    _log_empty.__name__ = 'log_empty'
    return _log_empty

def log_nonempty(fobj):
    def _log_nonempty(filename_and_codes: typing.Tuple[str, list]):
        filename, codes = filename_and_codes
        if len(codes) != 0:
            fobj.write(filename)
            fobj.write('\n')
    _log_nonempty.__name__ = 'log_nonempty'
    return _log_nonempty

def expand_singletone_product(x_times_ys: typing.Tuple[object, typing.Iterable]):
    x, ys = x_times_ys
    return ((x, y) for y in ys)


if __name__ == '__main__':
    files = get_files()
    print('About to proces {} files'.format(len(files)))
    with contextlib.ExitStack() as exit_stack:
        pool = exit_stack.enter_context(Pool(N_PROCESSES))
        f_out = exit_stack.enter_context(open('ps_to_okz.csv', 'w'))
        f_empties = exit_stack.enter_context(open('ps/empties.txt', 'w'))
        f_inhabitated = exit_stack.enter_context(open('ps/inhabitated.txt', 'w'))
        out = csv.writer(f_out)
        out.writerow(['program', 'okz'])
        f_out.flush()
        okzs = pool.imap_unordered(process_file, files, chunksize=8)
        # okzs = map(process_file, files)
        # okzs = map(retry_until_success(process_file), files)
        # okzs = map(tee(print), okzs)
        okzs = tqdm.tqdm(okzs)
        okzs = map(tee(log_empty(f_empties)), okzs)
        okzs = map(tee(log_nonempty(f_inhabitated)), okzs)
        okzs = map(expand_singletone_product, okzs)
        okzs = itertools.chain.from_iterable(okzs)
        okzs = map(tee(out.writerow), okzs)
        okzs = map(tee(lambda x: f_out.flush()), okzs)
        consume(okzs)
