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

N_PROCESSES = 2

# no time to fix camelot, just set set ulimit -n LARGE_NUMBER


def get_files():
    return glob.glob('programs/*.pdf')


def n_pages_in_file(filename):
    with open(filename, 'rb') as f:
        pdf = PyPDF2.PdfFileReader(f, strict=False)
        return pdf.getNumPages()

def get_dataframes(filename):
    n_pages = n_pages_in_file(filename)
    tables = camelot.read_pdf(
            filename,
            pages='1-{}'.format(n_pages),
            strict=False,
            flavor='lattice')
    tables = [t.df for t in tables]
    return tables

def is_profstandards_df(df):
    try:
        first_cell = ' '.join(map(str, df.iloc[0, :]))
        first_cell = PAT_WHITESPACES.sub(' ', first_cell)
        return bool(PAT_PROFSTANDARDS_HEADER.match(first_cell))
    except IndexError:
        return False

def get_profstandards_df(dfs):
    assert len(dfs) > 0
    return next(filter(is_profstandards_df, dfs))

def df_to_profstandard_codes(df):
    try:
        df = df.iloc[:, 1]
    except IndexError:
        return
    for s in df:
        if not isinstance(s, str):
            continue
        m = PAT_PROFSTANDARD_CODE.search(s)
        if not m: 
            continue
        yield m.group(0)

def filename_to_codes(filename: str) -> typing.Tuple[str, list]:
    program_code = PAT_PROGRAM_PDF.search(filename)
    assert program_code, filename
    program_code = program_code.group(1)
    # return (program_code, ['10.003'])
    profstandards = get_dataframes(filename)
    profstandards = get_profstandards_df(profstandards)
    profstandards = df_to_profstandard_codes(profstandards)
    profstandards = list(profstandards)
    return (program_code, profstandards)

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
        f_out = exit_stack.enter_context(open('program_to_profstandard.csv', 'w'))
        f_empties = exit_stack.enter_context(open('programs/empties.txt', 'w'))
        f_inhabitated = exit_stack.enter_context(open('programs/inhabitated.txt', 'w'))
        out = csv.writer(f_out)
        out.writerow(['program', 'profstandard'])
        profstandards = pool.imap_unordered(filename_to_codes, files, chunksize=4)
        # profstandards = map(retry_until_success(filename_to_codes), files)
        # profstandards = map(tee(print), profstandards)
        profstandards = tqdm.tqdm(profstandards)
        profstandards = map(tee(log_empty(f_empties)), profstandards)
        profstandards = map(tee(log_nonempty(f_inhabitated)), profstandards)
        profstandards = map(expand_singletone_product, profstandards)
        profstandards = itertools.chain.from_iterable(profstandards)
        profstandards = map(tee(out.writerow), profstandards)
        consume(profstandards)
