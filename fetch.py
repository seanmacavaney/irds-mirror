import io
import argparse
import ir_datasets
import pkgutil
import json
import sys
import datetime

parser = argparse.ArgumentParser()
parser.add_argument('download_id')
parser.add_argument('--list', action='store_true')
parser.add_argument('--expected_md5')
args = parser.parse_args()

if args.download_id.startswith('http://') or args.download_id.startswith('https://'):
  download = {'url': args.download_id, 'expected_md5': args.expected_md5}
else:
  downloads = json.loads(pkgutil.get_data('ir_datasets', 'etc/downloads.json'))

  def iter_downloads(data, prefix=''):
    if 'url' in data and 'expected_md5' in data:
      yield prefix, data
    elif 'instructions' in data:
      pass
    else:
      for key in data.keys():
        yield from iter_downloads(data[key], prefix=f'{prefix}/{key}' if prefix else key)

  download = None
  for dlid, dlc in iter_downloads(downloads):
    if args.list:
      print(f'{dlid}\t{dlc["url"]}\t{dlc["expected_md5"]}')
    if dlid == args.download_id:
      download = dlc
      if not args.list:
        break

  if not download:
    print(f'could not find download {args.download_id}')
    sys.exit(-1)

dlc = ir_datasets.util.Download([ir_datasets.util.RequestsDownload(download['url'])], expected_md5=download['expected_md5'], stream=True)
size = 0
with ir_datasets.util.finialized_file(download['expected_md5'], 'wb') as fout:
  with dlc.stream() as stream:
    inp = stream.read(io.DEFAULT_BUFFER_SIZE)
    while len(inp) > 0:
      fout.write(inp)
      size += len(inp)
      inp = stream.read(io.DEFAULT_BUFFER_SIZE)

def to_file_size(s):
  unit = 'B'
  units = ['KB', 'MB', 'GB']
  while (units and s > 1000):
    s = s / 1000
    unit = units.pop(0)
  if (unit == 'B'):
    s = f'{s:.0f}'
  else:
    s = f'{s:.1f}'
  return f'{s} {unit}'

with open('README.md', 'at') as f:
  f.write('| [`{expected_md5}`](https://mirror.ir-datasets.com/{expected_md5}) | {url} | {size} | {date} |\n'.format(**download, date=datetime.datetime.now().isoformat(), size=to_file_size(size)))
