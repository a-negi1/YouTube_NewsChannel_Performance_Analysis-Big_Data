import zipfile, json, io, struct, re

PBIT = r'D:\bigdataproject\New folder\PROJECT\generated_reports\youtube_dashboard_clean.pbit'


def mashup_m(data):
    size = struct.unpack('<I', data[:4])[0]
    package = zipfile.ZipFile(io.BytesIO(data[4:4 + size]))
    return package.read('Formulas/Section1.m').decode('utf-8-sig')


with zipfile.ZipFile(PBIT, 'r') as zf:
    names = zf.namelist()
    print('Parts:', ', '.join(names))
    layout = json.loads(zf.read('Report/Layout').decode('utf-16le'))
    schema = json.loads(zf.read('DataModelSchema').decode('utf-16le'))
    m = mashup_m(zf.read('DataMashup'))

src = re.search(r'File\.Contents\("([^"]+)"\)', m)
print('M source path:', src.group(1) if src else 'NOT FOUND')

table = next(t for t in schema['model']['tables'] if t['name'] == 'youtube_data_clean')
measures = {x['name'] for x in table.get('measures', [])}
columns = {x['name'] for x in table.get('columns', [])}
print('Model measures (%d): %s' % (len(measures), ', '.join(sorted(measures))))

ok = True
for sec in layout.get('sections', []):
    print('\nPage: %s (%d visuals)' % (sec['displayName'], len(sec['visualContainers'])))
    for vc in sec['visualContainers']:
        cfg = json.loads(vc['config'])
        sv = cfg.get('singleVisual', {})
        vtype = sv.get('visualType')
        refs = []
        for query in sv.get('prototypeQuery', {}).get('Select', []):
            for kind in ('Measure', 'Column'):
                if kind in query:
                    refs.append((kind, query[kind]['Property']))
        for kind, prop in refs:
            pool = measures if kind == 'Measure' else columns
            if prop not in pool:
                ok = False
                print('  MISSING %s: %s' % (kind, prop))
        print('  %-10s %-42s %s' % (vtype, ', '.join('%s:%s' % r for r in refs) or '-',
                                    'x%s y%s w%s h%s' % (vc['x'], vc['y'], vc['width'], vc['height'])))

print('\nAll visual references resolve:', ok)
