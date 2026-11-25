import zipfile, io, struct, json, shutil, uuid, os, subprocess, tempfile

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, 'generated_reports', 'youtube_news_performance_dashboard_clean.pbit')
DST = os.path.join(BASE, 'generated_reports', 'youtube_dashboard_clean.pbit')
CSV_PATH = os.path.join(BASE, 'processed_data', 'youtube_data_clean.csv')
PBI_CSV = os.path.join(BASE, 'processed_data', 'youtube_data_clean_pbi.csv')
PBI_TOOLS = r'C:\pbi-tools\pbi-tools.core.exe'

TABLE = 'youtube_data_clean'
PAGE_W, PAGE_H = 1280, 720
ACCENT = '#2D5BFF'
INK = '#252423'
MUTED = '#605E5C'
GRID = '#EDEBE9'
BORDER = '#CFE3F2'
CANVAS = '#EAF4FC'

COLUMNS = [
    'Video_ID', 'Title', 'Published_Date', 'Content_Type', 'Duration_Seconds',
    'Views', 'Likes', 'Comments', 'Engagement_Rate', 'Channel', 'Published_Year',
    'Published_Month', 'Published_DayOfWeek', 'Published_Hour', 'Published_Quarter',
    'Duration_Minutes', 'Duration_Hours', 'Views_per_Minute', 'Like_Rate',
    'Comment_Rate', 'Is_Long_Form', 'Is_Short', 'Is_Live', 'Engagement_Category',
    'View_Category',
]

TYPE_MAP = {
    'Video_ID': 'type text', 'Title': 'type text', 'Published_Date': 'type datetime',
    'Content_Type': 'type text', 'Duration_Seconds': 'type number, "en-US"',
    'Views': 'type number, "en-US"', 'Likes': 'type number, "en-US"',
    'Comments': 'type number, "en-US"', 'Engagement_Rate': 'type number, "en-US"',
    'Channel': 'type text', 'Published_Year': 'type number, "en-US"',
    'Published_Month': 'type number, "en-US"', 'Published_DayOfWeek': 'type number, "en-US"',
    'Published_Hour': 'type number, "en-US"', 'Published_Quarter': 'type number, "en-US"',
    'Duration_Minutes': 'type number, "en-US"', 'Duration_Hours': 'type number, "en-US"',
    'Views_per_Minute': 'type number, "en-US"', 'Like_Rate': 'type number, "en-US"',
    'Comment_Rate': 'type number, "en-US"', 'Is_Long_Form': 'type logical',
    'Is_Short': 'type logical', 'Is_Live': 'type logical',
    'Engagement_Category': 'type text', 'View_Category': 'type text',
}

MEASURES = [
    ('Total Views', "SUM('%s'[Views])" % TABLE, '#,0'),
    ('Total Likes', "SUM('%s'[Likes])" % TABLE, '#,0'),
    ('Total Comments', "SUM('%s'[Comments])" % TABLE, '#,0'),
    ('Total Videos', "COUNTROWS('%s')" % TABLE, '#,0'),
    ('Median Views', "MEDIAN('%s'[Views])" % TABLE, '#,0'),
    ('Avg Engagement_Rate', "AVERAGE('%s'[Engagement_Rate])" % TABLE, '0.00%'),
    ('Avg Duration_Seconds', "AVERAGE('%s'[Duration_Seconds])" % TABLE, '#,0'),
    ('Avg Duration_Minutes', "AVERAGE('%s'[Duration_Minutes])" % TABLE, '#,0.0'),
    ('Avg Views per Minute', "AVERAGE('%s'[Views_per_Minute])" % TABLE, '#,0.0'),
    ('Avg Like_Rate', "AVERAGE('%s'[Like_Rate])" % TABLE, '0.00%'),
]


def m_path():
    return PBI_CSV.replace('\\', '\\\\')


def ensure_pbi_source():
    """Write a Power Query-native copy of the clean dataset.

    Power Query expects ISO-8601 datetimes, literal true/false booleans and plain
    decimal numbers, so the dashboard's M query needs no custom conversion logic.
    """
    if os.path.exists(PBI_CSV) and os.path.getmtime(PBI_CSV) >= os.path.getmtime(CSV_PATH):
        print('Power BI source up to date:', PBI_CSV)
        return PBI_CSV

    import pandas as pd

    print('Writing Power BI-native source from', CSV_PATH)
    df = pd.read_csv(CSV_PATH, dtype={'Video_ID': str})
    df['Published_Date'] = pd.to_datetime(df['Published_Date']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    for col in ('Is_Long_Form', 'Is_Short', 'Is_Live'):
        df[col] = df[col].astype(int).map({0: 'false', 1: 'true'})
    df.to_csv(PBI_CSV, index=False, encoding='utf-8', float_format='%.6f')
    print('Power BI source written:', PBI_CSV, os.path.getsize(PBI_CSV), 'bytes')
    return PBI_CSV


def build_m_expression():
    types = ', '.join('{"%s", %s}' % (c, TYPE_MAP[c]) for c in COLUMNS)
    final_columns = ', '.join('"%s"' % c for c in COLUMNS)
    return (
        'let\n'
        '    Source = Csv.Document(File.Contents("%s"),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),\n'
        '    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),\n'
        '    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers",{%s}),\n'
        '    #"Final Columns" = Table.SelectColumns(#"Changed Types",{%s})\n'
        'in\n'
        '    #"Final Columns"' % (m_path(), types, final_columns)
    )


def build_m_section():
    return 'section Section1;\n\nshared #"%s" = %s;' % (TABLE, build_m_expression())


def build_mashup():
    content_types = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="m" ContentType="application/m" />'
        '<Default Extension="xml" ContentType="application/xml" />'
        '</Types>'
    )
    package_xml = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<Package xmlns="http://schemas.microsoft.com/DataMashup">'
        '<Version>2.87.0</Version><MinVersion>2.87.0</MinVersion>'
        '<Culture>en-US</Culture>'
        '<Items><Item Id="Section1" Type="Section" /></Items>'
        '</Package>'
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types)
        zf.writestr('Formulas/Section1.m', build_m_section().encode('utf-8'))
        zf.writestr('Package.xml', package_xml)
    permissions = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<Permissions xmlns="http://schemas.microsoft.com/DataMashup/DataSourcePermissions" />'
    )
    metadata = b''
    return (
        struct.pack('<I', len(buf.getvalue())) + buf.getvalue()
        + struct.pack('<I', len(permissions)) + permissions
        + struct.pack('<I', len(metadata)) + metadata
    )


def read_base_model():
    with zipfile.ZipFile(SRC, 'r') as zf:
        schema = json.loads(zf.read('DataModelSchema').decode('utf-16le'))
    return schema


def build_model(schema):
    model = json.loads(json.dumps(schema))
    table = next(t for t in model['model']['tables'] if t['name'] == TABLE)
    table['measures'] = [
        {'name': name, 'expression': expression, 'formatString': fmt}
        for name, expression, fmt in MEASURES
    ]
    table['partitions'] = [{
        'name': TABLE,
        'mode': 'import',
        'source': {'type': 'm', 'expression': build_m_expression()},
    }]
    return model


def L(val):
    return {"expr": {"Literal": {"Value": val}}}


def L_bool(val):
    return L('true' if val else 'false')


def L_num(val, suffix='D'):
    return L(str(val) + suffix)


def L_str(val):
    return L("'" + val + "'")


def solid(color):
    return {"solid": {"color": L_str(color)}}


def vc_base(visual_id, visual_type, title, x, y, w, h):
    return {
        "name": visual_id,
        "layouts": [{"id": 0, "position": {"x": float(x), "y": float(y), "z": 0,
                                           "width": float(w), "height": float(h), "tabOrder": 0}}],
        "singleVisual": {
            "visualType": visual_type,
            "drillFilterOtherVisuals": True,
            "objects": {
                "background": [{"properties": {"show": L_bool(True), "color": solid('#FFFFFF'),
                                               "transparency": L_num(0)}}],
                "border": [{"properties": {"show": L_bool(True), "color": solid(BORDER)}}],
                "visualHeader": [{"properties": {"show": L_bool(False)}}],
                "vcObjects": {
                    "title": [{"properties": {"show": L_bool(True), "text": L_str(title),
                                              "fontColor": solid(INK), "fontSize": L_num(12)}}]
                }
            }
        }
    }


def select_items(items):
    out = []
    for kind, prop in items:
        out.append({
            kind: {
                "Expression": {"SourceRef": {"Source": TABLE}},
                "Property": prop
            },
            "Name": '%s.%s' % (TABLE, prop),
            "NativeReferenceName": prop
        })
    return out


def make_container(x, y, w, h, cfg):
    return {
        "x": x, "y": y, "width": w, "height": h,
        "config": json.dumps(cfg, separators=(',', ':'), ensure_ascii=False),
        "filters": "[]", "query": "{}", "dataTransforms": "{}"
    }


def make_card(measure, title, x, y, w, h, color=ACCENT):
    cfg = vc_base(str(uuid.uuid4()), 'card', title, x, y, w, h)
    cfg['singleVisual']['projections'] = {"Values": [{"queryRef": '%s.%s' % (TABLE, measure)}]}
    cfg['singleVisual']['prototypeQuery'] = {
        "Version": 2,
        "From": [{"Name": TABLE, "Entity": TABLE, "Type": 0}],
        "Select": select_items([('Measure', measure)])
    }
    cfg['singleVisual']['objects']['dataPoint'] = [{"properties": {"defaultColor": solid(color)}}]
    cfg['singleVisual']['objects']['labels'] = [{"properties": {"fontSize": L_num(20), "fontColor": solid(INK)}}]
    return make_container(x, y, w, h, cfg)


def make_bar_chart(category, measure, title, x, y, w, h, color=ACCENT, legend=False):
    cfg = vc_base(str(uuid.uuid4()), 'barChart', title, x, y, w, h)
    cfg['singleVisual']['projections'] = {
        "Category": [{"queryRef": '%s.%s' % (TABLE, category), "active": True}],
        "Y": [{"queryRef": '%s.%s' % (TABLE, measure)}]
    }
    cfg['singleVisual']['prototypeQuery'] = {
        "Version": 2,
        "From": [{"Name": TABLE, "Entity": TABLE, "Type": 0}],
        "Select": select_items([('Column', category), ('Measure', measure)])
    }
    objects = cfg['singleVisual']['objects']
    objects['legend'] = [{"properties": {"show": L_bool(legend)}}]
    objects['categoryAxis'] = [{"properties": {"show": L_bool(True), "labelColor": solid(MUTED)}}]
    objects['valueAxis'] = [{"properties": {"show": L_bool(True), "labelColor": solid(MUTED),
                                           "gridlineColor": solid(GRID)}}]
    objects['dataPoint'] = [{"properties": {"defaultColor": solid(color)}}]
    return make_container(x, y, w, h, cfg)


def make_line_chart(category, measure, title, x, y, w, h, color=ACCENT):
    cfg = vc_base(str(uuid.uuid4()), 'lineChart', title, x, y, w, h)
    cfg['singleVisual']['projections'] = {
        "Category": [{"queryRef": '%s.%s' % (TABLE, category), "active": True}],
        "Y": [{"queryRef": '%s.%s' % (TABLE, measure)}]
    }
    cfg['singleVisual']['prototypeQuery'] = {
        "Version": 2,
        "From": [{"Name": TABLE, "Entity": TABLE, "Type": 0}],
        "Select": select_items([('Column', category), ('Measure', measure)])
    }
    objects = cfg['singleVisual']['objects']
    objects['legend'] = [{"properties": {"show": L_bool(False)}}]
    objects['categoryAxis'] = [{"properties": {"show": L_bool(True), "labelColor": solid(MUTED)}}]
    objects['valueAxis'] = [{"properties": {"show": L_bool(True), "labelColor": solid(MUTED),
                                           "gridlineColor": solid(GRID)}}]
    objects['dataPoint'] = [{"properties": {"defaultColor": solid(color)}}]
    objects['lineStyles'] = [{"properties": {"strokeWidth": L_num(3), "showMarker": L_bool(False)}}]
    return make_container(x, y, w, h, cfg)


def make_slicer(column, title, x, y, w, h):
    cfg = vc_base(str(uuid.uuid4()), 'slicer', title, x, y, w, h)
    cfg['singleVisual']['projections'] = {"Values": [{"queryRef": '%s.%s' % (TABLE, column), "active": True}]}
    cfg['singleVisual']['prototypeQuery'] = {
        "Version": 2,
        "From": [{"Name": TABLE, "Entity": TABLE, "Type": 0}],
        "Select": select_items([('Column', column)])
    }
    return make_container(x, y, w, h, cfg)


def make_textbox(text, x, y, w, h, font_size=20, color=INK, bold=True):
    cfg = {
        "name": str(uuid.uuid4()),
        "singleVisual": {
            "visualType": "textbox",
            "objects": {
                "general": [{
                    "properties": {
                        "paragraphs": [{
                            "textRuns": [{
                                "value": text,
                                "textStyle": {
                                    "fontSize": '%dpt' % font_size,
                                    "fontFamily": "Segoe UI",
                                    "fontWeight": "bold" if bold else "normal",
                                    "color": color
                                }
                            }]
                        }]
                    }
                }]
            }
        }
    }
    return make_container(x, y, w, h, cfg)


def header(containers, page_label, subtitle):
    containers.append(make_textbox('YT', 14, 14, 42, 34, 16, ACCENT, True))
    containers.append(make_textbox('YouTube News Performance', 66, 10, 620, 36, 22, INK, True))
    containers.append(make_textbox(subtitle, 66, 44, 720, 22, 11, '#0F5EA8', False))
    containers.append(make_textbox(page_label, 1000, 18, 266, 28, 12, '#0F5EA8', True))


def section(name, containers):
    return {
        "name": name,
        "displayName": name,
        "filters": "[]",
        "ordinal": 0,
        "visualContainers": containers,
        "config": json.dumps({
            "objects": {
                "background": [{"properties": {"color": solid(CANVAS), "transparency": L_num(0)}}]
            }
        }, separators=(',', ':'), ensure_ascii=False),
        "width": PAGE_W,
        "height": PAGE_H
    }


def build_overview():
    c = []
    header(c, 'Overview', '650,000 videos | 20 news channels | Jan 2024 - Dec 2025 | source: youtube_data_clean.csv')

    card_w, card_h, gap = 210, 114, 8
    top = 84
    cards = [
        ('Total Views', 'TOTAL VIEWS', ACCENT),
        ('Total Likes', 'TOTAL LIKES', '#0F9D58'),
        ('Total Comments', 'TOTAL COMMENTS', '#F2A20C'),
        ('Avg Engagement_Rate', 'AVG ENGAGEMENT', '#E5484D'),
        ('Total Videos', 'TOTAL VIDEOS', '#7A5AF8'),
    ]
    for i, (measure, title, color) in enumerate(cards):
        c.append(make_card(measure, title, 14, top + i * (card_h + gap), card_w, card_h, color))

    rail_x, rail_w = 1072, 194
    c.append(make_textbox('FILTERS', rail_x, 58, rail_w, 22, 11, INK, True))
    c.append(make_slicer('Content_Type', 'Content Type', rail_x, 84, rail_w, 190))
    c.append(make_slicer('Channel', 'Channel', rail_x, 286, rail_w, 190))
    c.append(make_slicer('Engagement_Category', 'Engagement', rail_x, 488, rail_w, 190))

    c.append(make_bar_chart('Channel', 'Total Views', 'Total Views by Channel', 238, 84, 400, 280, ACCENT))
    c.append(make_line_chart('Published_Date', 'Total Views', 'Total Views Over Time', 650, 84, 408, 280, ACCENT))
    c.append(make_bar_chart('Content_Type', 'Total Views', 'Total Views by Content Type', 238, 376, 400, 280, '#0F9D58'))
    c.append(make_bar_chart('Channel', 'Avg Engagement_Rate', 'Avg Engagement Rate by Channel', 650, 376, 408, 280, '#E5484D'))
    return c


def build_channels():
    c = []
    header(c, 'Channels & Content', 'Depth views by channel, format and engagement band')

    c.append(make_bar_chart('Channel', 'Median Views', 'Median Views by Channel', 14, 84, 620, 280, ACCENT))
    c.append(make_bar_chart('Content_Type', 'Avg Views per Minute', 'Avg Views per Minute by Content Type', 646, 84, 620, 280, '#0F9D58'))
    c.append(make_bar_chart('Engagement_Category', 'Total Views', 'Total Views by Engagement Band', 14, 376, 400, 280, '#7A5AF8'))
    c.append(make_bar_chart('Published_Hour', 'Median Views', 'Median Views by Publishing Hour', 426, 376, 400, 280, '#F2A20C'))
    c.append(make_bar_chart('Channel', 'Total Videos', 'Published Video Count by Channel', 838, 376, 428, 280, '#0F5EA8'))

    rail_x, rail_w = 1072, 194
    c.append(make_textbox('FILTERS', rail_x, 58, rail_w, 22, 11, INK, True))
    c.append(make_slicer('Channel', 'Channel', rail_x, 84, rail_w, 190))
    c.append(make_slicer('Content_Type', 'Content Type', rail_x, 286, rail_w, 190))
    c.append(make_slicer('Published_Year', 'Year', rail_x, 488, rail_w, 190))
    return c


def build_pages():
    pages = [section('Overview', build_overview()), section('Channels & Content', build_channels())]
    for ordinal, page in enumerate(pages):
        page['ordinal'] = ordinal
        for z, vc in enumerate(page['visualContainers']):
            vc['z'] = z
    return pages


def report_config():
    return {
        "version": "5.59",
        "themeCollection": {"baseTheme": {"name": "CY24SU10", "version": "5.59", "type": 2}},
        "activeSectionIndex": 0,
        "defaultDrillFilterOtherVisuals": True,
        "slowDataSourceSettings": {
            "isCrossHighlightingDisabled": False,
            "isSlicerSelectionsButtonEnabled": False,
            "isFilterSelectionsButtonEnabled": False,
            "isFieldWellButtonEnabled": False,
            "isApplyAllButtonEnabled": False
        },
        "linguisticSchemaSyncVersion": 2,
        "settings": {}
    }


def build_layout():
    return {
        "id": "00000000-0000-0000-0000-000000000000",
        "pods": [],
        "resourcePackages": [],
        "sections": build_pages(),
        "theme": {"name": "CY24SU10", "version": "5.59"},
        "config": json.dumps(report_config(), separators=(',', ':'), ensure_ascii=False),
        "filters": "[]"
    }


def write_project(project_dir, model, pages):
    base = os.path.join(project_dir, 'pbi')
    os.makedirs(os.path.join(base, 'Model'), exist_ok=True)
    os.makedirs(os.path.join(base, 'Mashup', 'Package', 'Formulas'), exist_ok=True)
    os.makedirs(os.path.join(base, 'Report', 'sections'), exist_ok=True)

    with open(os.path.join(base, '.pbixproj.json'), 'w', encoding='utf-8') as fh:
        json.dump({
            "version": "0.10",
            "settings": {"model": {"serializationMode": "Raw"}, "pbiSessions": []},
            "queries": {TABLE: "Section1"}
        }, fh, indent=2)

    with open(os.path.join(base, 'Version.txt'), 'w', encoding='utf-8') as fh:
        fh.write('1.25')

    with open(os.path.join(base, 'ReportMetadata.json'), 'w', encoding='utf-8') as fh:
        json.dump({
            "Version": 5, "AutoCreatedRelationships": [], "FileDescription": "",
            "CreatedFrom": "Cloud", "CreatedFromRelease": "2024.11"
        }, fh, indent=2)

    with open(os.path.join(base, 'ReportSettings.json'), 'w', encoding='utf-8') as fh:
        json.dump({
            "Version": 1, "ReportSettings": {},
            "QueriesSettings": {
                "TypeDetectionEnabled": True, "RelationshipImportEnabled": True,
                "RunBackgroundAnalysis": True, "Version": "2.130.0.0"
            }
        }, fh, indent=2)

    with open(os.path.join(base, 'DiagramLayout.json'), 'w', encoding='utf-8') as fh:
        json.dump({
            "version": "1.1.0",
            "diagrams": [{"ordinal": 0, "scrollPosition": {"x": 0, "y": 0}, "nodes": [], "zoomValue": 100}]
        }, fh, indent=2)

    with open(os.path.join(base, 'Model', 'database.json'), 'w', encoding='utf-8') as fh:
        json.dump(model, fh, indent=2, ensure_ascii=False)

    with open(os.path.join(base, 'Mashup', 'Package', 'Formulas', 'Section1.m'), 'w', encoding='utf-8') as fh:
        fh.write(build_m_section())

    with open(os.path.join(base, 'Report', 'config.json'), 'w', encoding='utf-8') as fh:
        json.dump(report_config(), fh, indent=2)

    with open(os.path.join(base, 'Report', 'report.json'), 'w', encoding='utf-8') as fh:
        json.dump({
            "id": "00000000-0000-0000-0000-000000000000",
            "pods": [], "resourcePackages": [],
            "sections": [p['name'] for p in pages],
            "theme": {"name": "CY24SU10", "version": "5.59"},
            "config": "{}", "filters": "[]"
        }, fh, indent=2)

    for page in pages:
        with open(os.path.join(base, 'Report', 'sections', '%s.json' % page['name']), 'w', encoding='utf-8') as fh:
            json.dump(page, fh, indent=2, ensure_ascii=False)

    return base


def replace_entries(pbit_path, replacements):
    tmp = pbit_path + '.tmp'
    with zipfile.ZipFile(pbit_path, 'r') as src:
        with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as dst:
            for info in src.infolist():
                if info.filename not in replacements:
                    dst.writestr(info, src.read(info.filename))
            for name, data in replacements.items():
                dst.writestr(name, data)
    shutil.move(tmp, pbit_path)


def compile_with_pbi_tools(project_dir, out_path):
    result = subprocess.run(
        [PBI_TOOLS, 'compile', project_dir, out_path, 'PBIT', 'True'],
        capture_output=True, text=True, timeout=300
    )
    if not os.path.exists(out_path):
        raise RuntimeError('pbi-tools compile failed\nSTDOUT: %s\nSTDERR: %s' % (result.stdout, result.stderr))
    return result.stdout + result.stderr


def fallback_surgery():
    shutil.copy2(SRC, DST)
    layout_bytes = json.dumps(build_layout(), separators=(',', ':'), ensure_ascii=False).encode('utf-16le')
    replace_entries(DST, {
        'DataMashup': build_mashup(),
        'Report/Layout': layout_bytes,
        'DataModelSchema': json.dumps(build_model(read_base_model()),
                                      separators=(',', ':'), ensure_ascii=False).encode('utf-16le'),
    })


def main():
    if not os.path.exists(CSV_PATH):
        raise SystemExit('Clean dataset missing: %s' % CSV_PATH)
    ensure_pbi_source()

    pages = build_pages()
    model = build_model(read_base_model())
    layout_bytes = json.dumps(build_layout(), separators=(',', ':'), ensure_ascii=False).encode('utf-16le')

    with tempfile.TemporaryDirectory(prefix='pbi_build_') as tmp:
        project_dir = write_project(tmp, model, pages)
        out_path = os.path.join(tmp, 'output.pbit')
        log = compile_with_pbi_tools(project_dir, out_path)
        print(log.strip())
        replace_entries(out_path, {
            'DataMashup': build_mashup(),
            'Report/Layout': layout_bytes,
        })
        shutil.copy2(out_path, DST)

    print('Dashboard template saved to:', DST)
    print('Data source:', CSV_PATH)


if __name__ == '__main__':
    main()
