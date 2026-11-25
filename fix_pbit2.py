import zipfile, io, struct, shutil

fixed_path = r'D:\bigdataproject\generated_reports\youtube_dashboard_fixed.pbit'
final_path = r'D:\bigdataproject\generated_reports\youtube_dashboard_final.pbit'

shutil.copy2(fixed_path, final_path)


new_m = """section Section1;

shared #"youtube_data_clean" = let
    Source = Csv.Document(File.Contents(CsvFilePath),[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers",{
        {"Video_ID", type text}, 
        {"Title", type text}, 
        {"Published_Date", type datetimezone}, 
        {"Content_Type", type text}, 
        {"Duration_Seconds", type number}, 
        {"Views", type number}, 
        {"Likes", type number}, 
        {"Comments", type number}, 
        {"Engagement_Rate", type number}, 
        {"Channel", type text}, 
        {"Published_Year", type number}, 
        {"Published_Month", type number}, 
        {"Published_DayOfWeek", type number}, 
        {"Published_Hour", type number}, 
        {"Published_Quarter", type number}, 
        {"Duration_Minutes", type number}, 
        {"Duration_Hours", type number}, 
        {"Views_per_Minute", type number}, 
        {"Like_Rate", type number}, 
        {"Comment_Rate", type number}, 
        {"Is_Long_Form", type logical}, 
        {"Is_Short", type logical}, 
        {"Is_Live", type logical}, 
        {"Engagement_Category", type text}, 
        {"View_Category", type text}
    })
in
    #"Changed Types";"""

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
    zf.writestr('Formulas/Section1.m', new_m.encode('utf-8'))
    zf.writestr('Package.xml', package_xml)
package_zip = buf.getvalue()

permissions = (
    b'<?xml version="1.0" encoding="utf-8"?>'
    b'<Permissions xmlns="http://schemas.microsoft.com/DataMashup/'
    b'DataSourcePermissions" />'
)
metadata = b''

mashup_bytes = (
    struct.pack('<I', len(package_zip)) + package_zip
    + struct.pack('<I', len(permissions)) + permissions
    + struct.pack('<I', len(metadata)) + metadata
)

tmp = final_path + '.tmp'
with zipfile.ZipFile(final_path, 'r') as src:
    with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            if info.filename != 'DataMashup':
                dst.writestr(info, src.read(info.filename))
        dst.writestr('DataMashup', mashup_bytes)
shutil.move(tmp, final_path)

print('Final .pbit saved to:', final_path)
print('Using datetimezone for Published_Date')