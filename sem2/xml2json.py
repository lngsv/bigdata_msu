import xml.etree.ElementTree as ET
import json

tree = ET.parse('sheets_info.xml')
root = tree.getroot()

info = [
    {col.tag: col.text for col in row} 
    for row in root
]

info_view = [{
    'description': r.get('Description', ''),
    'library_info': r['Contacts'],
    'publication_type': r['PublicationType'],
    'author': r.get('Author', ''),
    'title': r['Title'],
    'publication_year': r['PublicationYear'],
} for r in info]

print(json.dumps(info_view, indent=4, ensure_ascii=False))
