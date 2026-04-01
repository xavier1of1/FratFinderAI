import requests, bs4, re
from urllib.parse import urlparse
NIC='https://nicfraternity.org/member-fraternities/'
UA={'User-Agent':'Mozilla/5.0'}
html=requests.get(NIC,timeout=25,headers=UA).text
soup=bs4.BeautifulSoup(html,'html.parser')
rows=[]
for a in soup.find_all('a',href=True):
    name=' '.join(a.get_text(' ',strip=True).split())
    href=a['href'].strip()
    if not name or len(name)<3:
        continue
    if href.startswith('#'):
        continue
    if 'nicfraternity.org' in href or 'myfraternitylife.org' in href:
        continue
    if re.search(r'(facebook|instagram|twitter|linkedin|youtube)\\.com', href, re.I):
        continue
    if re.search(r'^(about|ifc|programs|media|contact)$', name, re.I):
        continue
    if re.search(r'fraternities at a glance|logo', name, re.I):
        continue
    rows.append((name,href))
seen=set(); uniq=[]
for n,h in rows:
    key=(n.lower(), urlparse(h).netloc.lower())
    if key in seen:
        continue
    seen.add(key)
    uniq.append((n,h))
clean=[]
for n,h in uniq:
    if any(x in n.lower() for x in ['alliance','partner','staff directory','contact us','career','application']):
        continue
    clean.append((n,h))
print('TOTAL_CANDIDATES',len(clean))
for n,h in clean[:40]:
    print(f'{n}\t{h}')
