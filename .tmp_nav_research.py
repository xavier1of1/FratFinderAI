import requests, bs4, re
from urllib.parse import urljoin, urlparse
UA={'User-Agent':'Mozilla/5.0'}
# handpicked 20 from NIC results
sites=[
 ('Acacia','https://www.acacia.org/'),
 ('Alpha Chi Rho','http://www.alphachirho.org/'),
 ('Alpha Delta Gamma','http://www.alphadeltagamma.org/'),
 ('Alpha Gamma Rho','https://www.alphagammarho.org/'),
 ('Alpha Tau Omega','https://ato.org/'),
 ('Beta Theta Pi','https://beta.org/'),
 ('Beta Upsilon Chi','https://betaupsilonchi.org/'),
 ('Chi Phi','https://chiphi.dynamic.omegafi.com/'),
 ('Chi Psi','https://www.chipsi.org/'),
 ('Delta Chi','https://deltachi.org/'),
 ('Delta Kappa Epsilon','https://dke.org/'),
 ('Delta Sigma Phi','https://www.deltasig.org/'),
 ('Delta Tau Delta','https://www.delts.org/'),
 ('Delta Upsilon','https://www.deltau.org/'),
 ('Kappa Delta Rho','https://www.kdr.com/'),
 ('Lambda Chi Alpha','https://www.lambdachi.org/'),
 ('Phi Gamma Delta','https://www.phigam.org/'),
 ('Sigma Chi','https://sigmachi.org/'),
 ('Sigma Phi Society','https://www.sigmaphi.org/'),
 ('Tau Kappa Epsilon','https://www.tke.org/'),
]
paths=['/chapters','/chapters/','/chapter-directory','/chapter-directory/','/find-a-chapter','/find-a-chapter/','/directory','/directory/','/locations','/alphas','/our-chapters','/about/overview/our-chapters/','/join-tke/find-a-chapter/']

def get(url):
    return requests.get(url,timeout=20,allow_redirects=True,headers=UA)

for name,base in sites:
    print('\n===',name,'===')
    try:
        r=get(base)
    except Exception as e:
        print('home_error',type(e).__name__,str(e)[:120]); continue
    print('home',r.status_code,'->',r.url)
    soup=bs4.BeautifulSoup(r.text,'html.parser')
    found=[]
    for a in soup.find_all('a',href=True):
        txt=' '.join(a.get_text(' ',strip=True).split())
        href=a['href'].strip()
        t=txt.lower(); h=href.lower()
        if any(k in t for k in ['chapter','chapters','find a chapter','directory','locations','alphas','go to site']) or any(k in h for k in ['chapter','chapters','find-a-chapter','directory','locations','alphas']):
            full=urljoin(r.url,href)
            found.append((txt[:70] or '[no-text]',full))
    ded=[]; seen=set()
    for t,u in found:
        key=(t,u)
        if key in seen: continue
        seen.add(key)
        ded.append((t,u))
    for t,u in ded[:8]:
        print('link',t,'=>',u)
    # probe common chapter paths
    ok=[]
    for p in paths:
        try:
            rr=get(urljoin(r.url,p))
            if rr.status_code < 400:
                ok.append((p,rr.status_code,rr.url))
        except Exception:
            pass
    for p,s,u in ok[:4]:
        print('path',p,s,'->',u)
