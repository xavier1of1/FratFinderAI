import requests, bs4, re, json
from urllib.parse import urljoin
UA={'User-Agent':'Mozilla/5.0'}
sites=[
 ('Acacia','https://www.acacia.org/'),('Alpha Chi Rho','http://www.alphachirho.org/'),('Alpha Delta Gamma','http://www.alphadeltagamma.org/'),('Alpha Delta Phi','https://www.alphadeltaphi.org/'),('Alpha Gamma Rho','https://www.alphagammarho.org/'),('Alpha Tau Omega','https://ato.org/'),('Beta Theta Pi','https://beta.org/'),('Beta Upsilon Chi','https://betaupsilonchi.org/'),('Chi Phi','https://chiphi.dynamic.omegafi.com/'),('Chi Psi','https://www.chipsi.org/'),('Delta Chi','https://deltachi.org/'),('Delta Kappa Epsilon','https://dke.org/'),('Delta Sigma Phi','https://www.deltasig.org/'),('Delta Tau Delta','https://www.delts.org/'),('Delta Upsilon','https://www.deltau.org/'),('Kappa Delta Rho','https://www.kdr.com/'),('Lambda Chi Alpha','https://www.lambdachi.org/'),('Phi Gamma Delta','https://www.phigam.org/'),('Sigma Chi','https://sigmachi.org/'),('Sigma Phi Society','https://www.sigmaphi.org/'),('Tau Kappa Epsilon','https://www.tke.org/')]

def fetch(url):
    return requests.get(url,timeout=8,allow_redirects=True,headers=UA)

out=[]
for name,base in sites:
    rec={'name':name,'base':base,'status':None,'final_url':None,'chapterish_links':[],'error':None}
    try:
        r=fetch(base)
        rec['status']=r.status_code; rec['final_url']=r.url
        soup=bs4.BeautifulSoup(r.text,'html.parser')
        links=[]
        for a in soup.find_all('a',href=True):
            txt=' '.join(a.get_text(' ',strip=True).split())
            href=a['href'].strip()
            t=txt.lower(); h=href.lower()
            if any(k in t for k in ['chapter','chapters','find a chapter','directory','locations','alphas','go to site','chapter map']) or any(k in h for k in ['chapter','chapters','find-a-chapter','directory','locations','alphas']):
                links.append({'text':txt[:120] if txt else '[no-text]','url':urljoin(r.url,href)})
        seen=set(); ded=[]
        for item in links:
            k=(item['text'],item['url'])
            if k in seen: continue
            seen.add(k); ded.append(item)
        rec['chapterish_links']=ded[:12]
    except Exception as e:
        rec['error']=f'{type(e).__name__}: {str(e)[:120]}'
    out.append(rec)

with open('research_nav_21.json','w',encoding='utf-8') as f:
    json.dump(out,f,indent=2)
print('WROTE',len(out),'records')
for rec in out:
    print(f"{rec['name']}\tstatus={rec['status']}\tlinks={len(rec['chapterish_links'])}\terror={rec['error'] or ''}")
