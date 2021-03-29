import requests
proxy = 'http://159.65.69.186:9300'
proxy = {"https": proxy}
link = 'https://www.zillow.com/homedetails/4032-Berkman-Dr-Austin-TX-78723/83125316_zpid/'
_user = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'
_accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
_acceptEncoding = 'gzip, deflate, br'
_acceptLanguage = 'en-US,en;q=0.5'
_referer = 'https://www.zillow.com/homes/for_rent/'
_headers = {'User-Agent': _user,
            'Accept': _accept,
            'Accept-Encoding': _acceptEncoding,
            'Accept-Language': _acceptLanguage}
try:
    page = requests.get(link, headers=_headers, proxies=proxy)
    print(page.content)
except Exception as e:
    print(proxy)
    print(e)

