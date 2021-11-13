
# -*- coding: utf-8 -*-
import time
import warnings
import requests
import re
import MySQLdb
import os
import sys
import datetime
from bs4 import BeautifulSoup as bs
from os import path
from PyPDF2 import PdfFileWriter as pdfyaz
from PyPDF2 import PdfFileReader
from fuzzywuzzy import fuzz
cont=0
warnings.simplefilter("ignore", DeprecationWarning)
startmain = time.time()
db = MySQLdb.connect(host="localhost", user="username", passwd="password", db="dbname", use_unicode=True,
                         charset='utf8')
c = db.cursor()
c.executemany("""INSERT INTO crontab (tarih) VALUES (%s)""",(datetime.datetime.now(),)) #crontab Recorder
db.commit()
print('Dosya İndirme İşlemi Başladı.')
# Fill in your details here to be posted to the login form.
payload = {
    'Email': 'ibrahimcansarisakal@gmail.com',#Bulletin username
    'Password': 'You can get in touch with email.'#Bulletin passwd
}

# Use 'with' to ensure the session context is closed after use.
with requests.Session() as s:
    p = s.post('https://bulten.turkpatent.gov.tr/bulten/login/loginuser', data=payload)
    # print the html returned or something more intelligent to see if it's a successful login page.
    bultenList = s.get('https://bulten.turkpatent.gov.tr/bulten/bulletinList/')
    # print(bultenList.text)
    bultenler = bs(bultenList.content,'html.parser')
    markaBultenleri = bultenler.find_all(lambda tag: tag.name == "a" and 'Marka Bülteni' in tag.text)
    if len(markaBultenleri)>0:
        HTMLsonBultenNo = re.findall('(\s\d{3})', markaBultenleri[0].text)[0].strip()
        HTMLsonBultenDate = re.findall('\d{2}\.\d{2}\.\d{4}', markaBultenleri[0].text)[0].strip()
        c.execute("""SELECT ID,brandName FROM brands
                              WHERE status = %s AND references = %s LIMIT 1""", (0,str(HTMLsonBultenNo)+'.pdf')) #get last added bulletin number from DB

        firmalar = c.fetchall()
        if firmalar:
            print(str(HTMLsonBultenNo)+' Numaralı Marka Bülteni Daha Önce Sisteme Kaydedilmiştir. İşlem Durduruldu ')
            exit()

        bultenPage = s.get('https://bulten.turkpatent.gov.tr' + markaBultenleri[0].get('href'))

        bultenFilesJSONUrlBs4 = bs(bultenPage.content, 'lxml').find(#linuxta lxml kullanılıyor winde html5lib #html5lib for win10 or lxml for ubuntu
            lambda tag: tag.name == "script" and 'initGrid' in tag.text)

        if bultenFilesJSONUrlBs4:
            bultenFilesJSONUrl = re.search("('/bulten//getList\?id=.+([^'])'([^']))", str(bultenFilesJSONUrlBs4)).group(
                0)
            bultenFilesJSONUrl = bultenFilesJSONUrl.replace(',', '')
            bultenFilesJSONUrl = bultenFilesJSONUrl.replace("'", '')
            bultenFilesJSONGet = s.get('https://bulten.turkpatent.gov.tr' + bultenFilesJSONUrl)
            bultenFilesJSON = bultenFilesJSONGet.json()
            # print(bultenFilesJSON)
            bultenPDFlink = "https://bulten.turkpatent.gov.tr/bulten/downloadFile?fileId=" + str(
                bultenFilesJSON['rows'][-1]['ttpId'])
            # print(bultenFilesJSON['rows'][-1])

            fileDownloaded = open('/path/to/pdf/'+str(HTMLsonBultenNo)+'.pdf', 'wb').write(
                s.get(bultenPDFlink, allow_redirects=True).content)

        # bultenIcerikler = bs(bultenPage.content,'html.parser')
        # bultenIceriklerDiv = bultenIcerikler.find_all('div',attrs={'class':'datagrid-cell'})
        # print(soup)
print('Dosya İndirme İşlemi Bitti.'+str(HTMLsonBultenNo)+'.pdf')
print('It took download', time.time() - startmain, 'seconds.')

startPDF = time.time()

print("botPDFtoDataST.py Başladı")
# ayristirici.add_argument('-h', '--help', nargs='+', required=False, help="Yardim Ekrani")
if path.exists('/path/to/pdf/'+str(HTMLsonBultenNo)+'.pdf'):
    #c = db.cursor()
    output = ''
    warnings.simplefilter("ignore", DeprecationWarning)

    pdfFileObj = open('/path/to/pdf/'+str(HTMLsonBultenNo)+'.pdf', 'rb')

    # print(pdfFileObj)
    pdfReader = PdfFileReader(pdfFileObj)
    for i in range(pdfReader.getNumPages()):
        pageObj = pdfReader.getPage(i)
        outputPre = pageObj.extractText()
        outputTrans = outputPre.maketrans("ÝÐÞýðþ", "İĞŞığş")
        output += outputPre.translate(outputTrans)
    sqlArray = []
    markalar = output.split('(210)')[1:]
    for marka in markalar:
        detaylarArray = re.split('(\([0-9]{3}\))', marka)
        if len(detaylarArray) >= 10:
            if (re.search('(\w{4}/(\w{8}))', marka)):
                detaylarArray[10] = ' '

            if detaylarArray[0]:
                markaBasvuruNo = detaylarArray[0]
            else:
                markaBasvuruNo = None
            if detaylarArray[2]:
                markaBasvuruTarihi = detaylarArray[2]
            else:
                markaBasvuruTarihi = None
            if detaylarArray[8]:
                niceCode = detaylarArray[8]
            else:
                niceCode = None
                break
            if detaylarArray[6]:
                markaAd = detaylarArray[6]
            else:
                markaAd = None

            if detaylarArray[10]:
                hizmetListesi = detaylarArray[10]
            else:
                hizmetListesi = None
                break

            if detaylarArray[4]:
                markaSahipBilgiler = detaylarArray[4]
            else:
                markaSahipBilgiler = None
            if HTMLsonBultenDate:
                markaBultenTarih = HTMLsonBultenDate.strip()
            else:
                markaBultenTarih = None
            sqlArray.append(
                (markaBultenTarih, markaBasvuruNo, markaBasvuruTarihi, niceCode, markaAd, hizmetListesi, markaSahipBilgiler,
                 str(HTMLsonBultenNo)+'.pdf'))
    print(len(sqlArray))
    try:
        try:
            c.executemany(
                """INSERT INTO brands (brandDate, brandNo, brandRegisDate, niceCode, brandName, serviceList, brandOwnerInfo, references) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)""",
                sqlArray)
            db.commit()
            print("Database Kaydı Tamamlandı")
        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)

    except TypeError as e:
        print(e)
    print(time.time() - startPDF)

else:
    print("olmadı")


startMatch = time.time()
c.execute("""SELECT ID,REPLACE(brandName, '\n', '')  FROM brands
          WHERE status = %s AND references = %s""", (0,str(HTMLsonBultenNo)+'.pdf'))
firmalar = c.fetchall()

if firmalar:
    print('Marka Eşleştirme İşlemi Başladı')
    c.execute("""SELECT ID,REPLACE(REPLACE(name, 'Şti.', ''), 'Ltd.', '')  FROM customers
              WHERE status = %s""", (0,))
    cari = c.fetchall()
    # print(cari)
    sqlArray=[]
    for firmaID,markaAd in firmalar:
        for cariID, ad in cari:
            if (fuzz.partial_ratio(markaAd.lower(), ad.lower())>=60) and (fuzz.ratio(markaAd.lower(), ad.lower())>=50) and (fuzz.token_sort_ratio(markaAd.lower(), ad.lower())>=50):

                sqlArray.append((firmaID, cariID, fuzz.ratio(markaAd.lower(), ad.lower()),fuzz.partial_ratio(markaAd.lower(), ad.lower()), fuzz.token_sort_ratio(markaAd.lower(), ad.lower()),str(HTMLsonBultenNo)+'.pdf'))

    if len(sqlArray)>0:
        try:
            try:
                c.executemany(
                    """INSERT INTO brandCostumerSimilarity (bulletinID, costumerID, ratio, partialRatio, tokenRatio, references) VALUES (%s, %s, %s, %s, %s, %s)""",
                    sqlArray
                )
                db.commit()
                print('Marka Eşleştirme İşlemi Bitti')
                print('It took', time.time() - startMatch, 'seconds.')
            except (MySQLdb.Error, MySQLdb.Warning) as e:
                print(e)

        except TypeError as e:
            print(e)
    else:
        print(sqlArray)
