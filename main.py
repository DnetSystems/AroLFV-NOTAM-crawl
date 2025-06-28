import requests
from io import BytesIO
import re
from pypdf import PdfReader
import pdfplumber
import json
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime

with open("config.json", "r", encoding="utf-8") as file:
    config = json.load(file)

connection = psycopg2.connect(database=config["db"]["database"], user=config["db"]["user"], password=config["db"]["pass"], host=config["db"]["host"], port=config["db"]["port"])
cursor = connection.cursor()

notampage = requests.get(config["notampage"]).text
soup = BeautifulSoup(notampage, "html.parser")
links = soup.find_all("a", href=True)
used_links = []
matching_links = [
    link["href"] for link in links
    if "ESAA FIR IFR 24hr_" in link["href"] or "ESAA FIR 99days_" in link["href"]
]
for l in matching_links:
    full_url = config["basedomain"] + l if l.startswith("/") else l
    if full_url not in used_links:
        used_links.append(full_url)
        if "ESAA FIR IFR 24hr_" in full_url:
            twentyfourhr_link = full_url
        elif "ESAA FIR 99days_" in full_url:
            nintyninedays_link = full_url

def get_pdf_content(file_obj):
    all_lines = []
    with pdfplumber.open(file_obj) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                lines = text.splitlines()
                all_lines.extend(line.strip() for line in lines if line.strip())
    return all_lines

all_airports = {
        "24hrs":{},
        "all":{}
    }
airports = []

def notam_save(url, type):
    r = requests.get(url, timeout=5)
    pdf_file = BytesIO(r.content)

    lines = get_pdf_content(pdf_file)

    airport = ""
    active = False
    active_current_notam = False

    curr = {"notam":""}
    curr_airport = []
    snowtam = False

    for line in lines:
        if "AERODROMES" in line:
            active = True
            continue
        if active == False:
            continue
        if "EN-ROUTE" in line:
            break
        if re.search(r'Page (\d+) of (\d+)', line) or line == "":
            continue
        
        if re.fullmatch(r'[A-Z]\d{4}/\d{2}', line):
            continue

        if "No information received or matching the query" in line:
            all_airports[type][airport] = []
            curr_airport = []
            continue
        # and line.lstrip().count(" ") == 2
        if re.match(r"^[A-Z]{4} - ", line) and "AREA - " not in str(line):
            if curr["notam"]:
                curr["type"] = type
                curr_airport.append(curr)
            all_airports[type][airport] = curr_airport
            curr = {
                "notam":""
            }
            curr_airport = []
            airport = line.split(" - ")[0]
            snowtam = False
            airports.append({"code":airport, "name":line.replace(" - ", "/")})
            continue

        if "SNOWTAM" in line:
            snowtam = True
            continue

        if snowtam == True:
            continue
        
        print(line)

        if "+ " in line:
            if curr["notam"]:
                curr["type"] = type

                curr_airport.append(curr)
            curr = {
                "from":None,
                "to":None,
                "notam":None,
                "perm":None,
                "active":None
            }
            curr["notam"] = line.replace("+ ", "")

        else:
            if "FROM:" in line or "TO: " in line:
                line = line.replace("EST", "").replace(" JAN ", "/01/").replace(" FEB ", "/02/").replace(" MAR ", "/03/").replace(" APR ", "/04/").replace(" MAY ", "/05/").replace(" JUN ", "/06/").replace(" JUL ", "/07/").replace(" AUG ", "/08/").replace(" SEP ", "/09/").replace(" OCT ", "/10/").replace(" NOV ", "/11/").replace(" DEC ", "/12/")
                line = re.sub(r'\b[A-Z]\d{4}/\d{2}\b', '', line)
                print(str(line))
                try:
                    curr["to"] = line.split("O: ")[1].lstrip().strip().replace("/", "-")
                    if str(curr["to"]) == "PERM":
                        curr["to"] = "9999-12-31 23:59:59"
                        curr["perm"] = True
                    else:
                        curr["to"] = curr["to"] + ":00"
                        curr["to"] = datetime.strptime(curr["to"], "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                        curr["perm"] = False
                except Exception as e:
                    print("error line#: ",str(e.__traceback__.tb_lineno) + "--")
                try:
                    curr["from"] = line.replace("FROM: ", "").split(" TO: ")[0].lstrip().replace("/", "-") + ":00"
                    curr["from"] = datetime.strptime(curr["from"], "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print("error line#: ",str(e.__traceback__.tb_lineno) + "--")
                try:
                    starttime = datetime.strptime(curr["from"], '%Y-%m-%d %H:%M:%S')
                    endtime = datetime.strptime(curr["to"], '%Y-%m-%d %H:%M:%S')
                    if starttime <= datetime.now() <= endtime:
                        curr["active"] = True
                    else:
                        curr["active"] = False
                except Exception as e:
                    print("error line#: ",str(e.__traceback__.tb_lineno) + "--")
            else:
                curr["notam"] += " " + line
cursor.execute('''DELETE from current_notamdata''', ())

twentyfourhrs_notamslist = []

notam_save(twentyfourhr_link, "24hrs")
for localairport, localairportdata in all_airports["24hrs"].items():
    for locdata in localairportdata:
        try:
            if locdata["from"] == None or locdata["to"] == None:
                continue
            query = f"""
            INSERT INTO current_notamdata (
                type, notamtext, starttime, endtime, airport, updated, perm, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                locdata["type"], locdata["notam"], locdata["from"], locdata["to"].lstrip().strip(), localairport, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), locdata["perm"], locdata["active"]
            ))
            twentyfourhrs_notamslist.append(locdata["notam"] + localairport + locdata["from"] + locdata["to"])
        except Exception as e:
            print(str(locdata))
            print("error line#: ",str(e.__traceback__.tb_lineno) + "--" + str(e))

notam_save(nintyninedays_link, "all")
for localairport, localairportdata in all_airports["all"].items():
    for locdata in localairportdata:
        try:
            if locdata["from"] == None or locdata["to"] == None:
                continue
            localdatainfo = locdata["notam"] + localairport + locdata["from"] + locdata["to"]
            if localdatainfo in twentyfourhrs_notamslist:
                print("Already exists with 24hrs")
                continue
            query = f"""
            INSERT INTO current_notamdata (
                type, notamtext, starttime, endtime, airport, updated, perm, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                locdata["type"], locdata["notam"], locdata["from"], locdata["to"], localairport, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), locdata["perm"], locdata["active"]
            ))
        except Exception as e:
            print(str(locdata))
            print("error line#: ",str(e.__traceback__.tb_lineno) + "--" + str(e))

for airport in airports:
    query = f"SELECT COUNT(*) FROM airports WHERE airport_code = %s"
    cursor.execute(query, (airport["code"],))
    exists = cursor.fetchone()[0] > 0
    if exists:
        update_query = f"""
            UPDATE airports
            SET airport_full = %s
            WHERE airport_code = %s
            """
        cursor.execute(update_query, (airport["name"], airport["code"]))
    else:
        query = f"""
        INSERT INTO airports (
            airport_code, airport_full
        ) VALUES (%s, %s)
        """
        cursor.execute(query, (
            airport["code"], airport["name"]
        ))

connection.commit()