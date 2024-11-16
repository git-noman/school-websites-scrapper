import requests
from bs4 import BeautifulSoup
import pandas as pd
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.common.by import By
import spacy
from spacy.cli import download

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from time import sleep
import time
from io import StringIO
import numpy as np
import json
import traceback

import pymysql


### Preload code

# Load the file
file_path = "./district_domains.xlsx"
df = pd.read_excel(file_path)

# Get all urls in the file
urls = []
for web_number in ["website_1", "website_2", "website_3", "website_4", "website_5"]:
    for url in df[web_number].dropna().tolist():
        urls.append(url)




### Meta Classes

## Console Color Class
class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

## General Util Class
class Utility:
    headers = {'User-Agent': 'Mozilla/5.0'}


    # Get proxies from proxies.txt
    # Proxies should be in `http: proxy:port https: proxy:port` format
    def get_proxies():
        proxies = []
        with open("proxies.txt", "r") as f:
            for line in f.read().split("\n"):
                http = line.split("http")[1].split("https")[0].replace(": ", "").replace(" ", "")
                https = line.split("https")[1].replace(": ", "").replace(" ", "")
                proxy = {"http": f"http://{http}", "https": f"http://{https}"}
                proxies.append(proxy)

        return proxies

    def get_from_excel(field):
        return df[field].dropna().tolist()

    # Wrapper over requests and BeautifulSoup
    def get_soup(url, **kwargs):
        timeout = kwargs.get("timeout", 15)

        r = requests.get(url, headers=Utility.headers, timeout=timeout)
        return BeautifulSoup(r.content, "html.parser")

    # Raw selenium.get wrapper
    def get_selenium_raw(self):
        options = Options()
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36")
        options.add_argument('headless')
        options.add_argument('--disable-gpu')

        driver = webdriver.Chrome(options=options)
        return driver

    # Wrapper over selenium.get
    # Returns html
    def get_selenium(url):
        print(f"{colors.OKBLUE}get_selenium: doc: {url} {colors.ENDC}")
        options = Options()
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36")
        options.add_argument('headless')
        options.add_argument('--disable-gpu')

        driver = webdriver.Chrome(options=options)
        driver.get(url)
        sleep(5)
        html = driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML").encode('utf-8').strip()
        driver.close()

        return html
    
    # Replaces a term into a relevant set parameter
    # eg the term Job Title is replaced to Honorific
    # see Utility.process_into_parameters docs for more details on the parameters
    def replace_into_params(key, value):
        # key = 'Job Title' etc
        # value = 'Principal' etc
        bundle = []
        lowkey = str(key).lower()
        lowvalue = str(value).lower()

        if "title" in lowkey:
            key = "Honorific"
        if "position" in lowkey:
            key = "Honorific"
        
        bundle.append(key)
        bundle.append(value)

        return bundle

    # Parse a name (str) into a list containing First Name, Last Name
    def replace_name(name):
        bundle = []
        first_name = None
        last_name = None
        name = str(name) # float? convert that shi
        # Lastname, Firstname
        if "," in name:
            last_name, first_name = name.split(",",1)
        # Firstname Lastname
        elif " " in name:
            first_name, last_name = name.split(" ",1)
        # mr/mrs.name
        elif "." in name:
            first_name, last_name = name.split(".",1)
        else:
            first_name, last_name = name, name

        bundle.append(first_name.replace(" ", ""))
        bundle.append(last_name.replace(" ", ""))
        return bundle
    
    # Turns a list of directories into a list of visit-able urls
    def directories_to_urls(subw, directories):
        parsed_directories = []
        url = Utility.remove_slash(subw)

        for directory in directories:
            directory = Utility.remove_slash(directory)
            if "http" in directory or "www." in directory:
                parsed_directories.append(directory)
            else:
                parsed_directories.append(f'{url}/{directory}')

        return parsed_directories

    # Clean duplicates from a list of lists
    # Mainly for optimization
    def clean(lists):
        new_lists = []
        occuring = []
        for list_ in lists:
            new_list = []
            for elem in list_:
                if elem in occuring:
                    continue
                else:
                    occuring.append(elem)
                    new_list.append(elem)

            new_lists.append(new_list)

        for list_ in new_lists:
            if list_ == []:
                new_lists.pop(new_lists.index(list_))
        return new_lists
    
    # Remove slash from the end and start, if there is one
    def remove_slash(url):
        if url[len(url) - 1] == '/':
            url = url[:-1]
        if url[0] == '/':
            url = url[1:]


        return url


## Table Utilities
## For table manipulation
class TableUtil:

    # Clean the table
    # Used by Utility.parse_table. You dont gotta do it yourself
    def clean_table(table):
        new_table = {}
        for name, parameters in table.items():
            new_params = {}
            # remove parameters with np.int64 as the key value
            for param,value in parameters.items():
                if isinstance(param, np.int64):
                    continue
                else:
                    new_params[param] = value

            if not isinstance(name, np.int64):
                new_table[name] = new_params

        return new_table

    # Is the table relevant?
    # Only pass a DataFrame table in the `table` argument
    def is_relevant(table: pd.DataFrame):
        try:
            keywords = ["name", "grade", "title", "job", "city", "email", "position"]
            relevant = False
            for index, row in table.iterrows():
                for keyword in keywords:
                    if keyword in row.to_string().lower():
                        relevant = True

            return relevant
        except Exception as e:
            print(f"Utility.is_relevant: {e}")

    # Turn a Table (HTML) into a pd.DataFrame
    def table_into_df(table):
        return pd.read_html(StringIO(str(table)))[0]



## Parsing and Processing Data
## That is received
class Parser:

    def parse_table(table):
        """
        Parse a DataFrame table into a dictionary, making it easier to use and store.
        Also cleans the parsed table through Utility.clean_table

        Returns:
            A dict with the format:
                full name : [parameters]
                e.g.
                    'Wilson, Ray': {'Job Title': 'Principal'}
        """

        table = table.reset_index()
        table = table.dropna(how="all") # drop rows with all NaN values

        data = {} # parsed table

        table.columns = table.iloc[0]
        table = table[1:]

        """
        # look for name row
        keywords = ["Name", "Staff Name", "Staff", "Contact", "Contact Name"]
        row_name = ''
        for index, row in table.iterrows():
            for ro in row:
                print(f"ro {ro}")
                if isinstance(ro, str):
                    if ro.lower().startswith("name") or ro.lower().startswith("staff"):
                        row_name = str(ro)

        for index, row in table.iterrows():
            name = row[row_name]
            data[name] = {col: row[col] for col in table.columns if col != row_name}
        """
        data = table.to_dict()
        data = TableUtil.clean_table(data)
        # data = {'Staff Name': {1: etc, 2:etc}, etc}
        # we must restructure into appropriate return dict

        new_data = {}
        name_key = list(data.keys())[0]

        for pos, name in data[name_key].items():
            # name = elizabeth shaddix
            # key = pos
            # now go thru data and assign values
            values_to_assign = {}
            for p_key, p_values in data.items():
                if p_key == name_key:
                    pass
                else:
                    k_to_assign = p_key
                    v_to_assign = p_values[pos]
                    values_to_assign[k_to_assign] = v_to_assign

            new_data[name] = values_to_assign

        return new_data

    def process_implications(table, id):
        """
        Process 'implications' of parameters.
        Basically adds those parameters to the table that weren't available on the school website itself.

        Only pass a parsed and processed `table`.
        Or just use `Utility.process_into_parameters`, which does everything automatically.

        `id` is the location of the Excel file from where from where the table has been extracted.
        Parameters this method adds to each entry:
            School District
            State
            School Name
        """

        if not table:
            return
        ## Fetch relevant data
        # Fetch district
        districts = Utility.get_from_excel("district_name")
        district = districts[id]

        # Fetch school name from district url
        school_url = Utility.get_from_excel("district_url")[id]
        resp = requests.get(school_url)
        temp = BeautifulSoup(resp.content, 'html.parser')

        school_name = temp.find(id="firstHeading").string
        ## Append that data
        for entry in table:
            entry["School District"] = district
            entry["School Name"] = school_name

            # also add filler/common parameters
            entry["State"] = "Alabama"


        return table

    def process_into_parameters(table, id):
        """
        Processes a parsed table into the set parameters.

        Parameters:
            State
            City
            School District
            School Name
            School Phone
            Grade Level
            Department
            Last Name
            First Name
            Honorific
            Email Address

        Parameters not found have `None` as the value.

        Returns:
            A list containing dicts, of which each dict represents ONE individual.
            For example:
                [ {'first name': etc, 'state': etc } ]
        """

        data = [] # processed table

        for name, values in table.items():
            processed_values = {}
            # replace values
            for key, value in values.items():
                bundle = Utility.replace_into_params(key, value)
                new_key, new_value = bundle[0], bundle[1]
                processed_values[new_key] = new_value

            # replace name
            processed_name = Utility.replace_name(name)
            first_name, last_name = processed_name[0], processed_name[1]

            # finally, collect and then append the processed individual entry to `data`
            entry = {}
            entry["First Name"] = first_name
            entry["Last Name"] = last_name
            for processed_k, processed_v in processed_values.items():
                entry[processed_k] = processed_v



            data.append(entry)

        # Add implicational parameters
        data = Parser.process_implications(data, id)

        return data


## Methods to extract data from a staff directory page
class Extractor:

    # Iframe method
    # Searches for tables in the form of iframes in page, then process and return as list of DataFrames
    def extr_iframe(soup) -> list:
        tables = []
        # get iframe srcs
        # one page can have multiple iframe srcs and therefore multiple tables
        iframes = soup.find_all('iframe')
        srcs = [] # -> urls
        for iframe in iframes:
            srcs.append(iframe['src'])

        # process srcs by opening them, turning them into dataframe tables thru pandas
        # and then append them to the `tables` variable
        for src in srcs:
            temp_logic = False
            for blocked in ["googletag", "youtube", "canva"]:
                if blocked in src:
                    print(f"{colors.WARNING}ignoring {src} {colors.ENDC}")
                    temp_logic = True
            if temp_logic:
                continue

            html = Utility.get_selenium(src)

            data = BeautifulSoup(html, "html.parser")
            tables_inpage = data.find_all('table')

            for table in tables_inpage:
                df = TableUtil.table_into_df(table)
                tables.append(df)

        return tables
    
    # Instead of finding tables in iframe links, find them directly on the webpage.
    def extr_table(soup) -> list:
        tables_inpage = soup.find_all('table')
        tables = []
        for table in tables_inpage:
            df = TableUtil.table_into_df(table)
            tables.append(df)

        return tables

    def extr_match(url):
        # this is a list of pattern specifications. The "match" key is an XPath
        # expression that identifies a top-level element that contains both the name
        # and email address. The "name" and "email" keys are callables that when
        # evaluated on the matched element return the desired data.
        patterns = [
            {
                "match": "//tr[(td/strong) and (td/a[contains(@href, 'mailto:')])]",
                "name": lambda ele: ele.find_element(By.XPATH, "td/strong").get_attribute(
                    "textContent"
                ),
                "email": lambda ele: ele.find_element(By.XPATH, "td/a").get_attribute("href"),
            },

            {
                "match": "//tr[td/table//a[contains(@href, 'mailto:')]]",
                "name": lambda ele: ele.find_element_by_xpath("td//strong").get_attribute(
                    "textContent"
                ),
                "email": lambda ele: ele.find_element_by_xpath("td//td/a").get_attribute(
                    "href"
                ),
            },

            {
                "match": "//div[span/a[contains(@href, 'mailto')]]",
                "name": lambda ele: ele.find_element_by_xpath(
                    "./preceding-sibling::div[1]/span"
                ).get_attribute("textContent"),
                "email": lambda ele: ele.find_element_by_xpath("span/a").get_attribute("href"),
            },
        ]

        driver = Utility.get_selenium_raw()
        driver.get(url)
        all_contacts = {}

        matches = driver.find_elements(By.XPATH, '//a[contains(@href, "mailto:")]')
        all_emails = set(x.get_attribute("href") for x in matches)
        print(f"expecting {len(all_emails)} contacts")

        for patternSpec in patterns:
            matches = driver.find_elements(By.XPATH, patternSpec["match"])
            for match in matches:
                try:
                    c_name = patternSpec["name"](match)
                    c_email = patternSpec["email"](match)
                except selenium.common.exceptions.NoSuchElementException:
                    # if we fail to process something, just skip it and move on
                    continue

                all_contacts[c_name] = c_email

        print(f"all contacts: {all_contacts}")
        print("missing:", all_emails.difference(all_contacts.values()))
    
    # Using NER to extract data
    def extr_ner(nlp, soup):
        print("entered ner")
        text = soup.get_text()

        sents = nlp(text)
        names = []
        emails = []

        temp = sents.ents
        for e in temp:
            print(e.label)
        



## SQL DB Management Class
class DBManager:
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "faculty_data"
    }

    # Connect to db
    def connect():
        return pymysql.connect(**DBManager.db_config)
    
    # Generate table name
    # Filler function (yet). Added for programmer's ease
    def generate_table_name(url):
        return str(url)
    
    # Dynamically create a table
    def create_table(connection, table_name, columns):
        with connection.cursor() as cursor:
            # Dynamically construct the CREATE TABLE query
            column_definitions = ", ".join([f"`{col}` TEXT" for col in columns])
            sql_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_definitions});"
            cursor.execute(sql_query)
    
    # Insert faculty data
    def insert_data(connection, table_name, data):
        with connection.cursor() as cursor:
            for entry_list in data:
                for entry in entry_list:
                    columns = ", ".join([f"`{col}`" for col in entry.keys()])
                    placeholders = ", ".join(["%s"] * len(entry))
                    sql_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders});"
                    cursor.execute(sql_query, list(entry.values()))


    # Wrapper
    def process_data(url, data):
        connection = DBManager.connect()
        if not data:
            return
        
        table_name = DBManager.generate_table_name(url)
        columns = data[0][0].keys()

        DBManager.create_table(connection, table_name, columns)
        DBManager.insert_data(connection, table_name, data)

        connection.commit()
        connection.close()



    

    
## Scrapper class
## Interface
class Scrapper:
    def __init__(self):

        try:
            self.nlp = spacy.load("en_core_web_md")
        except OSError:
            print('Downloading language model for the spaCy POS tagger\n'
                "(don't worry, this will only happen once)")
            download('en_core_web_md')
            self.nlp = spacy.load('en_core_web_md')

        self.parsed = []

    ## Scrapper Util
    # data handler
    def handle_data(self, url, data: list):
        """
        `data` should be a list containing tables.
        Make sure the `payload` from `scrape` is being passed. 
        """
        # pass to DBManager
        DBManager.process_data(url, data)


    # save pos to config.json 
    def save_pos(self, con_dict, pos):
        with open("config.json", "w") as f:
            con_dict["cache"] = pos
            f.write(json.dumps(con_dict))

    # save error in errors.json
    def save_error(self, pos, error):
        with open("errors.json", "r") as f:
            error_dict = json.loads(f.read())

        with open("errors.json", "w"):
            error_dict[str(pos)] = str(error)
            f.write(json.dumps(error_dict))

    ## Finders


    def find_subwebsites(self, soup) -> list:
        """
        Method for finding subwebsites from a school homepage.
        As of now we cant detect whether the website is actually a sub-school website
        or irrelevant.
        But self.find_directories does a pretty okay job at filtering that.

        Returns:
            A list of subwebsite URLs.
        """
        links = []
        for a in soup.find_all('a', href=True):
            links.append(a['href'])

        # eliminate blubber
        subwebsites = []
        for link in links:
            if link.startswith("http") and link.count('/') <= 3:
                if not link in subwebsites: # to avoid adding duplicates
                    subwebsites.append(link)

        return subwebsites
    

    def find_directories(self, soup) -> list:
        """
        Method for finding staff directories from a school website
        Not that accurate since it only operates on a list of keywords, but it does the job.

        Returns:
            A list of strings containing directories, but not full URLs
        """
        keywords = ['staff', 'faculty', 'teachers']
        links = soup.find_all('a', href=True)

        directories = []
        for link in links:
            halflink = link['href']
            for keyword in keywords:
                if keyword in halflink:
                    directories.append(halflink)
                    break

        return list(set(directories))
    
    def find_staff(self, soup, check=True) -> list:
        """
        Method for finding staff data from a page
        Pass a staff directory soup in the `soup` argument.
        `check` is for checking whether the tables are relevant, through `TableUtils.is_relevant`

        Uses a variety of methods from `Extractor` class to extract relevant data.

        Returns:
            Data is returned in a list of DataFrame objects (tables).
        """
        data = []

        # iframe method
        data = Extractor.extr_iframe(soup)
        if not data: # Extractor.extr_iframe didnt work? no worries
            data = Extractor.extr_table(soup)

        # filter `data` tables with TableUtil.is_relevant
        if check:
            new_data = []
            for table in data:
                if TableUtil.is_relevant(table):
                    new_data.append(table)

            data = new_data


        return data

    def scrape(self, url, pos, **kwargs):
        """
        Wrapper over the Scrapper class methods.
        Scrapes a single school website and multiple sub-websites and directories within it (if any).

        The `id` parameter is the place of the url in the excel file, starting from 1.

        Returns:
            List with scrapped data in dictionary form.

        """
        # Handle kwargs
        silent = kwargs.get("silent", False) # for errors/warnings/data info
        log_info = kwargs.get("log_info", True) # for info

        print(f"{colors.HEADER}{colors.UNDERLINE}SCRAPPING{colors.ENDC}:{colors.HEADER} {url} {colors.ENDC}")

        # Collect subwebsites
        try:
            soup = Utility.get_soup(url)
            subwebsites = self.find_subwebsites(soup)
        except Exception as e:
            print(f'{colors.FAIL}Scrapper: Couldnt scrap {url} :\n{e}\n{colors.ENDC}')
            return
        
        # Finding subwebsite directories
        # aka collecting faculty/staff directories in the subwebsites
        directories = []
        for subwebsite in subwebsites:
            try:
                subw_soup = Utility.get_soup(subwebsite)
                print(f"{colors.OKBLUE}finding staff directories in: {subwebsite} {colors.ENDC}")

                halflinks = self.find_directories(subw_soup) # halflinks of directories
                subw_directories = Utility.directories_to_urls(subwebsite, halflinks) # turn halflinks into urls

                if log_info:
                    if not halflinks:
                        print(f'{colors.WARNING}no directories{colors.ENDC}')

                for d in subw_directories:
                    directories.append(d)

            except Exception as e:
                print(f'{colors.FAIL}Scrapper: Couldnt find directories in subwebsite {subwebsite}\nReason: {e} {colors.ENDC}')

        if directories:
            print(f"\n{colors.OKGREEN}Collected directories, now scrapping them\nTotal dirs found: {len(directories)}\n{colors.ENDC}")

        # Get staff/faculty data from directories
        # then append to payload
        payload = [] # return

        try:
            for d_url in directories:
                d_soup = Utility.get_soup(d_url)
                print(f"{colors.OKBLUE}scrapping directory: {d_url} {colors.ENDC}")
                
                dataframes = self.find_staff(d_soup)

                for df in dataframes:

                    parsed = Parser.parse_table(df)
                    processed = Parser.process_into_parameters(parsed, pos)
                    payload.append(processed)


            print(f"{colors.HEADER}Tables scrapped:{len(payload)}")
        except Exception as e:
            if not directories:
                return
            
            print(f"{colors.FAIL}Couldnt scrape directory: {d_url}\nReason: {traceback.format_exc()} {colors.ENDC}")
            # log to errors.json
            self.save_error(pos, e)

        # finally we return the payload
        return payload



    def scrapes(self, **kwargs):
        """
        Mainloop wrapper over self.scrapper()
        Takes a list of urls and iterates over them to scrap

        `save` is for saving the result to an external file or db
        """
        save = kwargs.get("save", True)

        # start from where it left
        with open("config.json", "r") as f:
            con_dict = json.loads(f.read())


        left_at = con_dict["cache"] # pos of url that we left at
        pos = 1 # pos of url currently iterating on

        for url in urls:
            if pos >= left_at:
                result = self.scrape(url, pos, save=save) # -> payload

                # pass to handler if save=True and result is available
                if save and result:
                        self.handle_data(url, result)

                # save pos to remember where to start from next time
                self.save_pos(con_dict, pos)

            else:
                pass

            # run at all times
            pos += 1


    
    def concurrent_scrapes(self, **kwargs):
        """
        Experimental concurrent alternative to Scrapper.scrapes
        -- FIX --
        """
        save = kwargs.get("save", False)

        # get where it left
        with open("config.json", "r") as f:
            con_dict = json.loads(f.read())

        left_at = con_dict["cache"] # pos of url that we left at
        pos = 1 # pos of url currently iterating on

        futures = []
        for url in urls:
            if pos >= left_at:
                with ProcessPoolExecutor(max_workers=20) as executor:
                    future = executor.submit(self.scrape, url, pos)
                    futures.append(future)

                    self.save_pos(con_dict, pos)

            else:
                pass

            if len(futures) % 5 == 0:
                results = [future.result() for future in futures]
                print(f"results = {results}")        
            pos += 1

### Main
## Console Interface
def console():
    scrapper = Scrapper()
    save = True # True for default, False for debug mode
    concurrent = False

    msg = f"{colors.OKCYAN}webscrapper Console\n{colors.UNDERLINE}COMMANDS{colors.ENDC}{colors.OKCYAN}:\n1) start : start the webscrapper\n2) reset pos : reset count and start from pos 1\n3) reset data : reset data.json\n4) mode -[debug, default] : debug mode turns off save\n5) concurrent -[on, off]{colors.ENDC}"
    print(msg)

    def update_pos():
        with open("config.json", "r") as f:
            return int(json.loads(f.read())["cache"])
    
    while True: # command loop
        com = input(">>>").lower() # command

        pos = update_pos()

        if com == "start":
            pos = update_pos()
            print(f"Webscrapper starting from pos {pos}")
            conc_human = "off"
            if concurrent:
                conc_human = "on"
            print(f"Concurrency: {conc_human}")

            if concurrent:
                scrapper.concurrent_scrapes(save=save)
            else:
                scrapper.scrapes(save=save)

        elif com == "reset pos":
            pos = update_pos()

            with open("config.json", "w") as f:
                f.write(json.dumps({"cache": 1}))

            print(f"Success. Previous pos: {pos}")

        elif com == "reset data":
            with open("data.json", "w") as f:
                print("You are about to erase all contents in data.json. Are you sure? [y/n]")
                confirm = input(">>>").lower()
                if confirm == "y":
                    f.write(json.dumps({}))
                    print("Success.")
                else:
                    print("Exited.")

        elif com == "mode":
            print(f"default/`save`: {save}")
        elif com == "mode -debug":
            save = False
        elif com == "mode -default":
            save = True

        elif com == "concurrent":
            print(f"concurrent: {concurrent}")
        elif com == "concurrent -on":
            concurrent = True
            save = False
        elif com == "concurrent -off":
            concurrent = False
            save = True
        else:
            print(f"{colors.FAIL}Unknown command '{com}'{colors.ENDC}")


if __name__ == "__main__":
    scrapper = Scrapper()
    scrapper.scrapes()



"""
todo
NER method
"""
