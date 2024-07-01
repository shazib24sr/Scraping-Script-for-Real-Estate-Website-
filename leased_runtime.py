import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import re
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import os

# User agents and proxies
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
]

PROXIES = [
    "http://51.158.68.68:8811",
    "http://185.199.84.161:53281",
    "http://185.199.86.83:3128",
]

# Function to create a session with retry strategy
def create_session(retries=3, backoff_factor=0.3):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

session = create_session()

# Function to initialize Selenium WebDriver
def initialize_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

# Function to get HTML content from a URL
def get_html_content(url):
    try:
        response = session.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

# Function to extract full phone number using Selenium
def get_full_phone_number(driver, url):
    try:
        driver.get(url)
        
        # Wait for the phone number link to be present and clickable
        wait = WebDriverWait(driver, 10)
        phone_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.AgentDetails_button_1SE0f')))
        
        # Scroll the element into view
        driver.execute_script("arguments[0].scrollIntoView(true);", phone_link)
        
        # Use JavaScript to click the element
        driver.execute_script("arguments[0].click();", phone_link)
        
        # Wait for the phone number to be visible
        phone_number = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a.AgentDetails_button_1SE0f span')))
        full_phone_number = phone_number.text.strip()
        return full_phone_number
    except Exception as e:
        print(f"Error fetching full phone number for {url}: {e}")
        return None

# Function to extract property data from a URL
def extract_property_data(property_url, driver):
    html_content = get_html_content(property_url)
    if not html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    def get_text(selector):
        element = soup.select_one(selector)
        return element.text.strip() if element else None
    
    sub_title_parts = [
        get_text('.Price_priceLabel_18amG'),
        get_text('.PriceGroup_priceGroup_2W4BV')
    ]
    sub_title = ' - '.join(filter(None, sub_title_parts))
    address = get_text('h1.Address_container_3HZgj span')
    state, postal_code = None, None
    if address:
        match = re.search(r'(.*), ([A-Z]{2,3}) (\d{4})', address)
        if match:
            address = match.group(1).strip()
            state = match.group(2).strip()
            postal_code = match.group(3).strip()

    last_updated = None
    span_tags = soup.find_all('span', class_='IdAndLastUpdated_text_1pK5I')
    for span in span_tags:
        text = span.text.strip()
        if text.startswith("Last Updated:"):
            last_updated = text.replace("Last Updated:", "").strip()
            break

    attribute_labels = {
        'Land area': 'land_area',
        'Property extent': 'property_extent',
        'Lease terms': 'lease_terms',
        'Lease expiry': 'lease_expiry',
        'Parking info': 'parking',
        'Zoning': 'zoning',
        'Municipality': 'municipality',
        'NABERS' : 'nabers',
        'Car spaces':'car_space',
        'Parking info':'parking_info',
    }
    attributes = {k: None for k in attribute_labels.values()}
    attribute_divs = soup.find_all('div', class_='Attribute_attribute_3lq_3')

    for attribute_div in attribute_divs:
        label_tag = attribute_div.find('p', class_='Attribute_label_1bYjg')
        value_tag = attribute_div.find('p', class_='Attribute_value_i8Dee')

        if label_tag and value_tag:
            label_text = label_tag.text.strip()
            value_text = value_tag.text.strip()
            if label_text in attribute_labels:
                attributes[attribute_labels[label_text]] = value_text

    agency_data = {}

    agency_panels = soup.find_all('div', class_='AgencyPanel_agencyDetails_2LqtQ')

    for agency_panel in agency_panels:
        # Extract agency name
        agency_names = agency_panel.find_all('a', class_='AgencyPanel_agencyNameLink_nCd-h')
        agent_list = agency_panel.find_all('li', class_='AgentDetails_container_2xMTV')
        
        for agency_name in agency_names:
            ag = agency_name.text.strip()
            # Initialize an empty list to store agent details
            agency_data[ag] = []
        
        for agent in agent_list:
            agent_name = agent.find('h4', class_='AgentDetails_name_23QWU').text.strip()
            agent_phone_partial = agent.find('span').text.strip()

            # Extract the full phone number using Selenium
            full_phone_number = get_full_phone_number(driver, property_url)

            # Append agent details as a dictionary to the list associated with the agency
            agency_data[ag].append({
                'name': agent_name,
                'phone': full_phone_number
            })

    highlights_list = soup.find_all('li', class_='PrimaryDetailsBottom_highlight_1U_wa')
    highlights = ' | '.join([item.text.strip() for item in highlights_list])

    data = {
        'link': property_url,
        'address': address,
        'state': state,
        'postal_code': postal_code,
        'sub_title': sub_title,
        'property_type': get_text('.PrimaryDetailsTop_propertyTypes_1mGFK'),
        'property_id': get_text('span.IdAndLastUpdated_text_1pK5I'),
        'price': get_text('h2.PriceBar_heading_2z-88'),
        'property_last_updated': last_updated,
        'days_active': None,
        'highlights_of_property': highlights,
        'floor_area': get_text('.Attribute_value_i8Dee'),
        'car_spaces': attributes['car_space'],
        'parking_info': attributes['parking_info'],
        'zoning': attributes['zoning'],
        'nabers': attributes['nabers'],
        'land_area': attributes['land_area'],
        'property_extent': attributes['property_extent'],
        'Municipality': attributes['municipality'],
    }

    agency_counter = 1
    for agency_name, agents in agency_data.items():
        data[f'agency{agency_counter}name'] = agency_name
        for i, agent in enumerate(agents, start=1):
            if i > 4:
                break  # Only include up to 4 agents
            data[f'agency{agency_counter}salespeople{i}name'] = agent['name']
            data[f'agency{agency_counter}salespeople{i}number'] = agent['phone']
        agency_counter += 1

    return data

# Function to scrape properties incrementally
def scrape_properties_incremental(main_url, output_csv):
    base_url = 'https://www.realcommercial.com.au'
    counter = 0
    current_page = main_url
    columns = [
        'link', 'address', 'state', 'postal_code', 'sub_title', 'property_type', 'property_id',
        'price', 'property_last_updated', 'days_active', 'highlights_of_property', 'floor_area',
        'car_spaces', 'parking_info', 'zoning', 'nabers', 'land_area', 'property_extent',
        'Municipality', 'agency1name', 'agency1salespeople1name', 'agency1salespeople1number',
        'agency1salespeople2name', 'agency1salespeople2number', 'agency1salespeople3name',
        'agency1salespeople3number', 'agency1salespeople4name', 'agency1salespeople4number',
        'agency2name', 'agency2salespeople1name', 'agency2salespeople1number',
        'agency2salespeople2name', 'agency2salespeople2number', 'agency2salespeople3name',
        'agency2salespeople3number', 'agency2salespeople4name', 'agency2salespeople4number',
        'agency3name', 'agency3salespeople1name', 'agency3salespeople1number',
        'agency3salespeople2name', 'agency3salespeople2number', 'agency3salespeople3name',
        'agency3salespeople3number', 'agency3salespeople4name', 'agency3salespeople4number',
        'agency4name', 'agency4salespeople1name', 'agency4salespeople1number',
        'agency4salespeople2name', 'agency4salespeople2number', 'agency4salespeople3name',
        'agency4salespeople3number', 'agency4salespeople4name', 'agency4salespeople4number',
    ]

    # Load previously processed URLs from CSV if exists
    processed_urls = set()
    if os.path.exists(output_csv):
        existing_data = pd.read_csv(output_csv)
        processed_urls.update(existing_data['link'].unique())

    df = pd.DataFrame(columns=columns)
    driver = initialize_driver()

    def process_property_page(url):
        nonlocal counter
        nonlocal df  # Ensure df is accessible from the enclosing scope
        html_content = get_html_content(url)
        if not html_content:
            return None
        
        soup = BeautifulSoup(html_content, 'html.parser')

        for a in soup.select('a.Address_link_1aaSW'):
            link = urljoin(base_url, a.get('href'))
            if link in processed_urls:
                print(f"Skipping already processed link: {link}")
                continue

            counter += 1
            print(f"Links Found: {counter}")

            try:
                # Check if the link is already in the DataFrame
                if not df[df['link'] == link].empty:
                    print(f"Skipping duplicate link in DataFrame: {link}")
                    processed_urls.add(link)  # Add to processed_urls to skip in future
                    continue

                data = extract_property_data(link, driver)
                if data:
                    df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
                    df.to_csv(output_csv, index=False)
                    print(f"Data from {link} saved to {output_csv}")
                    processed_urls.add(link)  # Add to processed_urls after successful processing
                    print("-------")
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                print(f"Error extracting data from {link}: {e}")

        next_page_elem = soup.select_one('.ArrowLinkButton__StyledLinkButton-sc-hsclgu-0.kXfeNe[rel="next"]')
        if next_page_elem:
            next_page = urljoin(base_url, next_page_elem.get('href'))
            return next_page
        return None

    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            while current_page:
                next_page = executor.submit(process_property_page, current_page).result()
                current_page = next_page

    finally:
        driver.quit()
        print("WebDriver closed successfully.")

    print(f"Total links Found: {counter} property links")

# Entry point to start scraping
if __name__ == "__main__":
    main_url = 'https://www.realcommercial.com.au/leased/vic/?includePropertiesWithin=includesurrounding'
    output_csv = 'property_listings_leased.csv'
    scrape_properties_incremental(main_url, output_csv)


    



   