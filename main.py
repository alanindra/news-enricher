import pandas as pd
import requests
from bs4 import BeautifulSoup
import re # library for regex
from urllib.parse import urlparse
import datetime
import time
from tqdm import tqdm
import os
import concurrent.futures

def get_protocol_affix(page_link):
    # If the URL already starts with a protocol, assume it's correct.
    if page_link.startswith("http://") or page_link.startswith("https://"):
        return page_link

    https_url = "https://" + page_link
    try:
        response = requests.head(https_url, timeout=5)
        # Consider a status code less than 400 as valid.
        if response.status_code < 400:
            return https_url
    except Exception:
        pass

    http_url = "http://" + page_link
    try:
        response = requests.head(http_url, timeout=5)
        if response.status_code < 400:
            return http_url
    except Exception:
        pass

    return None

def init_soup(page_link):
    response = requests.get(page_link)
    soup = BeautifulSoup(response.text, 'lxml')

    return soup

def get_content(page_link):
    try:
        soup = init_soup(page_link)
        paragraphs = [
            p.get_text(separator=" ", strip=True)
            for p in soup.find_all('p')
            if p.get_text(strip=True)
        ]
        
        content = "".join(paragraphs).replace("\n", " ")
        
        return content
    except (AttributeError, requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
        #tqdm.write(f"Error processing URL '{page_link}': {e}")
        return None

def get_title(page_link):
    try:
        soup = init_soup(page_link)
        
        title = soup.title.string
        
        if title:
            title = " ".join(title.split()).replace("\n", " ") #remove whitespaces and line breaks
            title = re.sub(r'\s*-\s*.*$', '', title)
            return title
        else:
            title = None
            return title
    except (AttributeError, requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
        #tqdm.write(f"Error processing URL '{page_link}': {e}")
        return None

def get_date(page_link):
    try:
        soup = init_soup(page_link)

        date = soup.find(attrs={'class': re.compile(r'.*date|calendar|published.*', re.IGNORECASE)})
        if date:
            raw_date = date.get_text(strip=True)
            raw_date = re.sub(r'\s*-\s*.*$', '', raw_date)
            raw_date = re.sub(r'.*?(\d{2}\s+[A-Za-z]{3}\s+\d{4}).*', r'\1', raw_date)
            return raw_date
        else:
            return None
    except (AttributeError, requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
        #tqdm.write(f"Error processing URL '{page_link}': {e}")
        return None
    
def get_media_name(page_link):
    try:
        # Ensure the URL is a string.
        if not isinstance(page_link, str):
            return None
        
        # If the URL doesn't start with a protocol, prepend "http://"
        if not (page_link.startswith("http://") or page_link.startswith("https://")):
            page_link = "http://" + page_link
        
        # Parse the URL to extract the domain.
        domain = urlparse(page_link).netloc
        
        # Remove "www." if it exists.
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except (AttributeError, requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
        #tqdm.write(f"Error processing URL '{page_link}': {e}")
        return None
    
def get_journalist_name(page_link):
    try:
        soup = init_soup(page_link)
        
        # Define a list of attribute dictionaries to check
        meta_checks = [
            {'name': 'author'},
            {'property': 'article:author'},
            {'property': 'content:author'}
        ]
        
        for attrs in meta_checks:
            meta_tag = soup.find('meta', attrs=attrs)
            if meta_tag and meta_tag.get('content'):
                journalist_name = meta_tag['content'].strip()
                return(journalist_name)
    
    except (AttributeError, requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
        #tqdm.write(f"Error processing URL '{page_link}': {e}")
        return None
    

# --- Functions to process each column --- #
def process_content(df):
    tqdm.pandas(desc="Getting article content", colour="green")
    return df['page_link'].progress_apply(lambda x: get_content(get_protocol_affix(x)))

def process_title(df):
    tqdm.pandas(desc="Getting article title", colour="green")
    return df['page_link'].progress_apply(lambda x: get_title(get_protocol_affix(x)))

def process_date(df):
    tqdm.pandas(desc="Getting article date", colour="green")
    return df['page_link'].progress_apply(lambda x: get_date(get_protocol_affix(x)))

def process_media_name(df):
    tqdm.pandas(desc="Getting article media name", colour="green")
    return df['page_link'].progress_apply(lambda x: get_media_name(get_protocol_affix(x)))

def process_journalist_name(df):
    tqdm.pandas(desc="Getting article journalist name", colour="green")
    return df['page_link'].progress_apply(lambda x: get_journalist_name(get_protocol_affix(x)))

# --- Main Program --- #
if __name__ == "__main__":
    time_start = time.time()
    
    # Register tqdm for pandas
    tqdm.pandas()

    path = "/Users/qaulanmaruf/Desktop/news_enrichment/input"
    folder = os.listdir(path)
    
    df_list = []
    for file in folder:
        if file.endswith(".csv"):
            df_list.append(pd.read_csv(os.path.join(path, file)))
    
    df = pd.concat(df_list, ignore_index=True)
    
    # Use a ThreadPoolExecutor to run each processing function concurrently.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        print("\nEnriching news articles...\n")
        future_content = executor.submit(process_content, df)
        future_title = executor.submit(process_title, df)
        future_date = executor.submit(process_date, df)
        future_media = executor.submit(process_media_name, df)
        future_journalist = executor.submit(process_journalist_name, df)
        
        # Wait for each to complete and assign to new columns.
        df['content'] = future_content.result()
        df['title'] = future_title.result()
        df['date'] = future_date.result()
        df['media_name'] = future_media.result()
        df['journalist_name'] = future_journalist.result()
    
    # Save the updated DataFrame.
    output_path = f"/Users/qaulanmaruf/Desktop/news_enrichment/output/enriched_data_{time.time()}.csv"
    df.to_csv(output_path, index=False)
    
    print("\n\nEnrichment complete. Enriched news article file saved to output folder.")

    elapsed = time.time() - time_start
    minutes, seconds = divmod(elapsed, 60)
    print("\nTime elapsed: %d minutes and %.2f seconds" % (minutes, seconds))
    print("No. of news articles: ", df.shape[0], "\n")
