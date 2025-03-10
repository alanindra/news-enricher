import pandas as pd
import requests
from bs4 import BeautifulSoup
import re # library for regex
import time
from tqdm import tqdm
import os
import concurrent.futures
import logging


# ---initializing functions---
#initialize log file
def setup_logging():
    log_folder = "logs"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    timestamp = time.strftime("%d-%m-%Y-%H.%M.%S", time.localtime(time.time()))
    log_file_path = f"{log_folder}/enrichment_logs_{timestamp}.log"
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging setup complete.")

#initialize beautiful soup that handles errors
def init_soup(page_link): 
    for attempt in range(3):
        try:
            response = requests.get(page_link, timeout = 6)
            soup = BeautifulSoup(response.text, 'lxml')
            return soup
        except (AttributeError, 
                requests.exceptions.ChunkedEncodingError, 
                requests.exceptions.ConnectionError, 
                requests.exceptions.SSLError,
                requests.exceptions.ReadTimeout) as e:
            logging.error(f"Error processing URL '{page_link}': {e}")
            time.sleep(1)
    
    return None

# ---main functions---
# enrich content
def enrich_content(df):
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
        except (AttributeError, 
                requests.exceptions.MissingSchema, 
                requests.exceptions.ConnectionError, 
                requests.exceptions.SSLError) as e:
            logging.error(f"Error processing URL '{page_link}': {e}")
            return None
    tqdm.pandas(desc="Getting article content", colour="green")
    return df['page_link'].progress_apply(get_content)

# enrich title
def enrich_title(df):
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
        except (AttributeError, 
                requests.exceptions.MissingSchema, 
                requests.exceptions.ConnectionError, 
                requests.exceptions.SSLError) as e:
            logging.error(f"Error processing URL '{page_link}': {e}")
            return None
    tqdm.pandas(desc="Getting article title", colour="green")
    return df['page_link'].progress_apply(get_title)

# enrich date
def enrich_date(df):
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
        except (AttributeError, 
                requests.exceptions.MissingSchema, 
                requests.exceptions.ConnectionError, 
                requests.exceptions.SSLError) as e:
            logging.error(f"Error processing URL '{page_link}': {e}")
            return None 
    tqdm.pandas(desc="Getting article date", colour="green")
    return df['page_link'].progress_apply(get_date)

# enrich media name
def enrich_media_name(df):
    pattern = r'^(?:https?://)?([^/]+)'
    tqdm.pandas(desc="Getting article media name", colour="green")
    return df['page_link'].progress_apply(
        lambda page_link: re.match(pattern, page_link).group(1) if re.match(pattern, page_link) else None
    )

# enrich journalist name
def enrich_journalist_name(df):
    def get_journalist_name(page_link):
        try:
            soup = init_soup(page_link)
            
            # list of attribute dictionaries to check
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
        
        except (AttributeError, 
                requests.exceptions.MissingSchema, 
                requests.exceptions.ConnectionError, 
                requests.exceptions.SSLError) as e:
            logging.error(f"Error processing URL '{page_link}': {e}")
            return None
    tqdm.pandas(desc="Getting article journalist name", colour="green")
    return df['page_link'].progress_apply(get_journalist_name)

# ---main program---
if __name__ == "__main__":
    time_start = time.time() #determine the current time
    setup_logging() #set up log file
    tqdm.pandas() #register to tqdm

    input_path = "/Users/qaulanmaruf/Desktop/news_enrichment/input"
    folder = os.listdir(input_path)
    
    df_list = []
    for file in folder:
        if file.endswith(".csv"):
            df_list.append(pd.read_csv(os.path.join(input_path, file)))
    
    df = pd.concat(df_list, ignore_index=True)
    
    # run each processing function concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        print("\nEnriching news articles...\n")
        future_content = executor.submit(enrich_content, df)
        future_title = executor.submit(enrich_title, df)
        future_date = executor.submit(enrich_date, df)
        future_media = executor.submit(enrich_media_name, df)
        future_journalist = executor.submit(enrich_journalist_name, df)
        
        # wait for each to complete and assign to new columns
        df['content'] = future_content.result()
        df['title'] = future_title.result()
        df['date'] = future_date.result()
        df['media_name'] = future_media.result()
        df['journalist_name'] = future_journalist.result()
    
    # save the updated df
    output_path = "/Users/qaulanmaruf/Desktop/news_enrichment/output/"
    timestamp = time.strftime("%d-%m-%Y-%H.%M.%S", time.localtime(time.time()))
    df_output_name = f"{output_path}_{timestamp}.csv"
    df.to_csv(output_path, index=False)

    #statistics on elapsed time
    elapsed = time.time() - time_start
    minutes, seconds = divmod(elapsed, 60)

    #update logging info
    logging.info((df[["title", "content"]]))
    logging.info("Enrichment complete. Enriched news article file saved to output folder.")
    logging.info(f"Time elapsed: {int(minutes)} minutes and {seconds:.2f} seconds")
    logging.info(f"No. of news articles: {df.shape[0]}")
    
    #print statements in terminal
    print((df[["title", "content"]]))
    print("\n\nEnrichment complete. Enriched news article file saved to output folder.")
    print("\nTime elapsed: %d minutes and %.2f seconds" % (minutes, seconds))
    print("No. of news articles: ", df.shape[0], "\n")
