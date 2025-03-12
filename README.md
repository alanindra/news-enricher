# news-enrichment
This program takes in csv file as an input, crawls to list of news article links, and outputs csv file with information of the title, content, date published, journalist name, and proper person names of a news article. This program requires key to Google NLP API.

The program is intended to automate media monitoring process. The whole program takes 4 minutes to enrich the data of 100 links of news articles. The main library used for crawling in this program is BeautifulSoup. Therefore it is best suited for static websites. It is built to crawl Indonesian and English news article.

Note: ensure that the news article link contains protocol information (https:// or http://) otherwise it will retrieve an error.
