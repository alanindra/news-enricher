# news-enrichment

This program automates the enrichment of news article data. It accepts a CSV file containing a list of article URLs, crawls each link, and outputs a new CSV file with the following information:

- Article title  
- Full content  
- Publication date  
- Journalist name (if available)

Designed to support media monitoring workflows, the tool can process and enrich 100 news article links in approximately 4 minutes.

The program uses `BeautifulSoup` as the core crawling library, making it best suited for **static websites**. It is compatible with both **Indonesian** and **English** news sources.

> [!NOTE] 
> Make sure that each news article URL in the input file includes the full protocol (`https://` or `http://`).  
> Missing protocol information will result in a crawling error.
