"""
Job Crawler Spiders
Three spiders for crawling job data from TopCV
"""

import scrapy
from scrapy.http import Request
from datetime import datetime
from .items import RawJobItem


class BaseJobSpider(scrapy.Spider):
    """Base spider with common configurations for all job crawlers"""
    
    name = "base_job_spider"
    allowed_domains = ["topcv.vn"]
    start_urls = ["https://www.topcv.vn/viec-lam-it"]
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 1,
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }


class RegularCrawlSpider(BaseJobSpider):
    """
    Spider 1: Regular crawling
    - Browse job listing pages and crawl each job detail
    - Save raw HTML to S3
    """
    
    name = "regular_job_crawler"
    
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_job_list, meta={'page': 1})
    
    def parse_job_list(self, response):
        """Parse job listing page and extract links to each job"""
        all_links = response.css('a::attr(href)').getall()
        job_links = list(set([l for l in all_links if '/viec-lam/' in l and l != response.url]))
        job_links = [l for l in job_links if not l.endswith('-it') and not l.endswith('-it/')]
        
        self.logger.info(f"Found {len(job_links)} job links on page {response.meta.get('page', 1)}")
        
        for link in job_links:
            yield Request(url=response.urljoin(link), callback=self.parse_job_detail)
        
        # Pagination
        current_page = response.meta.get('page', 1)
        max_pages = getattr(self, 'max_pages', 10)
        if current_page < max_pages:
            next_page = f"https://www.topcv.vn/viec-lam-it?page={current_page + 1}"
            yield Request(url=next_page, callback=self.parse_job_list, meta={'page': current_page + 1})
    
    def parse_job_detail(self, response):
        """Save raw HTML of job detail page"""
        item = RawJobItem()
        item['url'] = response.url
        item['source'] = 'topcv.vn'
        item['html'] = response.text
        item['crawled_at'] = datetime.now().isoformat()
        item['status'] = response.status
        yield item


class RawHtmlSpider(BaseJobSpider):
    """
    Spider 2: Raw HTML crawling from URL list
    - Accept list of URLs as input
    - Save entire HTML response
    """
    
    name = "raw_html_crawler"
    
    def __init__(self, urls=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls_to_crawl = urls.split(',') if urls else self.start_urls
    
    def start_requests(self):
        for url in self.urls_to_crawl:
            yield Request(url=url, callback=self.parse_raw_html, errback=self.handle_error)
    
    def parse_raw_html(self, response):
        item = RawJobItem()
        item['url'] = response.url
        item['source'] = 'topcv.vn'
        item['html'] = response.text
        item['crawled_at'] = datetime.now().isoformat()
        item['status'] = response.status
        yield item
    
    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.request.url}")
        item = RawJobItem()
        item['url'] = failure.request.url
        item['source'] = 'topcv.vn'
        item['html'] = None
        item['crawled_at'] = datetime.now().isoformat()
        item['status'] = 'error'
        yield item


class JobStatusSpider(BaseJobSpider):
    """
    Spider 3: Check if job is still active
    - Accept list of URLs to check
    - Return status: active, expired, deleted, filled
    """
    
    name = "job_status_checker"
    
    custom_settings = {
        **BaseJobSpider.custom_settings,
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 0.5,
    }
    
    def __init__(self, urls=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls_to_check = urls.split(',') if urls else []
    
    def start_requests(self):
        for url in self.urls_to_check:
            yield Request(url=url, callback=self.check_job_status, errback=self.handle_dead_job, dont_filter=True)
    
    def check_job_status(self, response):
        status = self._detect_job_status(response)
        yield {
            'url': response.url,
            'http_status': response.status,
            'job_status': status,
            'checked_at': datetime.now().isoformat(),
            'is_alive': status == 'active',
        }
    
    def _detect_job_status(self, response):
        """Analyze response to determine job status"""
        if response.status == 404:
            return 'deleted'
        if response.status == 410:
            return 'removed'
        if response.status != 200:
            return 'error'
        
        body_text = response.text.lower()
        
        # Check for expired patterns (Vietnamese)
        expired_patterns = ['việc làm đã hết hạn', 'job expired', 'đã hết hạn nộp hồ sơ']
        if any(p in body_text for p in expired_patterns):
            return 'expired'
        
        # Check for filled patterns
        filled_patterns = ['đã tuyển đủ', 'position has been filled']
        if any(p in body_text for p in filled_patterns):
            return 'filled'
        
        # Check for deleted patterns
        deleted_patterns = ['không tìm thấy', 'page not found']
        if any(p in body_text for p in deleted_patterns):
            return 'deleted'
        
        return 'active'
    
    def handle_dead_job(self, failure):
        yield {
            'url': failure.request.url,
            'http_status': None,
            'job_status': 'unreachable',
            'checked_at': datetime.now().isoformat(),
            'is_alive': False,
            'error': str(failure.value),
        }