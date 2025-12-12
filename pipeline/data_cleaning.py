"""
Data Cleaning Pipeline - Bronze to Silver
Parse raw HTML from Bronze layer and extract structured data to Silver layer
"""

import json
import boto3
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional
import os
import re
from dotenv import load_dotenv

load_dotenv()


class JobDataCleaner:
    """Parse HTML from raw jobs and extract structured data"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'ap-southeast-1')
        )
        self.bucket = os.getenv('AWS_S3_BUCKET')
    
    def parse_job_html(self, html: str, url: str) -> Dict:
        """Parse HTML of a job posting and extract information"""
        soup = BeautifulSoup(html, 'html.parser')
        
        return {
            'url': url,
            'title': self._extract_title(soup),
            'company': self._extract_company(soup),
            'salary': self._extract_salary(soup),
            'location': self._extract_location(soup),
            'experience': self._extract_experience(soup),
            'job_type': self._extract_job_type(soup),
            'deadline': self._extract_deadline(soup),
            'description': self._extract_description(soup),
            'requirements': self._extract_requirements(soup),
            'benefits': self._extract_benefits(soup),
            'skills': self._extract_skills(soup),
            'company_info': self._extract_company_info(soup),
            'parsed_at': datetime.now().isoformat(),
        }
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job title"""
        for sel in ['h1.job-title', 'h1.title', '.job-detail-title h1', 'h1']:
            elem = soup.select_one(sel)
            if elem:
                return self._clean_text(elem.get_text())
        return None
    
    def _extract_company(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract company name"""
        for sel in ['.company-name', '.company-title', 'a.company', '.employer-name']:
            elem = soup.select_one(sel)
            if elem:
                return self._clean_text(elem.get_text())
        return None
    
    def _extract_salary(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract salary information"""
        for sel in ['.salary', '.job-salary', '[class*="salary"]']:
            elem = soup.select_one(sel)
            if elem:
                text = self._clean_text(elem.get_text())
                if text and any(k in text.lower() for k in ['triệu', 'usd', 'thỏa thuận']):
                    return text
        return None
    
    def _extract_location(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job location"""
        for sel in ['.location', '.job-location', '[class*="location"]']:
            elem = soup.select_one(sel)
            if elem:
                return self._clean_text(elem.get_text())
        return None
    
    def _extract_experience(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract experience requirement"""
        for sel in ['.experience', '[class*="experience"]']:
            elem = soup.select_one(sel)
            if elem:
                return self._clean_text(elem.get_text())
        
        # Search in text
        text = soup.get_text().lower()
        patterns = [r'(\d+)\s*[-–]\s*(\d+)\s*năm', r'(\d+)\+?\s*năm']
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(0)
        return None
    
    def _extract_job_type(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job type"""
        text = soup.get_text().lower()
        if 'full-time' in text or 'toàn thời gian' in text:
            return 'Full-time'
        elif 'part-time' in text:
            return 'Part-time'
        elif 'remote' in text:
            return 'Remote'
        elif 'intern' in text or 'thực tập' in text:
            return 'Internship'
        return None
    
    def _extract_deadline(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract application deadline"""
        for sel in ['.deadline', '.job-deadline', '[class*="deadline"]']:
            elem = soup.select_one(sel)
            if elem:
                text = self._clean_text(elem.get_text())
                date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', text)
                return date_match.group(0) if date_match else text
        return None
    
    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job description"""
        for sel in ['.job-description', '.job-detail-content', '[class*="description"]']:
            elem = soup.select_one(sel)
            if elem:
                return self._clean_text(elem.get_text())
        return None
    
    def _extract_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job requirements"""
        for sel in ['.job-requirements', '[class*="requirement"]']:
            elem = soup.select_one(sel)
            if elem:
                return self._clean_text(elem.get_text())
        return None
    
    def _extract_benefits(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job benefits"""
        for sel in ['.job-benefits', '[class*="benefit"]']:
            elem = soup.select_one(sel)
            if elem:
                return self._clean_text(elem.get_text())
        return None
    
    def _extract_skills(self, soup: BeautifulSoup) -> List[str]:
        """Extract required skills/tags"""
        skills = []
        for sel in ['.skill-tag', '.tag', '[class*="skill"]']:
            for elem in soup.select(sel):
                text = self._clean_text(elem.get_text())
                if text and len(text) < 50:
                    skills.append(text)
        return list(set(skills))
    
    def _extract_company_info(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract company information"""
        info = {}
        size_match = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*nhân viên', soup.get_text())
        if size_match:
            info['size'] = f"{size_match.group(1)}-{size_match.group(2)}"
        
        addr_elem = soup.select_one('.company-address, .address')
        if addr_elem:
            info['address'] = self._clean_text(addr_elem.get_text())
        
        return info if info else None
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


class BronzeToSilverPipeline:
    """Pipeline to read from Bronze (raw) and write to Silver (processed)"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'ap-southeast-1')
        )
        self.bucket = os.getenv('AWS_S3_BUCKET')
        self.cleaner = JobDataCleaner()
        self.stats = {'total_processed': 0, 'success': 0, 'failed': 0, 'skipped': 0}
    
    def run(self, date: str = None, limit: int = None):
        """Run Bronze -> Silver pipeline"""
        print(f"\n{'='*60}")
        print(f"Bronze to Silver Pipeline")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        
        bronze_prefix = "raw/jobs/"
        if date:
            bronze_prefix += f"source=topcv.vn/year={date.split('/')[0]}/month={date.split('/')[1]}/day={date.split('/')[2]}/"
        
        print(f"Reading from: s3://{self.bucket}/{bronze_prefix}")
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        count = 0
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=bronze_prefix):
            for obj in page.get('Contents', []):
                if limit and count >= limit:
                    break
                
                key = obj['Key']
                if key.endswith('.json'):
                    self._process_file(key)
                    count += 1
            
            if limit and count >= limit:
                break
        
        print(f"\n{'='*60}")
        print(f"Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Stats: {self.stats}")
        print(f"{'='*60}\n")
    
    def _process_file(self, bronze_key: str):
        """Process a single file from Bronze and write to Silver"""
        try:
            self.stats['total_processed'] += 1
            
            response = self.s3_client.get_object(Bucket=self.bucket, Key=bronze_key)
            raw_data = json.loads(response['Body'].read().decode('utf-8'))
            
            html = raw_data.get('html', '')
            url = raw_data.get('url', '')
            
            if not html:
                self.stats['skipped'] += 1
                return
            
            cleaned_data = self.cleaner.parse_job_html(html, url)
            cleaned_data['source'] = raw_data.get('source', 'unknown')
            cleaned_data['crawled_at'] = raw_data.get('crawled_at')
            cleaned_data['bronze_path'] = bronze_key
            
            silver_key = bronze_key.replace('raw/', 'processed/', 1)
            
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=silver_key,
                Body=json.dumps(cleaned_data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            
            self.stats['success'] += 1
            print(f"✓ Processed: {bronze_key.split('/')[-1]}")
            
        except Exception as e:
            self.stats['failed'] += 1
            print(f"✗ Failed: {bronze_key} - {str(e)}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Bronze to Silver Data Pipeline")
    parser.add_argument('--date', type=str, help='Date to process (YYYY/MM/DD)')
    parser.add_argument('--limit', type=int, help='Limit number of records')
    
    args = parser.parse_args()
    
    pipeline = BronzeToSilverPipeline()
    pipeline.run(date=args.date, limit=args.limit)


if __name__ == "__main__":
    main()
