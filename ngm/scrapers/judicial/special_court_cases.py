import scrapy
import os
from datetime import datetime, timedelta
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
from bs4 import BeautifulSoup
from nepali.datetime import nepalidate
from ..config import FILES_STORE, DEFAULT_SETTINGS
from ..utils import normalize_whitespace, normalize_date, nepali_to_roman_numerals, fix_parenthesis_spacing, parse_judges


class SpecialCourtCasesSpider(scrapy.Spider):
    name = "special_court_cases"
    base_url = "https://supremecourt.gov.np/special/syspublic.php?d=reports&f=daily_public"
    
    custom_settings = {
        **DEFAULT_SETTINGS,
        "FEEDS": {
            os.path.join(FILES_STORE, "supreme-court/special-court-cases/cases.jsonl"): {
                "format": "jsonlines",
                "encoding": "utf-8",
                "overwrite": False,
            }
        },
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Track processed dates in memory during this run to avoid duplicate R1 requests
        self.processed_dates_this_run = set()
        # Load dates that have already been scraped
        self.processed_dates = self.load_processed_dates()

    def load_processed_dates(self):
        """
        Load dates that have already been scraped from the JSONL file.
        Returns a set of date_ad strings that have been fully processed.
        Works for both local and S3/R2 storage.
        """
        processed = set()
        jsonl_path = os.path.join(FILES_STORE, "supreme-court/special-court-cases/cases.jsonl")
        
        try:
            # For S3/R2 storage, we need to use boto3 to read the file
            if FILES_STORE.startswith('s3://'):
                try:
                    import boto3
                    import io
                    import json
                    
                    # Parse S3 path
                    s3_path = jsonl_path.replace('s3://', '')
                    bucket_name = s3_path.split('/')[0]
                    key = '/'.join(s3_path.split('/')[1:])
                    
                    # Get S3 client
                    s3_client = boto3.client('s3')
                    
                    # Download and read the file
                    response = s3_client.get_object(Bucket=bucket_name, Key=key)
                    content = response['Body'].read().decode('utf-8')
                    
                    # Parse JSONL and extract dates
                    for line in content.split('\n'):
                        if line.strip():
                            case = json.loads(line)
                            processed.add(case.get('date_ad'))
                    
                    self.logger.info(f"Loaded {len(processed)} processed dates from S3")
                    
                except Exception as e:
                    self.logger.warning(f"Could not load from S3 (file may not exist yet): {e}")
            
            # For local storage
            else:
                if os.path.exists(jsonl_path):
                    import json
                    with open(jsonl_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                case = json.loads(line)
                                processed.add(case.get('date_ad'))
                    
                    self.logger.info(f"Loaded {len(processed)} processed dates from local storage")
                else:
                    self.logger.info("No existing data file found, starting fresh")
        
        except Exception as e:
            self.logger.warning(f"Error loading processed dates: {e}")
        
        return processed

    def start_requests(self):
        """Generate requests for the past 5 years, going backwards from today"""
        end_date = datetime.now().date() - timedelta(days=2)  # 2 days ago
        start_date = end_date - timedelta(days=5*365)  # 5 years ago
        
        current_date = end_date
        while current_date >= start_date:
            date_str = current_date.isoformat()
            
            # Skip if this date has already been processed
            if date_str in self.processed_dates:
                self.logger.debug(f"Skipping already processed date: {date_str}")
                current_date -= timedelta(days=1)
                continue
            
            # Convert to Nepali date
            try:
                nepali_date = nepalidate.from_date(current_date)
                syy = str(nepali_date.year)
                smm = str(nepali_date.month).zfill(2)
                sdd = str(nepali_date.day).zfill(2)
                
                self.logger.info(f"Processing date: {date_str} -> BS {syy}/{smm}/{sdd}")
                
                # R1: First request to get bench types for this date
                yield FormRequest(
                    url=self.base_url,
                    formdata={
                        'mode': 'showbench',
                        'syy': syy,
                        'smm': smm,
                        'sdd': sdd
                    },
                    callback=self.parse_bench_types,
                    meta={
                        'date_ad': date_str,
                        'syy': syy,
                        'smm': smm,
                        'sdd': sdd
                    },
                    dont_filter=True
                )
            except Exception as e:
                self.logger.error(f"Error converting date {date_str}: {e}")
            
            current_date -= timedelta(days=1)

    def parse_bench_types(self, response):
        """Parse the bench types from R1 response and checkpoint on date"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        date_ad = response.meta['date_ad']
        syy = response.meta['syy']
        smm = response.meta['smm']
        sdd = response.meta['sdd']
        
        # Mark this date as processed for this run
        if date_ad in self.processed_dates_this_run:
            self.logger.debug(f"Date {date_ad} already processed in this run, skipping")
            return
        
        self.processed_dates_this_run.add(date_ad)
        
        # Find the bench_type select element
        bench_select = soup.find('select', {'name': 'bench_type'})
        
        if not bench_select:
            self.logger.info(f"No bench types found for date {date_ad} (BS {syy}/{smm}/{sdd})")
            return
        
        # Extract bench type options with both value and label
        bench_options = bench_select.find_all('option')
        benches = []
        
        for option in bench_options:
            value = option.get('value', '').strip()
            label = option.get_text(strip=True)
            if value:  # Skip empty options
                benches.append({'value': value, 'label': label})
        
        self.logger.info(f"Found {len(benches)} bench types for date {date_ad}")
        
        # Find the yo hidden input value
        yo_input = soup.find('input', {'name': 'yo', 'type': 'hidden'})
        yo_value = yo_input.get('value', '1') if yo_input else '1'
        
        # R2: Request each bench type
        for bench in benches:
            yield FormRequest(
                url=self.base_url,
                formdata={
                    'mode': 'show',
                    'syy': syy,
                    'smm': smm,
                    'sdd': sdd,
                    'bench_type': bench['value'],
                    'yo': yo_value
                },
                callback=self.parse_cases,
                meta={
                    'date_ad': date_ad,
                    'syy': syy,
                    'smm': smm,
                    'sdd': sdd,
                    'bench_type': bench['value'],
                    'bench_label': bench['label']
                },
                dont_filter=True
            )

    def parse_cases(self, response):
        """Parse the case details from R2 bench response and yield items for feed export"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        date_ad = response.meta['date_ad']
        syy = response.meta['syy']
        smm = response.meta['smm']
        sdd = response.meta['sdd']
        bench_type = response.meta['bench_type']
        bench_label = response.meta['bench_label']
        
        # Extract court number (इजलास नं)
        court_number_elem = soup.find('font', string=lambda x: x and 'इजलास' in x and 'नं' in x)
        court_number = normalize_whitespace(court_number_elem.get_text()) if court_number_elem else ""
        
        # Extract judges - look for the bold font containing judge names
        judges_text = ""
        # Find all font tags with size="2" and bold
        for font_tag in soup.find_all('font', {'size': '2'}):
            text = font_tag.get_text(strip=True)
            if 'अध्यक्ष माननीय न्यायाधीश' in text or 'सदस्य माननीय न्यायाधीश' in text:
                # Get the parent td to capture all judge text
                parent_td = font_tag.find_parent('td')
                if parent_td:
                    # Extract text preserving line breaks from <br> tags
                    # Replace <br> tags with newlines before extracting text
                    for br in parent_td.find_all('br'):
                        br.replace_with('\n')
                    # Get text without normalizing whitespace (to preserve newlines)
                    judges_text = parent_td.get_text()
                    break
        
        # Extract footer (इजलास अधिकृत info)
        footer_text = ""
        # Find the last table which contains footer info
        all_tables = soup.find_all('table', {'width': '100%', 'border': '0'})
        if all_tables:
            footer_table = all_tables[-1]
            # Extract and clean up footer text
            footer_text = normalize_whitespace(footer_table.get_text())
        
        # Extract case table
        case_table = soup.find('table', {'width': '100%', 'border': '1'})
        
        if not case_table:
            self.logger.warning(f"No case table found for bench {bench_type} on {date_ad}")
            return
        
        # Parse table rows
        rows = case_table.find_all('tr')[1:]  # Skip header row
        
        cases_found = 0
        for row in rows:
            cells = row.find_all('td')
            
            if len(cells) < 11:
                continue
            
            # Extract case data and normalize whitespace
            serial_no = nepali_to_roman_numerals(normalize_whitespace(cells[0].get_text()))
            category = normalize_whitespace(cells[1].get_text())
            registration_date = normalize_date(normalize_whitespace(cells[2].get_text()))
            case_type = normalize_whitespace(cells[3].get_text())
            case_number = normalize_whitespace(cells[4].get_text())
            plaintiff = normalize_whitespace(cells[5].get_text())
            defendant = normalize_whitespace(cells[6].get_text())
            original_case_number = fix_parenthesis_spacing(normalize_whitespace(cells[7].get_text()))
            remarks = normalize_whitespace(cells[8].get_text())
            case_status = normalize_whitespace(cells[9].get_text())
            decision_type = normalize_whitespace(cells[10].get_text())
            
            # Skip if no case number
            if not case_number:
                continue
            
            # Parse judges into structured list
            judges_list = parse_judges(judges_text)
            
            # Normalize bench_label spacing
            bench_label_normalized = normalize_whitespace(bench_label)
            
            # Yield case data for feed export
            cases_found += 1
            yield {
                'case_number': case_number,
                'date_ad': date_ad,
                'date_bs': f"{syy}-{smm}-{sdd}",
                'bench_type': bench_type,
                'bench_label': bench_label_normalized,
                'court_number': court_number,
                'judges': judges_list,
                'serial_no': serial_no,
                'category': category,
                'registration_date': registration_date,
                'case_type': case_type,
                'plaintiff': plaintiff,
                'defendant': defendant,
                'original_case_number': original_case_number,
                'remarks': remarks,
                'case_status': case_status,
                'decision_type': decision_type,
                'footer': footer_text,
                'scraped_at': datetime.now().isoformat()
            }
        
        self.logger.info(f"Yielded {cases_found} cases for bench {bench_type} on {date_ad}")


if __name__ == "__main__":
    process = CrawlerProcess({"LOG_LEVEL": "INFO"})
    process.crawl(SpecialCourtCasesSpider)
    process.start()
