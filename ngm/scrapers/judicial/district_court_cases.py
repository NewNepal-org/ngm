"""
District Court Cases Scraper

Scrapes daily case lists (pesi) from all district courts in Nepal.
URL pattern: https://supremecourt.gov.np/weekly_dainik/pesi/daily/{district_id}
POST params: todays_date (BS), pesi_date (yyyy-mm-dd BS)

Checkpointing is done per-district court to allow resuming.
"""

import scrapy
import json
from datetime import datetime, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
from bs4 import BeautifulSoup
from nepali.datetime import nepalidate
from ..config import OUTPUT_DIR, CONCURRENT_REQUESTS, DOWNLOAD_TIMEOUT
from ..utils import normalize_whitespace, normalize_date, nepali_to_roman_numerals

# Base output directory for district court cases
DISTRICT_COURTS_DIR = OUTPUT_DIR / "court-cases"

# Load district courts configuration
DISTRICT_COURTS_JSON = Path(__file__).parent / "district_courts.json"



def load_district_courts():
    """Load district courts from JSON file"""
    with open(DISTRICT_COURTS_JSON, 'r', encoding='utf-8') as f:
        courts = json.load(f)
    # Filter out courts without code_name
    return [c for c in courts if c.get('code_name')]


def get_checkpoint_file(code_name: str) -> Path:
    """Get checkpoint file path for a district court"""
    return DISTRICT_COURTS_DIR / code_name / ".checkpoint.json"


def load_checkpoint(code_name: str) -> set:
    """Load set of already processed dates for a district court"""
    checkpoint_file = get_checkpoint_file(code_name)
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('processed_dates', []))
    return set()


def save_checkpoint(code_name: str, processed_dates: set):
    """Save processed dates to checkpoint file"""
    checkpoint_file = get_checkpoint_file(code_name)
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump({
            'processed_dates': sorted(list(processed_dates)),
            'last_updated': datetime.now().isoformat()
        }, f, ensure_ascii=False, indent=2)


class DistrictCourtCasesSpider(scrapy.Spider):
    name = "district_court_cases"
    base_url = "https://supremecourt.gov.np/weekly_dainik/pesi/daily/{district_id}"
    
    custom_settings = {
        "CONCURRENT_REQUESTS": CONCURRENT_REQUESTS,
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.district_courts = load_district_courts()
        # Per-court checkpoint tracking
        self.checkpoints = {}

    def start_requests(self):
        """Generate requests for all district courts"""
        # Calculate date range - 5 years back
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=5*365)
        
        for court in self.district_courts:
            code_name = court['code_name']
            district_id = court['district_id']
            district_name = court['district']
            
            # Load checkpoint for this court
            self.checkpoints[code_name] = load_checkpoint(code_name)
            
            self.logger.info(
                f"Starting scrape for {district_name} ({code_name}), "
                f"id={district_id}, {len(self.checkpoints[code_name])} dates already processed"
            )
            
            # Generate requests for each date
            current_date = end_date
            while current_date >= start_date:
                date_str = current_date.isoformat()
                
                # Skip if already processed
                if date_str in self.checkpoints[code_name]:
                    self.logger.debug(f"Skipping {code_name} {date_str} (already processed)")
                    current_date -= timedelta(days=1)
                    continue
                
                # Convert to Nepali date
                try:
                    nepali_date = nepalidate.from_date(current_date)
                    pesi_date = f"{nepali_date.year}-{str(nepali_date.month).zfill(2)}-{str(nepali_date.day).zfill(2)}"
                    todays_nepali = nepalidate.from_date(datetime.now().date())
                    todays_date = f"{todays_nepali.year}-{str(todays_nepali.month).zfill(2)}-{str(todays_nepali.day).zfill(2)}"
                    
                    url = self.base_url.format(district_id=district_id)
                    
                    yield FormRequest(
                        url=url,
                        method='POST',
                        formdata={
                            'todays_date': todays_date,
                            'pesi_date': pesi_date,
                            'submit': 'खोज्नु होस्'
                        },
                        callback=self.parse_daily_list,
                        meta={
                            'code_name': code_name,
                            'district_id': district_id,
                            'district_name': district_name,
                            'date_ad': date_str,
                            'date_bs': pesi_date,
                        },
                        dont_filter=True
                    )
                except Exception as e:
                    self.logger.error(f"Error converting date {date_str}: {e}")
                
                current_date -= timedelta(days=1)

    def parse_daily_list(self, response):
        """Parse the daily case list response"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        code_name = response.meta['code_name']
        district_id = response.meta['district_id']
        district_name = response.meta['district_name']
        date_ad = response.meta['date_ad']
        date_bs = response.meta['date_bs']
        
        # Check if no data available (look for error message)
        error_div = soup.find('div', class_='alert_error')
        if error_div and 'Causelist is not available' in error_div.get_text():
            self.logger.info(f"No cases for {code_name} on {date_bs} ({date_ad})")
            self._mark_processed(code_name, date_ad)
            return
        
        # Find all case tables (border="1" with class="record_display")
        case_tables = soup.find_all('table', {'border': '1', 'class': 'record_display'})
        
        if not case_tables:
            self.logger.info(f"No case tables found for {code_name} on {date_bs}")
            self._mark_processed(code_name, date_ad)
            return
        
        cases_found = 0
        current_bench = None
        current_judge = None
        
        # Process each table - benches are in preceding tables
        for table in case_tables:
            # Look for bench info in preceding sibling table
            prev_table = table.find_previous_sibling('table')
            if prev_table:
                bench_row = prev_table.find('tr')
                if bench_row:
                    bench_td = bench_row.find('td', align='right')
                    judge_td = bench_row.find('td', class_='judge')
                    if bench_td:
                        current_bench = normalize_whitespace(bench_td.get_text())
                    if judge_td:
                        current_judge = normalize_whitespace(judge_td.get_text())
            
            # Parse case rows (skip header row)
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                
                # Skip header rows and footer rows
                if len(cells) < 10:
                    # Check if it's a footer row with officer info
                    if len(cells) == 1 and 'इजलास अधिकृत' in cells[0].get_text():
                        continue
                    continue
                
                # Check if this is a header row
                if row.find('th'):
                    continue
                
                # Extract case data
                try:
                    serial_no = normalize_whitespace(cells[0].get_text())
                    
                    # Case number cell has format: "०८१-C१-०१३६<br>(३५-०८१-००७१३)"
                    case_parts = cells[1].get_text(separator='\n').strip().split('\n')
                    case_number = normalize_whitespace(case_parts[0]) if case_parts else ""
                    case_id = normalize_whitespace(case_parts[1].strip('()')) if len(case_parts) > 1 else ""
                    
                    # Registration date cell may have extra text after <br> (e.g., "सामान्य मार्ग")
                    reg_date_parts = cells[2].get_text(separator='\n').strip().split('\n')
                    registration_date = normalize_date(normalize_whitespace(reg_date_parts[0])) if reg_date_parts else ""
                    case_type = normalize_whitespace(cells[3].get_text())
                    plaintiff = normalize_whitespace(cells[4].get_text())
                    defendant = normalize_whitespace(cells[5].get_text())
                    section = normalize_whitespace(cells[6].get_text()) or ""  # फाँटबाला
                    priority = normalize_whitespace(cells[7].get_text()) or ""  # प्राथमिकता
                    remarks = normalize_whitespace(cells[8].get_text()) or ""  # कैफियत
                    decision_type = normalize_whitespace(cells[9].get_text()) or ""  # आदेश फैसलाको किसिम
                    
                    if not case_number:
                        continue
                    
                    case_data = {
                        'case_number': nepali_to_roman_numerals(case_number),
                        'case_id': nepali_to_roman_numerals(case_id),
                        'district_code': code_name,
                        'district_id': district_id,
                        'district_name': district_name,
                        'date_ad': date_ad,
                        'date_bs': date_bs,
                        'bench': current_bench,
                        'judge': current_judge,
                        'serial_no': nepali_to_roman_numerals(serial_no),
                        'registration_date': registration_date,
                        'case_type': case_type,
                        'plaintiff': plaintiff,
                        'defendant': defendant,
                        'section': section,
                        'priority': priority,
                        'remarks': remarks,
                        'decision_type': decision_type,
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    self.save_case(case_data)
                    cases_found += 1
                    
                except Exception as e:
                    self.logger.error(f"Error parsing row: {e}")
                    continue
        
        self.logger.info(f"Found {cases_found} cases for {code_name} on {date_bs}")
        self._mark_processed(code_name, date_ad)

    def _mark_processed(self, code_name: str, date_ad: str):
        """Mark a date as processed for a district court"""
        self.checkpoints[code_name].add(date_ad)
        save_checkpoint(code_name, self.checkpoints[code_name])

    def save_case(self, case_data: dict):
        """Save case data to JSON file organized by registration date and case number"""
        code_name = case_data['district_code']
        case_number = case_data['case_number']
        date_bs = case_data['date_bs']
        registration_date = case_data['registration_date']
        
        # Use registration date for directory organization
        reg_date_dir = registration_date if registration_date else "unknown"
        
        # Sanitize case number for directory name
        case_dir_name = case_number.replace('/', '-').replace('\\', '-')
        
        # Organize as: court-cases/<code_name>/<reg-date>/<case-number>/activity/<date>.json
        case_dir = DISTRICT_COURTS_DIR / code_name / reg_date_dir / case_dir_name / "activity"
        filepath = case_dir / f"{date_bs}.json"
        
        # Skip if file already exists
        if filepath.exists():
            self.logger.debug(f"Case {case_number} for {date_bs} already exists")
            return
        
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(case_data, f, ensure_ascii=False, indent=2)
        
        self.logger.debug(f"Saved: {filepath}")


if __name__ == "__main__":
    DISTRICT_COURTS_DIR.mkdir(parents=True, exist_ok=True)
    process = CrawlerProcess({"LOG_LEVEL": "INFO"})
    process.crawl(DistrictCourtCasesSpider)
    process.start()
