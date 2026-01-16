import scrapy
import json
from datetime import datetime, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
from bs4 import BeautifulSoup
from nepali.datetime import nepalidate
from ..config import OUTPUT_DIR, CONCURRENT_REQUESTS, DOWNLOAD_TIMEOUT
from ..utils import normalize_whitespace, normalize_date, nepali_to_roman_numerals, fix_parenthesis_spacing, parse_judges

SPECIAL_COURT_DIR = OUTPUT_DIR / "court-cases" / "specialcourt"
JOBDIR = SPECIAL_COURT_DIR / ".scrapy_state"
CHECKPOINT_FILE = SPECIAL_COURT_DIR / ".checkpoint.json"


class SpecialCourtCasesSpider(scrapy.Spider):
    name = "special_court_cases"
    base_url = "https://supremecourt.gov.np/special/syspublic.php?d=reports&f=daily_public"
    
    custom_settings = {
        "CONCURRENT_REQUESTS": CONCURRENT_REQUESTS,
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "JOBDIR": str(JOBDIR),  # Enable Scrapy's built-in checkpointing
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_dates = self.load_checkpoint()
    
    def load_checkpoint(self):
        """Load set of already processed dates"""
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get('processed_dates', []))
        return set()
    
    def save_checkpoint(self, date_str):
        """Save a processed date to checkpoint"""
        self.processed_dates.add(date_str)
        CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'processed_dates': sorted(list(self.processed_dates)),
                'last_updated': datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2)

    def start_requests(self):
        """Generate requests for the past 5 years, going backwards from today"""
        end_date = datetime.now().date() - timedelta(days=2) # 2 days ago
        start_date = end_date - timedelta(days=5*365)  # 5 years ago

        # start_date = end_date - timedelta(days=) # TODO: For debuging purposes
        
        current_date = end_date
        while current_date >= start_date:
            date_str = current_date.isoformat()
            
            # Skip if already processed
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
                
                # First request to get bench types
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
        """Parse the bench types from the first response"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        date_ad = response.meta['date_ad']
        syy = response.meta['syy']
        smm = response.meta['smm']
        sdd = response.meta['sdd']
        
        # Find the bench_type select element
        bench_select = soup.find('select', {'name': 'bench_type'})
        
        if not bench_select:
            self.logger.info(f"No bench types found for date {date_ad} (BS {syy}/{smm}/{sdd})")
            # Mark date as processed even if no benches found
            self.save_checkpoint(date_ad)
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
        
        # Request each bench type
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
        
        # Mark date as processed after all bench requests are queued
        self.save_checkpoint(date_ad)

    def parse_cases(self, response):
        """Parse the case details from the bench response"""
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
            
            # Create case data structure
            case_data = {
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
            
            # Save case to file
            self.save_case(case_data)

    def save_case(self, case_data):
        """Save case data to JSON file organized by registration date and case number"""
        case_number = case_data['case_number']
        date_bs = case_data['date_bs'].replace('/', '-')  # e.g., "2082-09-02"
        registration_date = case_data['registration_date']  # e.g., "2082-04-23"
        
        # Use registration date for directory organization
        # If registration_date is empty, use "unknown"
        reg_date_dir = registration_date if registration_date else "unknown"
        
        # Sanitize case number for directory name
        case_dir_name = case_number.replace('/', '-').replace('\\', '-')
        
        # Organize as: specialcourt/<reg-date>/<case-number>/activity/<date>.json
        case_dir = SPECIAL_COURT_DIR / reg_date_dir / case_dir_name / "activity"
        filepath = case_dir / f"{date_bs}.json"
        
        # Skip if file already exists (avoid re-scraping)
        if filepath.exists():
            self.logger.debug(f"Case {case_number} for date {date_bs} already exists, skipping")
            return
        
        # Create directory if it doesn't exist
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(case_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved case: {case_number} for date {date_bs} to {filepath}")


if __name__ == "__main__":
    SPECIAL_COURT_DIR.mkdir(parents=True, exist_ok=True)
    process = CrawlerProcess({"LOG_LEVEL": "INFO"})
    process.crawl(SpecialCourtCasesSpider)
    process.start()
