import scrapy
import json
from datetime import datetime, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
from bs4 import BeautifulSoup
from nepali.datetime import nepalidate
from ..config import OUTPUT_DIR, CONCURRENT_REQUESTS, DOWNLOAD_TIMEOUT
from ..utils import normalize_whitespace, normalize_date, nepali_to_roman_numerals, fix_parenthesis_spacing

SUPREME_COURT_DIR = OUTPUT_DIR / "court-cases" / "supremecourt"
JOBDIR = SUPREME_COURT_DIR / ".scrapy_state"
CHECKPOINT_FILE = SUPREME_COURT_DIR / ".checkpoint.json"


class SupremeCourtCasesSpider(scrapy.Spider):
    name = "supreme_court_cases"
    base_url = "https://supremecourt.gov.np/lic/sys.php?d=reports&f=weekly_suppli_public"
    
    custom_settings = {
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 2,
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "JOBDIR": str(JOBDIR),
        # Retry configuration
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "RETRY_PRIORITY_ADJUST": -1,
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
    
    def _find_case_table(self, soup):
        """
        Find the case table using multiple strategies for robustness.
        
        Strategy 1: Look for table with specific attributes
        Strategy 2: Look for table containing header row with expected columns
        Strategy 3: Look for table with 10 columns (क्र स, फांट, दर्ता मिती, etc.)
        """
        # Strategy 1: Exact attribute match
        table = soup.find('table', {
            'width': '100%',
            'border': '0',
            'cellspacing': '0',
            'bordercolor': '#ffffff'
        })
        if table and self._validate_case_table(table):
            return table
        
        # Strategy 2: Find table with header containing expected column names
        all_tables = soup.find_all('table')
        for table in all_tables:
            header_row = table.find('tr', bgcolor='#FFCC00')
            if not header_row:
                # Also try finding header by checking first row
                rows = table.find_all('tr')
                if rows:
                    first_row = rows[0]
                    if first_row.get('bgcolor') == '#FFCC00':
                        header_row = first_row
            
            if header_row:
                # Check if header contains expected columns
                header_text = header_row.get_text()
                if 'क्र' in header_text and 'मुद्दा नं' in header_text and 'पक्ष' in header_text:
                    if self._validate_case_table(table):
                        return table
        
        # Strategy 3: Find table with 10 columns
        for table in all_tables:
            rows = table.find_all('tr')
            if not rows:
                continue
            
            # Check first row for column count
            first_row = rows[0]
            cells = first_row.find_all(['td', 'th'])
            if len(cells) == 10:
                if self._validate_case_table(table):
                    return table
        
        return None
    
    def _validate_case_table(self, table):
        """
        Validate that the table is actually a case table.
        
        Checks:
        - Has at least one header row
        - Header row has 10 columns
        - Has at least one data row
        """
        if not table:
            return False
        
        rows = table.find_all('tr')
        if len(rows) < 2:  # Need at least header + 1 data row
            return False
        
        # Check header row
        header_row = rows[0]
        header_cells = header_row.find_all(['td', 'th'])
        if len(header_cells) != 10:
            return False
        
        return True
    
    def _find_case_rows(self, table):
        """
        Find all case data rows in the table.
        
        Returns rows with bgcolor="#ffffff" (lowercase) which are data rows.
        Excludes header rows and continuation rows without serial numbers.
        """
        return table.find_all('tr', bgcolor='#ffffff')
    
    def _clean_case_number(self, case_number):
        """
        Clean case number by removing parenthetical information.
        
        Examples:
        - "079-RF-0005 ( मिसिल नष्ट नभएको )" -> "079-RF-0005"
        - "082-AP-0304" -> "082-AP-0304"
        """
        if not case_number:
            return case_number
        
        # Remove anything in parentheses (including the parentheses)
        import re
        cleaned = re.sub(r'\s*\([^)]*\)\s*', '', case_number)
        return cleaned.strip()
    
    def _clean_division(self, division):
        """
        Clean division field by removing leading/trailing dashes and underscores.
        
        Examples:
        - "- रिट १ _" -> "रिट १"
        - "- मुद्दा फांट ९ _" -> "मुद्दा फांट ९"
        - "" -> ""
        """
        if not division:
            return division
        
        # Remove leading "- " and trailing " _"
        cleaned = division.strip()
        if cleaned.startswith('- '):
            cleaned = cleaned[2:]
        if cleaned.endswith(' _'):
            cleaned = cleaned[:-2]
        
        return cleaned.strip()
    
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
        end_date = datetime.now().date() - timedelta(days=2)  # 2 days ago
        start_date = end_date - timedelta(days=5*365)  # 5 years ago
        
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
                
                # Request the weekly supplementary causelist
                yield FormRequest(
                    url=self.base_url,
                    formdata={
                        'syy': syy,
                        'smm': smm,
                        'sdd': sdd,
                        'mode': 'show',
                        'yo': '1'
                    },
                    headers={
                        'Referer': 'https://supremecourt.gov.np/',
                        'Origin': 'https://supremecourt.gov.np',
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                    callback=self.parse_cases,
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

    def parse_cases(self, response):
        """Parse the case details from the response"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        date_ad = response.meta['date_ad']
        syy = response.meta['syy']
        smm = response.meta['smm']
        sdd = response.meta['sdd']
        
        # Check if request was blocked by WAF
        if "The requested URL was rejected" in response.text or "support ID is:" in response.text:
            self.logger.error(f"Request blocked by WAF for date {date_ad}. Try reducing CONCURRENT_REQUESTS or increasing DOWNLOAD_DELAY.")
            # Don't save checkpoint so we can retry later
            return
        
        # Find the case table using multiple strategies for robustness
        case_table = self._find_case_table(soup)
        
        if not case_table:
            self.logger.warning(f"No case table found for date {date_ad}")
            self.save_checkpoint(date_ad)
            return
        
        # Parse table rows (skip header row) - look for rows with bgcolor="#ffffff"
        rows = self._find_case_rows(case_table)
        
        if not rows:
            self.logger.info(f"No cases found for date {date_ad} (BS {syy}/{smm}/{sdd})")
            self.save_checkpoint(date_ad)
            return
        
        for row in rows:
            cells = row.find_all('td')
            
            if len(cells) < 10:
                continue
            
            # Extract case data and normalize whitespace
            serial_no = nepali_to_roman_numerals(normalize_whitespace(cells[0].get_text()))
            division = self._clean_division(normalize_whitespace(cells[1].get_text()))
            registration_date = normalize_date(normalize_whitespace(cells[2].get_text()))
            bench_type = normalize_whitespace(cells[3].get_text())
            case_type = normalize_whitespace(cells[4].get_text())
            case_number = self._clean_case_number(normalize_whitespace(cells[5].get_text()))
            parties = normalize_whitespace(cells[6].get_text())
            judges_cannot_hear = normalize_whitespace(cells[7].get_text())
            judges_must_hear = normalize_whitespace(cells[8].get_text())
            remarks = normalize_whitespace(cells[9].get_text())
            
            # Skip if no case number
            if not case_number:
                continue
            
            # Split parties into plaintiff and defendant
            plaintiff = ""
            defendant = ""
            if "||" in parties:
                parts = parties.split("||", 1)
                plaintiff = normalize_whitespace(parts[0])
                defendant = normalize_whitespace(parts[1])
            else:
                plaintiff = parties
            
            # Parse judges who must hear the case
            judges_list = []
            if judges_must_hear:
                # The judges_must_hear cell may contain <br> tags
                # BeautifulSoup's get_text() with separator will help
                judges_cell = cells[8]
                # Replace <br> with newlines before extracting text
                for br in judges_cell.find_all('br'):
                    br.replace_with('\n')
                judges_text = judges_cell.get_text()
                judge_names = judges_text.split('\n')
                for name in judge_names:
                    name = normalize_whitespace(name)
                    if name:
                        judges_list.append(name)
            
            # Create case data structure
            case_data = {
                'case_number': case_number,
                'date_ad': date_ad,
                'date_bs': f"{syy}-{smm}-{sdd}",
                'serial_no': serial_no,
                'division': division,
                'registration_date': registration_date,
                'bench_type': bench_type,
                'case_type': case_type,
                'plaintiff': plaintiff,
                'defendant': defendant,
                'judges_cannot_hear': judges_cannot_hear,
                'judges_must_hear': judges_list,
                'remarks': remarks,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Save case to file
            self.save_case(case_data)
        
        # Mark date as processed
        self.save_checkpoint(date_ad)

    def save_case(self, case_data):
        """Save case data to JSON file organized by registration date and case number"""
        case_number = case_data['case_number']
        date_bs = case_data['date_bs'].replace('/', '-')
        registration_date = case_data['registration_date']
        
        # Use registration date for directory organization
        reg_date_dir = registration_date if registration_date else "unknown"
        
        # Sanitize case number for directory name (already cleaned, just sanitize for filesystem)
        case_dir_name = case_number.replace('/', '-').replace('\\', '-').strip()
        
        # Organize as: supremecourt/<reg-date>/<case-number>/activity/<date>.json
        case_dir = SUPREME_COURT_DIR / reg_date_dir / case_dir_name / "activity"
        filepath = case_dir / f"{date_bs}.json"
        
        # Skip if file already exists
        if filepath.exists():
            return
        
        # Create directory if it doesn't exist
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(case_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved case: {case_number} for date {date_bs}")


if __name__ == "__main__":
    SUPREME_COURT_DIR.mkdir(parents=True, exist_ok=True)
    process = CrawlerProcess({"LOG_LEVEL": "INFO"})
    process.crawl(SupremeCourtCasesSpider)
    process.start()
