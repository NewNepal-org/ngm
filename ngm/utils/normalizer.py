import re

def normalize_whitespace(text):
    """Normalize all Unicode whitespace characters to regular spaces and clean up"""
    if not text:
        return ""
    # Replace all Unicode whitespace with regular space, then clean up
    # This regex matches all Unicode whitespace characters
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    # Strip surrounding quotes if present (sometimes HTML has stray quotes)
    text = text.strip('"\'')
    # Return empty string if only whitespace remained
    return text if text.strip() else ""


def nepali_to_roman_numerals(text):
    """Convert Nepali numerals (Devanagari digits) to Roman numerals"""
    if not text:
        return text
    
    # Mapping of Nepali digits to Roman digits
    nepali_to_roman = {
        '०': '0', '१': '1', '२': '2', '३': '3', '४': '4',
        '५': '5', '६': '6', '७': '7', '८': '8', '९': '9'
    }
    
    result = text
    for nepali, roman in nepali_to_roman.items():
        result = result.replace(nepali, roman)
    
    return result


def roman_to_nepali_numerals(text):
    """Convert Roman numerals (ASCII digits) to Nepali numerals (Devanagari digits)"""
    if not text:
        return text
    
    # Mapping of Roman digits to Nepali digits
    roman_to_nepali = {
        '0': '०', '1': '१', '2': '२', '3': '३', '4': '४',
        '5': '५', '6': '६', '7': '७', '8': '८', '9': '९'
    }
    
    result = text
    for roman, nepali in roman_to_nepali.items():
        result = result.replace(roman, nepali)
    
    return result


def normalize_date(date_str):
    """Normalize date format to use dashes and Roman numerals (e.g., २०८१/०९/२८ -> 2081-09-28)"""
    if not date_str:
        return date_str
    
    # Convert Nepali numerals to Roman
    date_str = nepali_to_roman_numerals(date_str)
    
    # Replace various date separators with dashes
    date_str = date_str.replace('/', '-')
    date_str = date_str.replace('।', '-')  # Devanagari danda (U+0964)
    date_str = date_str.replace('|', '-')  # Pipe character
    
    return date_str


def fix_parenthesis_spacing(text):
    """Fix spacing around parentheses (e.g., '082-CR-0048( text)' -> '082-CR-0048 (text)')"""
    if not text:
        return text
    
    import re
    # Add space before opening parenthesis if missing
    text = re.sub(r'(\S)\(', r'\1 (', text)
    # Remove space after opening parenthesis
    text = re.sub(r'\(\s+', '(', text)
    # Remove space before closing parenthesis
    text = re.sub(r'\s+\)', ')', text)
    
    return text


