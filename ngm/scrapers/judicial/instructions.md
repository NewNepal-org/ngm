Write a district_court_cases.py to scrape court cases from district courts.

First, read /Users/kwame/Documents/projects/newnepal2/workspaces/jawafdehi/services/NepalGovernmentModernization/ngm/scrapers/judicial/district_courts.json.

URL is https://supremecourt.gov.np/weekly_dainik/pesi/daily/35

todays_date: (nepal time, B.S.)

pesi_date: yyyy-mm-dd (B.S.)

When found: /Users/kwame/Documents/projects/newnepal2/workspaces/jawafdehi/laboratory/example.html

When not found: /Users/kwame/Documents/projects/newnepal2/workspaces/jawafdehi/laboratory/example2.html

Outputs should go to output/court-cases/{district code_name}/...

Take inspiration from /Users/kwame/Documents/projects/newnepal2/workspaces/jawafdehi/services/NepalGovernmentModernization/ngm/scrapers/judicial/special_court_cases.py.

But we would like a single scraper to scrape over all district courts.

The checkpointing should be done at the district court level.



For DEBUG (/dev), let's just do a single court for the past 5 days (I will remove the code after testing is complete).