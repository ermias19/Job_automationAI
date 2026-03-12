from job_automation.reports import JOB_AUTOMATION_HEADERS, PHD_ROLE_HEADERS

JOB_TITLES = [
    "Full Stack Developer",
    "Software Engineer",
    "Frontend Developer",
    "Backend Developer",
    "Python Developer",
    "Mobile Developer",
    "Network Engineer",
    "DevOps Engineer",
    "Cloud Engineer",
    "Site Reliability Engineer",
    "API Developer",
    "Systems Engineer",
]

COUNTRIES = [
    {"code": "IT", "city": "Pisa", "full": "Italy"},
    {"code": "DE", "city": "Berlin", "full": "Germany"},
    {"code": "NL", "city": "Amsterdam", "full": "Netherlands"},
    {"code": "GB", "city": "London", "full": "United Kingdom"},
    {"code": "FR", "city": "Paris", "full": "France"},
    {"code": "ES", "city": "Barcelona", "full": "Spain"},
    {"code": "SE", "city": "Stockholm", "full": "Sweden"},
    {"code": "NO", "city": "Oslo", "full": "Norway"},
    {"code": "CH", "city": "Zurich", "full": "Switzerland"},
    {"code": "BE", "city": "Brussels", "full": "Belgium"},
    {"code": "PL", "city": "Warsaw", "full": "Poland"},
    {"code": "PT", "city": "Lisbon", "full": "Portugal"},
]

DEFAULT_SITES = ["linkedin"]

DEFAULT_SHEET_HEADERS = list(JOB_AUTOMATION_HEADERS)
DEFAULT_PHD_REPORT_HEADERS = list(PHD_ROLE_HEADERS)
