from src.utils.config import Paths

def test_default_paths_exist_format():
    p = Paths()
    assert p.RAW_PRICES_DIR.startswith("/opt/data") or p.RAW_PRICES_DIR.startswith("/")
    assert p.RAW_COMPANIES_CSV.endswith(".csv")

