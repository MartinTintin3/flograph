import download_util
import db

seed = "064ad7f4-8d16-4dd2-94b1-1dd1c45c3832"

db.initialize_database()

download_util.crawl(seed_id=seed, depth=2)