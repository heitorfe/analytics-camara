import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set.")
engine = create_engine(DATABASE_URL, echo=True, future=True)
# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a session
session = Session()

# Test the connection
try:
    # Execute a simple query to test the connection
    session.execute(text("SELECT 1"))
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
finally:
    session.close()