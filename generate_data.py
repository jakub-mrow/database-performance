import random
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
import time
import matplotlib.pyplot as plt

# Initialize Faker
fake = Faker()

# Function to generate synthetic data
def generate_data(num_employees, num_departments):
    employees = []
    departments = []

    # Generate department data
    for dept_id in range(1, num_departments + 1):
        departments.append({
            'dept_id': dept_id,
            'dept_name': fake.company(),
            'location': fake.city(),
            'budget': round(random.uniform(50000, 500000), 2)
        })

    # Generate employee data
    for emp_id in range(1, num_employees + 1):
        employees.append({
            'emp_id': emp_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'department_id': random.randint(1, num_departments),
            'salary': round(random.uniform(30000, 120000), 2),
            'hire_date': fake.date_between(start_date='-10y', end_date='today')
        })

    return pd.DataFrame(employees), pd.DataFrame(departments)

# Generate data
num_employees = 100000
num_departments = 100
employees_df, departments_df = generate_data(num_employees, num_departments)

# Database connection parameters
db_url = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'

# Create an SQLAlchemy engine
engine = create_engine(db_url)

# Insert data into PostgreSQL
employees_df.to_sql('employees', engine, if_exists='replace', index=False)
departments_df.to_sql('departments', engine, if_exists='replace', index=False)