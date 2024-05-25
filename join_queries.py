import time
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

# Function to measure query execution time and get the execution plan
def measure_execution(query, engine):
    execution_time = None
    with engine.connect() as connection:
        start_time = time.time()
        connection.execute(text(query))
        end_time = time.time()
        execution_time = end_time - start_time

        # Get the execution plan
        plan = connection.execute(text(f"EXPLAIN ANALYZE {query}")).fetchall()
    return execution_time, plan

# Define the join queries
queries = [
    ("Inner Join", "SELECT e.first_name, e.last_name, d.dept_name FROM employees e JOIN departments d ON e.department_id = d.dept_id"),
    ("Left Join", "SELECT e.first_name, e.last_name, d.dept_name FROM employees e LEFT JOIN departments d ON e.department_id = d.dept_id"),
    ("Right Join", "SELECT e.first_name, e.last_name, d.dept_name FROM employees e RIGHT JOIN departments d ON e.department_id = d.dept_id"),
    ("Full Outer Join", "SELECT e.first_name, e.last_name, d.dept_name FROM employees e FULL OUTER JOIN departments d ON e.department_id = d.dept_id"),
    ("Join with LIKE", "SELECT e.first_name, e.last_name, d.dept_name FROM employees e JOIN departments d ON e.department_id = d.dept_id WHERE e.first_name LIKE 'A%'"),
    ("Join with BETWEEN", "SELECT e.first_name, e.last_name, d.dept_name FROM employees e JOIN departments d ON e.department_id = d.dept_id WHERE e.salary BETWEEN 50000 AND 100000")
]

# Measure execution times and get execution plans
execution_results = []
db_url = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
engine = create_engine(db_url)

for name, query in queries:
    exec_time, plan = measure_execution(query, engine)
    execution_results.append((name, exec_time, plan))

# Print results
for name, exec_time, plan in execution_results:
    print(f"Query: {name}")
    print(f"Execution Time: {exec_time:.4f} seconds")
    print("Execution Plan:")
    for line in plan:
        print(line[0])
    print("\n")
    
    
# Extract execution times for plotting
query_names = [result[0] for result in execution_results]
execution_times = [result[1] for result in execution_results]

##########################################
# INDEXES
##########################################

with engine.connect() as connection:
    connection.execute("CREATE INDEX idx_employees_department_id ON employees(department_id)")
    connection.execute("CREATE INDEX idx_departments_dept_id ON departments(dept_id)")


# Measure execution times and get execution plans with indexes
execution_results_with_index = []
for name, query in queries:
    exec_time_index, plan_index = measure_execution(query, engine)
    execution_results_with_index.append((name, exec_time_index, plan_index))


# Extract execution times for plotting
query_names_index = [result[0] for result in execution_results_with_index]
execution_times_index = [result[1] for result in execution_results_with_index]

# Drop the indexes
with engine.connect() as connection:
    connection.execute("DROP INDEX IF EXISTS idx_employees_department_id")
    connection.execute("DROP INDEX IF EXISTS idx_departments_dept_id")

# Plot execution times
plt.figure(figsize=(12, 8))
plt.barh(query_names, execution_times, color='skyblue')
plt.xlabel('Execution Time (seconds)')
plt.title('Join Query Execution Times')
plt.show()



# Plot execution times with indexes
plt.figure(figsize=(12, 8))
plt.barh(query_names_index, execution_times_index, color='skyblue')
plt.xlabel('Execution Time (seconds)')
plt.title('Join Query Execution Times with Indexes')
plt.show()

