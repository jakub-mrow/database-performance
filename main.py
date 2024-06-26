import subprocess

from join_method_test import JoinMethodTest
from join_index_test import JoinIndexTest
from join_like_test import JoinLikeTest
from join_between_test import JoinBetweenTest

EMPLOYEES_NUMBER_LIST = [1000, 10000, 100000]
#EMPLOYEES_NUMBER_LIST = [10000]
NUM_OF_DEPARTMENTS = 100

if __name__ == "__main__":
    db_url = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
    
    purge_command = 'docker exec -it postgres psql -U postgres -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"'
    for emploeey_number in EMPLOYEES_NUMBER_LIST:
        JoinMethodTest(db_url, emploeey_number, NUM_OF_DEPARTMENTS).execute()
        subprocess.run(purge_command, shell=True)
        
        JoinIndexTest(db_url, emploeey_number, NUM_OF_DEPARTMENTS).execute()
        subprocess.run(purge_command, shell=True)
        
        JoinLikeTest(db_url, emploeey_number, NUM_OF_DEPARTMENTS).execute()
        subprocess.run(purge_command, shell=True)
        
        JoinBetweenTest(db_url, emploeey_number, NUM_OF_DEPARTMENTS).execute()
        subprocess.run(purge_command, shell=True)
        