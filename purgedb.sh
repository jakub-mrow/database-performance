# purge all tables in postgres db that is running inside of container
docker exec -it postgres psql -U postgres -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
