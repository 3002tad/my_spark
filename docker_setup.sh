#!/bin/bash

# Tạo Docker Network
docker network create mynetwork
sleep 3

# Chạy container MongoDB trong network mynetwork
docker run -d -p 27017:27017 --network mynetwork --name mymongodb mongo
sleep 3

# Chạy container unica-full với biến môi trường Mongo_HOST
docker run -e Mongo_HOST=mymongodb --network mynetwork --name unica-full 3002tad/unica_full_data
sleep 3

# Chạy container PostgreSQL với các thông số user, password, và database
docker run --name mypostgres --network mynetwork -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=12345 -e POSTGRES_DB=unica_db -p 5432:5432 -d postgres
sleep 3

# Tạo bảng instructor trong database unica_db
docker exec -it mypostgres psql -U postgres -d unica_db -c "CREATE TABLE instructor (instructor_id SERIAL PRIMARY KEY, instructor_name VARCHAR(255) UNIQUE);"
sleep 3

# Tạo bảng course trong database unica_db
docker exec -it mypostgres psql -U postgres -d unica_db -c "CREATE TABLE course (course_id SERIAL PRIMARY KEY, course_name VARCHAR(255), new_price FLOAT, old_price FLOAT, number_of_students INTEGER, rating FLOAT, sections INTEGER, lectures INTEGER, total_duration_hours FLOAT, what_you_learn TEXT, instructor_id INTEGER REFERENCES instructor(instructor_id));"
sleep 3

# Tạo bảng course_tag trong database unica_db
docker exec -it mypostgres psql -U postgres -d unica_db -c "CREATE TABLE course_tag (tag_id SERIAL PRIMARY KEY, tag_name VARCHAR(255) UNIQUE);"
sleep 3

# Tạo bảng course_tag_assignments trong database unica_db
docker exec -it mypostgres psql -U postgres -d unica_db -c "CREATE TABLE course_tag_assignments (course_id INTEGER REFERENCES course(course_id), tag_id INTEGER REFERENCES course_tag(tag_id), PRIMARY KEY (course_id, tag_id));"
sleep 3

# Chạy container Spark với volume dữ liệu từ thư mục /home/ntd/spark_data
docker run -e Mongo_HOST=mymongodb --network mynetwork --name spark_container -v /home/ntd/spark_data:/spark_data 3002tad/my_spark