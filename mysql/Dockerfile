FROM mysql:5.7

MAINTAINER Joumen Ali HARZLI

COPY mysql.cnf /etc/mysql/conf.d/
COPY initdb.sql /docker-entrypoint-initdb.d/


ENV MYSQL_ROOT_PASSWORD="root"
ENV MYSQL_DATABASE="test"

ADD setup.sql /tmp/setup.sql

RUN "mysql -u root --password="root" test < /tmp/setup.sql"
