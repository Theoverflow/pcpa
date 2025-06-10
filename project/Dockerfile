FROM postgres:15

# Install pgBackRest & Citus
RUN apt-get update \
    && apt-get install -y pgbackrest postgresql-15-citus \
    && rm -rf /var/lib/apt/lists/*

# Copy custom configuration
COPY ./postgresql.conf /etc/postgresql/postgresql.conf
COPY ./pgbackrest.conf /etc/pgbackrest/pgbackrest.conf


ENV POSTGRES_INITDB_ARGS="--username=postgres --encoding=UTF8" \
    PGDATA=/var/lib/postgresql/data/pgdata

# Create WAL archive directory
RUN mkdir -p /var/lib/postgresql/wal_archive \
    && chown -R postgres:postgres /var/lib/postgresql/wal_archive

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
