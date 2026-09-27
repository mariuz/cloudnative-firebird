-- Runs once, when the image entrypoint creates the database on the bootstrap primary:
-- journal every table (including tables created later) for replication.
ALTER DATABASE ENABLE PUBLICATION;
ALTER DATABASE INCLUDE ALL TO PUBLICATION;
COMMIT;
