# Uploader

Uploader is the web frontend to the Piazza demo app.
It allows users to introduce files into the processing pipeline by uploading them.
The current pipeline inspects uploaded files for metadata and inserts the metadata into a Postgres table.
The uploader webapp also includes a search form to query the Postgres table and verify the data made it all the way through the pipeline.
