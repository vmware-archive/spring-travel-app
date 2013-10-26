create external table Booking  (id bigint, hotel_id bigint)
location ('pxf://<name_node_hostname>:<name_node_http_port>/sta_tables/APP.BOOKING?Fragmenter=GemFireXDFragmenter&Accessor=GemFireXDAccessor&Resolver=GemFireXDResolver&CHECKPOINT=FALSE')
format 'custom' (formatter='pxfwritable_import');
