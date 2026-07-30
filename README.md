Trying to connect to 10.177.103.199:50072
[2026-07-30 12:31:53] INFO - Trying namenode 10.177.103.199
[2026-07-30 12:31:53] INFO - Instantiated <InsecureClient(url='http://10.177.103.199:50072')>.
[2026-07-30 12:31:53] INFO - Fetching status for '/'.
[2026-07-30 12:31:53] INFO - Using namenode 10.177.103.199 for hook
[2026-07-30 12:31:53] INFO - Uploading '/tmp/airflow_oracle_exports/S-B_Circle_wise_report_20260331.psv' to '/airflow/reports/S-B_Circle_wise_report_20260331.psv'.
[2026-07-30 12:31:53] INFO - Listing '/airflow/reports/S-B_Circle_wise_report_20260331.psv'.
[2026-07-30 12:31:53] INFO - Writing to '/airflow/reports/S-B_Circle_wise_report_20260331.psv'.
[2026-07-30 12:31:53] ERROR - Error while uploading. Attempting cleanup.
ConnectionError: HTTPConnectionPool(host='fcdevhdfsdata2', port=50075): Max retries exceeded with url: /webhdfs/v1/airflow/reports/S-B_Circle_wise_report_20260331.psv?op=CREATE&user.name=root&namenoderpcaddress=fincoredev&createflag=&createparent=true&overwrite=true&user.name=root (Caused by NameResolutionError("HTTPConnection(host='fcdevhdfsdata2', port=50075): Failed to resolve 'fcdevhdfsdata2' ([Errno -3] Temporary failure in name resolution)"))
File "/home/airflow/.local/lib/python3.10/site-packages/hdfs/client.py", line 643 in upload

File "/home/airflow/.local/lib/python3.10/site-packages/hdfs/client.py", line 574 in _upload

File "/home/airflow/.local/lib/python3.10/site-packages/hdfs/client.py", line 520 in write

File "/home/airflow/.local/lib/python3.10/site-packages/hdfs/client.py", line 509 in consumer

File "/home/airflow/.local/lib/python3.10/site-packages/hdfs/client.py", line 209 in _request

File "/home/airflow/.local/lib/python3.10/site-packages/requests/sessions.py", line 589 in request

File "/home/airflow/.local/lib/python3.10/site-packages/requests/sessions.py", line 703 in send

File "/home/airflow/.local/lib/python3.10/site-packages/requests/adapters.py", line 677 in send

MaxRetryError: HTTPConnectionPool(host='fcdevhdfsdata2', port=50075): Max retries exceeded with url: /webhdfs/v1/airflow/reports/S-B_Circle_wise_report_20260331.psv?op=CREATE&user.name=root&namenoderpcaddress=fincoredev&createflag=&createparent=true&overwrite=true&user.name=root (Caused by NameResolutionError("HTTPConnection(host='fcdevhdfsdata2', port=50075): Failed to resolve 'fcdevhdfsdata2' ([Errno -3] Temporary failure in name resolution)"))
File "/home/airflow/.local/lib/python3.10/site-packages/requests/adapters.py", line 644 in send

File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/connectionpool.py", line 841 in urlopen

File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/util/retry.py", line 535 in increment

NameResolutionError: HTTPConnection(host='fcdevhdfsdata2', port=50075): Failed to resolve 'fcdevhdfsdata2' ([Errno -3] Temporary failure in name resolution)
File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/connectionpool.py", line 787 in urlopen

File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/connectionpool.py", line 493 in _make_request

File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/connection.py", line 500 in request

File "/usr/python/lib/python3.10/http/client.py", line 1298 in endheaders

File "/usr/python/lib/python3.10/http/client.py", line 1058 in _send_output

File "/usr/python/lib/python3.10/http/client.py", line 996 in send

File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/connection.py", line 331 in connect

File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/connection.py", line 211 in _new_conn

gaierror: [Errno -3] Temporary failure in name resolution
File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/connection.py", line 204 in _new_conn

File "/home/airflow/.local/lib/python3.10/site-packages/urllib3/util/connection.py", line 60 in create_connection

File "/usr/python/lib/python3.10/socket.py", line 967 in getaddrinfo

[2026-07-30 12:31:53] INFO - Deleting '/airflow/reports/S-B_Circle_wise_report_20260331.psv' recursively.
[2026-07-30 12:31:53] ERROR - Task failed with exception
ConnectionError: HTTPConnectionPool(host='fcdevhdfsdata2', port=50075): Max retries exceeded with url: /webhdfs/v1/airflow/reports/S-B_Circle_wise_report_20260331.psv?op=CREATE&user.name=root&namenoderpcaddress=fincoredev&createflag=&createparent=true&overwrite=true&user.name=root (Caused by NameResolutionError("HTTPConnection(host='fcdevhdfsdata2', port=50075): Failed to resolve 'fcdevhdfsdata2' ([Errno -3] Temporary failure in name resolution)"))
File "/home/airflow/.local/lib/python3.10/site-pack
