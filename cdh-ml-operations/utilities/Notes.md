### snippets

```
$env:AZCOPY_CRED_TYPE = "OAuthToken";
$env:AZCOPY_CONCURRENCY_VALUE = "AUTO";
./azcopy login;
./azcopy.exe copy "C:\Users\ulc0\Documents\wheels\*" "https://davsynapseanalyticsdev.blob.core.windows.net/cdh/wslibraries/" --overwrite=prompt --from-to=LocalBlob --blob-type BlockBlob --follow-symlinks --check-length=true --put-md5 --follow-symlinks --disable-auto-decoding=false --list-of-files "C:\Users\ulc0\AppData\Local\Temp\2\stg-exp-azcopy-44da30b7-1592-4359-abf2-1b602d0e095d.txt" --recursive --trusted-microsoft-suffixes=davsynapseanalyticsdev.blob.core.windows.net --log-level=INFO;
./azcopy logout;
$env:AZCOPY_CRED_TYPE = "";
$env:AZCOPY_CONCURRENCY_VALUE = "";
```


#### 'abfss://cdh@davsynapseanalyticsdev.dfs.core.windows.net/cdh/wslibraries/'