## skydnsctl - cli tool for interacting with skynds


#### Commands:
* add
* get
* update
* delete


### Connect to your skynds http endpoint
To connect to the skydns http endpoint for issuing commands set the environment 
variable `SKYDNS_HTTP_ADDR` or you can run the cli app with the `--host` flag.

```bash
expose SKYDNS_HTTP_ADDR="http://localhost:8080"
# OR
skydnsctl --host "http://localhost:8080" 1001
```

#### Add a new service

```bash
skydnsctl add  1001 '{"Name":"TestService","Version":"1.0.0","Environment":"Production","Region":"Test","Host":"web
1.site.com","Port":9000,"TTL":1000}'
1001 added to skydns
```

#### Get an existing service by uuid

```bash
skydnsctl 1001

UUID: 1001
Name: TestService
Host: web1.site.com
Port: 9000
Environment: Production
Region: Test
Version: 1.0.0

TTL 492
Remaining TTL: 492
```

#### Get an existing service with json output

```bash
skydnsctl --json 1001
{"UUID":"1001","Name":"TestService","Version":"1.0.0","Environment":"Production","Region":"Test","Host":"web1.site.com","Port":9000,"TTL":987,"Expires":"2014-01-17T23:09:19.827085688-08:00"}
```

#### Update an existing service

```bash
skydnsctl update 1001 3000
1001 ttl updated to 3000
```

#### Delete an existing service

```bash
skydnsctl delete 1001
1001 removed from skydns
```