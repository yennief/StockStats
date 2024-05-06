 #!/bin/bash

# Create the data directory
mkdir /tmp/datadir

# Start the MySQL container
docker run --name mymysql -v /tmp/datadir:/var/lib/mysql -p 6033:3306 \
-e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:debian

