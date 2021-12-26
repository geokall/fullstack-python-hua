# STEPS

## Installation
User the [docker-fedora-guide](https://docs.docker.com/engine/install/fedora/) to install docker in fedora
```bash
sudo dnf install docker-ce docker-ce-cli containerd.io
```

Install neo4j on docker using the name hua-neo4j and environment neo4j/hua-neo4j
If the port 7687 is already in use, you should use the command "sudo lsof -i -P -n | grep"
and then kill it with "sudo kill (process_id)".
```bash
sudo docker run \
    --name hua-neo4j \
    -p7474:7474 -p7687:7687 \
    -d \
    -v $HOME/neo4j/data:/data \
    -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/var/lib/neo4j/import \
    -v $HOME/neo4j/plugins:/plugins \
    --env NEO4J_AUTH=neo4j/hua-neo4j \
    neo4j:latest
```

docker login --> credentials
sudo docker run --name hua-mysql -p 3307:3307 -e MYSQL_ROOT_PASSWORD=Qwerty123! -d mysql

Creating MySQL Script
        table = ("CREATE TABLE `full_stack`.`Product` ("
                 "`productID` INT NOT NULL,"
                 "`name` VARCHAR(150) NULL,"
                 "`price` DOUBLE NULL,"
                 "`rating` DOUBLE NULL,"
                 "`type` varchar(40) NULL,"
                 " PRIMARY KEY (`productID`),"
                 "UNIQUE INDEX `productID_UNIQUE` (`productID` ASC) VISIBLE);")