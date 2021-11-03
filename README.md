![POD](https://img.shields.io/badge/version-v1.0.0-blue.svg) ![POD](https://img.shields.io/badge/language-Scala-black.svg) ![POD](https://img.shields.io/badge/platform-Spark-red.svg) ![POD](https://img.shields.io/badge/license-gnu-lightgrey.svg) ![SBT compatible](https://img.shields.io/badge/SBT-compatible-4BC51D.svg?style=flat)

# Shape Test - Data Engineer

![](image002.png)

In this assignment, you will get to know the industry we work in – don’t worry, no previous experience is required. The assignment involves an FPSO (Floating Production, Storage, and Offloading) vessel data, and we need your help with equipment failure data ingestion. We hope this exercise will give you some insight into the challenges we face on a daily basis (for a reference of what is an FPSO, please check this LINK).

—

The FPSO vessel contains some equipment and each equipment have multiple sensors. Every time a failure happens, we get all the sensors data from the failed equipment, and we store this information in a log file (the time is in GMT time zone).



You are provided 3 files: the log file named “equipment_failure_sensors.log”; the file named “equipment_sensors.csv” with the relationships between sensors and equipment’s; and the file named “equipment.json” with the equipment data.



To solve this problem, we expect you to answer a few questions related to January 2020 (considering GMT time zone):



1 – Total equipment failures that happened?

2 – Which equipment code had most failures?

3 – Average amount of failures across equipment group, ordered by the number of failures in ascending order?

---


    ```
	URL: hoohoo.ncsa.uiuc.edu  QTDE: 251
	URL: piweba3y.prodigy.com  QTDE: 157
	URL: jbiagioni.npt.nuwc.navy.mil  QTDE: 132
	URL: piweba1y.prodigy.com  QTDE: 114
	URL: www-d4.proxy.aol.com  QTDE: 91
	```



## Requisitos mínimos

Tecnologia | Versão
------- | --------
Java | 11
Scala | 2.12.x
Spark | 3.1.0
SBT | 1.4.9
Docker   | Lastest


# Instruções de como testar a solução

```
$ git clone git@github.com:Jeffersonmf/shape-tech-test.git

$ sbt clean package

$ sbt run

$ sbt test for the unit tests
```

# Instruções de como testar a solução localmente

```
[ssh]
$ git clone git@github.com:Jeffersonmf/teste-nasa-logs.git

[https]
$ git clone https://github.com/Jeffersonmf/teste-nasa-logs.git

$ sbt clean package

$ sbt run

```

## Como utilizar as API's

**Testando se a solução está disponível**
```
Method: Get
http://localhost:9000/healthCheck
```

**Realiza o processamento dos logs com a necessidade copiar os arquivos de Logs na pasta de entrada. "É necessário COPIAR os arquivos para a pasta de entrada. A Pasta pode ser configurada no arquivo application.conf [SourceLocalFolder]", tendo o valor default ./inputdata**
```
Method: Get
http://localhost:9000/
```

