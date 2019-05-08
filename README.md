# Zenoss Protocols

Zenoss's [Protobuf](http://code.google.com/apis/protocolbuffers/) interfaces
for communicating with ZEP and Impact.

# Python Build

    cd python
   asd
   
   asd
   fasdfpython setup.py bdist_egg

# Java Build

    cd java
    mvn install
    
# Helpful documents:

- [Language Guide](http://code.google.com/apis/protocolbuffers/docs/proto.html)

# Releasing

Use git flow to release a version to the `master` branch.

The version is defined twice: in [python/setup.py](./python/setup.py) and in [java/pom.xml](./java/pom.xml). The version must be changed in both files.

For Zenoss employees, the details on using git-flow to release a version is documented on the Zenoss Engineering [web site](https://sites.google.com/a/zenoss.com/engineering/home/faq/developer-patterns/using-git-flow).
 After the git flow process is complete, a jenkins job can be triggered manually to build and 
 publish the artifacts. 
