#!/bin/sh

mvn install:install-file -Dfile=hy.common.xcql.jar                              -DpomFile=./src/main/resources/META-INF/maven/cn.openapis/hy.common.xcql/pom.xml
mvn install:install-file -Dfile=hy.common.xcql-sources.jar -Dclassifier=sources -DpomFile=./src/main/resources/META-INF/maven/cn.openapis/hy.common.xcql/pom.xml

mvn deploy:deploy-file   -Dfile=hy.common.xcql.jar                              -DpomFile=./src/main/resources/META-INF/maven/cn.openapis/hy.common.xcql/pom.xml -DrepositoryId=thirdparty -Durl=http://HY-ZhengWei:8081/repository/thirdparty
mvn deploy:deploy-file   -Dfile=hy.common.xcql-sources.jar -Dclassifier=sources -DpomFile=./src/main/resources/META-INF/maven/cn.openapis/hy.common.xcql/pom.xml -DrepositoryId=thirdparty -Durl=http://HY-ZhengWei:8081/repository/thirdparty
