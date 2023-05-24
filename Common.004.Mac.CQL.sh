#!/bin/sh

cd ./bin


rm -R ./org/hy/common/cql/junit

jar cvfm hy.common.cql.jar MANIFEST.MF META-INF org

cp hy.common.cql.jar ..
rm hy.common.cql.jar
cd ..





cd ./src
jar cvfm hy.common.cql-sources.jar MANIFEST.MF META-INF org 
cp hy.common.cql-sources.jar ..
rm hy.common.cql-sources.jar
cd ..
