#!/bin/sh

cd ./bin


rm -R ./org/hy/common/xcql/junit

jar cvfm hy.common.xcql.jar MANIFEST.MF META-INF org

cp hy.common.xcql.jar ..
rm hy.common.xcql.jar
cd ..





cd ./src
jar cvfm hy.common.xcql-sources.jar MANIFEST.MF META-INF org 
cp hy.common.xcql-sources.jar ..
rm hy.common.xcql-sources.jar
cd ..
