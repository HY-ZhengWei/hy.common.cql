

del /Q hy.common.cql.jar
del /Q hy.common.cql-sources.jar


call mvn clean package
cd .\target\classes


rd /s/q .\org\hy\common\cql\junit

jar cvfm hy.common.cql.jar META-INF/MANIFEST.MF META-INF org

copy hy.common.cql.jar ..\..
del /q hy.common.cql.jar
cd ..\..





cd .\src\main\java
xcopy /S ..\resources\* .
jar cvfm hy.common.cql-sources.jar META-INF\MANIFEST.MF META-INF org 
copy hy.common.cql-sources.jar ..\..\..
del /Q hy.common.cql-sources.jar
rd /s/q META-INF
cd ..\..\..
